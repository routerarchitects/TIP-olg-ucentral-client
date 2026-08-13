package nats

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sync"

	"github.com/Telecominfraproject/olg-nats-agent-core/agentcore"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/config"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/contracts"
)

type NATSClient struct {
	target string
	conn   *nats.Conn
	js     jetstream.JetStream
	kv     jetstream.KeyValue
	kvMu   sync.Mutex
}

// NewNATSClient initializes a NATS connection.
// SECURITY CONTRACT: This constructor MUST enforce tls.Config{MinVersion: tls.VersionTLS13}.
// It must return a fatal error if CAFile is empty, or if any Server URL is insecure.
func NewNATSClient(cfg config.NATSConfig, onStateChange func(contracts.LinkState)) (*NATSClient, error) {
	if err := contracts.ValidateNATSTarget(cfg.Target); err != nil {
		return nil, fmt.Errorf("nats: fatal: %w", err)
	}
	if cfg.CAFile == "" {
		return nil, errors.New("nats: fatal: CAFile is required")
	}
	if cfg.CredentialsFile == "" {
		return nil, errors.New("nats: fatal: CredentialsFile is required")
	}
	if len(cfg.Servers) == 0 {
		return nil, errors.New("nats: fatal: at least one server is required")
	}
	for _, s := range cfg.Servers {
		u, err := url.Parse(s)
		if err != nil {
			return nil, fmt.Errorf("nats: fatal: invalid server URL %q: %w", s, err)
		}
		if u.Scheme != "tls" {
			return nil, fmt.Errorf("nats: fatal: insecure server URL detected (%s), must use tls://", s)
		}
		if u.Host == "" {
			return nil, fmt.Errorf("nats: fatal: server URL missing host (%s)", s)
		}
	}

	caCert, err := os.ReadFile(cfg.CAFile)
	if err != nil {
		return nil, fmt.Errorf("nats: failed to read CAFile: %w", err)
	}
	caCertPool := x509.NewCertPool()
	if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
		return nil, errors.New("nats: fatal: failed to parse valid PEM from CAFile")
	}

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS13,
		RootCAs:    caCertPool,
	}

	if cfg.ClientCertFile != "" || cfg.ClientKeyFile != "" {
		if cfg.ClientCertFile == "" || cfg.ClientKeyFile == "" {
			return nil, errors.New("nats: fatal: both ClientCertFile and ClientKeyFile must be provided for mTLS")
		}
		cert, err := tls.LoadX509KeyPair(cfg.ClientCertFile, cfg.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("nats: failed to load client cert: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	opts := []nats.Option{
		nats.UserCredentials(cfg.CredentialsFile),
		nats.Secure(tlsConfig),
		nats.MaxReconnects(-1),   // Keep reconnecting forever
		nats.ReconnectBufSize(0), // REQ-012: Fail-fast immediately if disconnected
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if onStateChange != nil {
				onStateChange(contracts.LinkConnecting)
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			if onStateChange != nil {
				onStateChange(contracts.LinkConnected)
			}
		}),
	}

	serverURLs := ""
	for i, s := range cfg.Servers {
		if i > 0 {
			serverURLs += ","
		}
		serverURLs += s
	}

	conn, err := nats.Connect(serverURLs, opts...)
	if err != nil {
		return nil, fmt.Errorf("nats: failed to connect: %w", err)
	}

	if onStateChange != nil {
		onStateChange(contracts.LinkConnected)
	}

	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("nats: failed to initialize JetStream: %w", err)
	}

	// Will bind to the KV store in a separate step or here if we know the bucket name
	// For now, we return the client. The KV binding might happen later or in a specific method.

	return &NATSClient{
		target: cfg.Target,
		conn:   conn,
		js:     js,
	}, nil
}

// PublishConfigTrigger publishes a notification to the target device indicating that a new configuration is available.
func (n *NATSClient) PublishConfigTrigger(ctx context.Context, cmd *agentcore.ConfigureNotification) error {
	if ctx == nil {
		return errors.New("context cannot be nil")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context canceled before publish: %w", err)
	}

	if cmd == nil {
		return errors.New("command cannot be nil")
	}

	if err := contracts.ValidateConfigureNotification(cmd); err != nil {
		return fmt.Errorf("invalid ConfigureNotification: %w", err)
	}
	payload, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal ConfigureNotification: %w", err)
	}

	if cmd.Target != n.target {
		return fmt.Errorf("target mismatch: got %q, expected %q", cmd.Target, n.target)
	}
	subject := fmt.Sprintf("cmd.configure.%s", n.target)
	msg := &nats.Msg{
		Subject: subject,
		Data:    payload,
	}
	if err := n.conn.PublishMsg(msg); err != nil {
		return err
	}
	return n.conn.FlushWithContext(ctx)
}

// ExecuteAction publishes an action command (e.g., reboot, factory-reset) to the target device.
func (n *NATSClient) ExecuteAction(ctx context.Context, cmd *agentcore.ActionCommand) error {
	if ctx == nil {
		return errors.New("context cannot be nil")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context canceled before publish: %w", err)
	}

	if cmd == nil {
		return errors.New("command cannot be nil")
	}

	if err := contracts.ValidateActionCommand(cmd); err != nil {
		return fmt.Errorf("invalid ActionCommand: %w", err)
	}
	payload, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal ActionCommand: %w", err)
	}

	if cmd.Target != n.target {
		return fmt.Errorf("target mismatch: got %q, expected %q", cmd.Target, n.target)
	}
	subject := fmt.Sprintf("cmd.action.%s.%s", n.target, cmd.Action)
	msg := &nats.Msg{
		Subject: subject,
		Data:    payload,
	}
	if err := n.conn.PublishMsg(msg); err != nil {
		return err
	}
	return n.conn.FlushWithContext(ctx)
}

// SubscribeResults subscribes to the global results subject for a specific device.
func (n *NATSClient) SubscribeResults(ctx context.Context, handler func(msg *nats.Msg)) (*nats.Subscription, error) {
	if ctx == nil {
		return nil, errors.New("context cannot be nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context canceled before subscribe: %w", err)
	}

	subject := fmt.Sprintf("result.%s", n.target)
	sub, err := n.conn.Subscribe(subject, handler)
	if err != nil {
		return nil, err
	}
	if err := n.conn.FlushWithContext(ctx); err != nil {
		sub.Unsubscribe()
		return nil, err
	}
	return sub, nil
}

// QueryCapabilities performs a synchronous request to fetch the device capabilities.
func (n *NATSClient) QueryCapabilities(ctx context.Context, query *contracts.CloudCapabilitiesQuery) (*agentcore.ResultEnvelope, error) {
	if ctx == nil {
		return nil, errors.New("context cannot be nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context canceled before query: %w", err)
	}

	if query == nil {
		return nil, fmt.Errorf("invalid capabilities query: query cannot be nil")
	}
	if err := query.Validate(); err != nil {
		return nil, fmt.Errorf("invalid capabilities query: %w", err)
	}
	if query.Target != n.target {
		return nil, fmt.Errorf("target mismatch: got %q, expected %q", query.Target, n.target)
	}
	payload, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal capabilities query: %w", err)
	}

	subject := fmt.Sprintf("capabilities.get.%s", n.target)
	msg, err := n.conn.RequestWithContext(ctx, subject, payload)
	if err != nil {
		return nil, fmt.Errorf("request to %s failed: %w", subject, err)
	}

	var env agentcore.ResultEnvelope
	if err := json.Unmarshal(msg.Data, &env); err != nil {
		return nil, fmt.Errorf("failed to unmarshal capabilities response: %w", err)
	}
	if err := contracts.ValidateResultEnvelope(&env); err != nil {
		return nil, fmt.Errorf("invalid capabilities response: %w", err)
	}
	if env.Target != n.target {
		return nil, fmt.Errorf("target mismatch in capabilities response: got %q, expected %q", env.Target, n.target)
	}
	if env.RPCID != query.RPCID {
		return nil, fmt.Errorf("rpc_id mismatch in capabilities response: got %q, expected %q", env.RPCID, query.RPCID)
	}
	return &env, nil
}

// QueryDeviceStatus performs a synchronous request to fetch the device status.
func (n *NATSClient) QueryDeviceStatus(ctx context.Context, query *contracts.CloudDeviceStatusQuery) (*agentcore.StatusEnvelope, error) {
	if ctx == nil {
		return nil, errors.New("context cannot be nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context canceled before query: %w", err)
	}

	if query == nil {
		return nil, fmt.Errorf("invalid status query: query cannot be nil")
	}
	if err := query.Validate(); err != nil {
		return nil, fmt.Errorf("invalid status query: %w", err)
	}
	if query.Target != n.target {
		return nil, fmt.Errorf("target mismatch: got %q, expected %q", query.Target, n.target)
	}
	payload, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal status query: %w", err)
	}

	subject := fmt.Sprintf("status.get.%s", n.target)
	msg, err := n.conn.RequestWithContext(ctx, subject, payload)
	if err != nil {
		return nil, fmt.Errorf("request to %s failed: %w", subject, err)
	}

	var status agentcore.StatusEnvelope
	if err := json.Unmarshal(msg.Data, &status); err != nil {
		return nil, fmt.Errorf("failed to unmarshal status response: %w", err)
	}
	if err := contracts.ValidateStatusEnvelope(&status); err != nil {
		return nil, fmt.Errorf("invalid status response: %w", err)
	}
	if status.Target != n.target {
		return nil, fmt.Errorf("target mismatch in status response: got %q, expected %q", status.Target, n.target)
	}
	if status.RPCID != query.RPCID {
		return nil, fmt.Errorf("rpc_id mismatch in status response: got %q, expected %q", status.RPCID, query.RPCID)
	}
	return &status, nil
}

// SubscribeTelemetry subscribes to local device telemetry.
func (n *NATSClient) SubscribeTelemetry(ctx context.Context, handler func(msg *nats.Msg)) (*nats.Subscription, error) {
	if ctx == nil {
		return nil, errors.New("context cannot be nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context canceled before subscribe: %w", err)
	}

	subject := fmt.Sprintf("telemetry.%s", n.target)
	sub, err := n.conn.Subscribe(subject, handler)
	if err != nil {
		return nil, err
	}
	if err := n.conn.FlushWithContext(ctx); err != nil {
		sub.Unsubscribe()
		return nil, err
	}
	return sub, nil
}

// SubscribeLogs subscribes to local device logs.
func (n *NATSClient) SubscribeLogs(ctx context.Context, handler func(msg *nats.Msg)) (*nats.Subscription, error) {
	if ctx == nil {
		return nil, errors.New("context cannot be nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context canceled before subscribe: %w", err)
	}

	subject := fmt.Sprintf("logs.%s", n.target)
	sub, err := n.conn.Subscribe(subject, handler)
	if err != nil {
		return nil, err
	}
	if err := n.conn.FlushWithContext(ctx); err != nil {
		sub.Unsubscribe()
		return nil, err
	}
	return sub, nil
}

// SubscribeHealth subscribes to local device health reports.
func (n *NATSClient) SubscribeHealth(ctx context.Context, handler func(msg *nats.Msg)) (*nats.Subscription, error) {
	if ctx == nil {
		return nil, errors.New("context cannot be nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context canceled before subscribe: %w", err)
	}

	subject := fmt.Sprintf("health.%s", n.target)
	sub, err := n.conn.Subscribe(subject, handler)
	if err != nil {
		return nil, err
	}
	if err := n.conn.FlushWithContext(ctx); err != nil {
		sub.Unsubscribe()
		return nil, err
	}
	return sub, nil
}

// SubscribeState subscribes to local device state changes.
func (n *NATSClient) SubscribeState(ctx context.Context, handler func(msg *nats.Msg)) (*nats.Subscription, error) {
	if ctx == nil {
		return nil, errors.New("context cannot be nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context canceled before subscribe: %w", err)
	}

	subject := fmt.Sprintf("status.%s", n.target)
	sub, err := n.conn.Subscribe(subject, handler)
	if err != nil {
		return nil, err
	}
	if err := n.conn.FlushWithContext(ctx); err != nil {
		sub.Unsubscribe()
		return nil, err
	}
	return sub, nil
}

func (n *NATSClient) getKV(ctx context.Context) (jetstream.KeyValue, error) {
	n.kvMu.Lock()
	defer n.kvMu.Unlock()

	if n.kv != nil {
		return n.kv, nil
	}

	kv, err := n.js.KeyValue(ctx, "cfg_desired")
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		kv, err = n.js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "cfg_desired"})
	}
	if err != nil {
		return nil, fmt.Errorf("failed to bind to KV store 'cfg_desired': %w", err)
	}

	n.kv = kv
	return kv, nil
}

// WriteDesiredConfig writes the desired configuration to the JetStream KeyValue store.
func (n *NATSClient) WriteDesiredConfig(ctx context.Context, record agentcore.DesiredConfigRecord) (uint64, error) {
	if ctx == nil {
		return 0, errors.New("context cannot be nil")
	}
	if err := ctx.Err(); err != nil {
		return 0, fmt.Errorf("context canceled before write: %w", err)
	}

	if err := contracts.ValidateDesiredConfigRecord(&record); err != nil {
		return 0, fmt.Errorf("invalid DesiredConfigRecord: %w", err)
	}
	if record.Target != n.target {
		return 0, fmt.Errorf("target mismatch: got %q, expected %q", record.Target, n.target)
	}

	kv, err := n.getKV(ctx)
	if err != nil {
		return 0, err
	}

	payload, err := json.Marshal(record)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal desired config record: %w", err)
	}

	key := fmt.Sprintf("desired.%s", n.target)
	rev, err := kv.Put(ctx, key, payload)
	if err != nil {
		return 0, fmt.Errorf("failed to write config to KV: %w", err)
	}
	return rev, nil
}

// GetDesiredConfigMetadata retrieves the metadata of the desired configuration from the JetStream KeyValue store.
func (n *NATSClient) GetDesiredConfigMetadata(ctx context.Context) (uint64, string, error) {
	if ctx == nil {
		return 0, "", errors.New("context cannot be nil")
	}
	if err := ctx.Err(); err != nil {
		return 0, "", fmt.Errorf("context canceled before read: %w", err)
	}

	kv, err := n.getKV(ctx)
	if err != nil {
		return 0, "", err
	}

	key := fmt.Sprintf("desired.%s", n.target)
	entry, err := kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return 0, "", nil // Not an error if it just doesn't exist yet
		}
		return 0, "", fmt.Errorf("failed to get config from KV: %w", err)
	}

	// The metadata string could be a hash, for now we can just return the string format of the revision
	var record agentcore.DesiredConfigRecord
	if err := json.Unmarshal(entry.Value(), &record); err != nil {
		return 0, "", fmt.Errorf("failed to decode DesiredConfigRecord: %w", err)
	}
	if err := contracts.ValidateDesiredConfigRecord(&record); err != nil {
		return 0, "", fmt.Errorf("invalid stored DesiredConfigRecord: %w", err)
	}
	if record.Target != n.target {
		return 0, "", fmt.Errorf("target mismatch in stored config: got %q, expected %q", record.Target, n.target)
	}

	return entry.Revision(), record.UUID, nil
}

// Close gracefully drains subscriptions and closes the NATS connection.
func (n *NATSClient) Close(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context cannot be nil")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context canceled before close: %w", err)
	}

	if n.conn == nil {
		return nil
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- n.conn.Drain()
	}()

	select {
	case <-ctx.Done():
		n.conn.Close()
		return fmt.Errorf("timeout draining connection: %w", ctx.Err())
	case err := <-errCh:
		if err != nil {
			n.conn.Close()
			return fmt.Errorf("failed to drain connection: %w", err)
		}
		// Drain automatically closes the connection upon completion.
		return nil
	}
}
