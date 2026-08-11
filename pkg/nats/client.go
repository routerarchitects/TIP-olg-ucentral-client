package nats

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/Telecominfraproject/olg-nats-agent-core/agentcore"
	"github.com/nats-io/nats.go"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/config"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/contracts"
)

type NATSClient struct {
	conn *nats.Conn
	js   nats.JetStreamContext
	kv   nats.KeyValue
	kvMu sync.Mutex
}

// NewNATSClient initializes a NATS connection.
// SECURITY CONTRACT: This constructor MUST enforce tls.Config{MinVersion: tls.VersionTLS13}.
// It must return a fatal error if CAFile is empty, or if any Server URL is insecure.
func NewNATSClient(cfg config.NATSConfig, onStateChange func(contracts.LinkState)) (*NATSClient, error) {
	if cfg.CAFile == "" {
		return nil, errors.New("nats: fatal: CAFile is required")
	}
	if cfg.CredentialsFile == "" {
		return nil, errors.New("nats: fatal: CredentialsFile is required")
	}
	if len(cfg.Servers) == 0 {
		return nil, errors.New("nats: fatal: at least one server is required")
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

	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("nats: failed to initialize JetStream: %w", err)
	}

	// Will bind to the KV store in a separate step or here if we know the bucket name
	// For now, we return the client. The KV binding might happen later or in a specific method.

	return &NATSClient{
		conn: conn,
		js:   js,
	}, nil
}

// PublishConfigTrigger publishes a configuration trigger to the NATS bus.
func (n *NATSClient) PublishConfigTrigger(ctx context.Context, cmd *agentcore.ConfigureNotification) error {
	if err := contracts.ValidateConfigureNotification(cmd); err != nil {
		return fmt.Errorf("invalid ConfigureNotification: %w", err)
	}
	payload, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal ConfigureNotification: %w", err)
	}

	subject := fmt.Sprintf("cmd.configure.%s", cmd.Target)
	msg := &nats.Msg{
		Subject: subject,
		Data:    payload,
	}
	if err := n.conn.PublishMsg(msg); err != nil {
		return err
	}
	return n.conn.FlushWithContext(ctx)
}

// ExecuteAction publishes an action command to the NATS bus.
func (n *NATSClient) ExecuteAction(ctx context.Context, cmd *agentcore.ActionCommand) error {
	if err := contracts.ValidateActionCommand(cmd); err != nil {
		return fmt.Errorf("invalid ActionCommand: %w", err)
	}
	payload, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal ActionCommand: %w", err)
	}

	subject := fmt.Sprintf("cmd.action.%s.%s", cmd.Target, cmd.Action)
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
func (n *NATSClient) SubscribeResults(serial string, handler func(msg *nats.Msg)) (*nats.Subscription, error) {
	subject := fmt.Sprintf("result.%s", serial)
	return n.conn.Subscribe(subject, handler)
}

// QueryCapabilities performs a synchronous request to fetch the device capabilities.
func (n *NATSClient) QueryCapabilities(ctx context.Context, query *contracts.CloudCapabilitiesQuery, targetSerial string) (*agentcore.ResultEnvelope, error) {
	subject := fmt.Sprintf("capabilities.get.%s", targetSerial)
	msg, err := n.conn.RequestWithContext(ctx, subject, []byte("{}"))
	if err != nil {
		return nil, fmt.Errorf("request to %s failed: %w", subject, err)
	}

	var env agentcore.ResultEnvelope
	if err := json.Unmarshal(msg.Data, &env); err != nil {
		return nil, fmt.Errorf("failed to unmarshal capabilities response: %w", err)
	}
	return &env, nil
}

// QueryDeviceStatus performs a synchronous request to fetch the device status.
func (n *NATSClient) QueryDeviceStatus(ctx context.Context, query *contracts.CloudDeviceStatusQuery, targetSerial string) (*contracts.DeviceStatus, error) {
	subject := fmt.Sprintf("status.get.%s", targetSerial)
	msg, err := n.conn.RequestWithContext(ctx, subject, []byte("{}"))
	if err != nil {
		return nil, fmt.Errorf("request to %s failed: %w", subject, err)
	}

	var status contracts.DeviceStatus
	if err := json.Unmarshal(msg.Data, &status); err != nil {
		return nil, fmt.Errorf("failed to unmarshal status response: %w", err)
	}
	return &status, nil
}

// SubscribeTelemetry subscribes to local device telemetry.
func (n *NATSClient) SubscribeTelemetry(serial string, handler func(msg *nats.Msg)) (*nats.Subscription, error) {
	subject := fmt.Sprintf("telemetry.%s", serial)
	return n.conn.Subscribe(subject, handler)
}

// SubscribeLogs subscribes to local device logs.
func (n *NATSClient) SubscribeLogs(serial string, handler func(msg *nats.Msg)) (*nats.Subscription, error) {
	subject := fmt.Sprintf("logs.%s", serial)
	return n.conn.Subscribe(subject, handler)
}

// SubscribeHealth subscribes to local device health reports.
func (n *NATSClient) SubscribeHealth(serial string, handler func(msg *nats.Msg)) (*nats.Subscription, error) {
	subject := fmt.Sprintf("health.%s", serial)
	return n.conn.Subscribe(subject, handler)
}

// SubscribeState subscribes to local device state changes.
func (n *NATSClient) SubscribeState(serial string, handler func(msg *nats.Msg)) (*nats.Subscription, error) {
	subject := fmt.Sprintf("status.%s", serial)
	return n.conn.Subscribe(subject, handler)
}

func (n *NATSClient) getKV() (nats.KeyValue, error) {
	n.kvMu.Lock()
	defer n.kvMu.Unlock()

	if n.kv != nil {
		return n.kv, nil
	}

	kv, err := n.js.KeyValue("cfg_desired")
	if err == nats.ErrBucketNotFound {
		kv, err = n.js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "cfg_desired"})
	}
	if err != nil {
		return nil, fmt.Errorf("failed to bind to KV store 'cfg_desired': %w", err)
	}

	n.kv = kv
	return kv, nil
}

// WriteDesiredConfig writes the desired configuration to the JetStream KeyValue store.
func (n *NATSClient) WriteDesiredConfig(ctx context.Context, serial string, config []byte) (uint64, error) {
	kv, err := n.getKV()
	if err != nil {
		return 0, err
	}

	key := fmt.Sprintf("desired.%s", serial)
	rev, err := kv.Put(key, config)
	if err != nil {
		return 0, fmt.Errorf("failed to write config to KV: %w", err)
	}
	return rev, nil
}

// GetDesiredConfigMetadata retrieves the metadata of the desired configuration from the JetStream KeyValue store.
func (n *NATSClient) GetDesiredConfigMetadata(ctx context.Context, serial string) (uint64, string, error) {
	kv, err := n.getKV()
	if err != nil {
		return 0, "", err
	}

	key := fmt.Sprintf("desired.%s", serial)
	entry, err := kv.Get(key)
	if err != nil {
		if errors.Is(err, nats.ErrKeyNotFound) {
			return 0, "", nil // Not an error if it just doesn't exist yet
		}
		return 0, "", fmt.Errorf("failed to get config from KV: %w", err)
	}

	// The metadata string could be a hash, for now we can just return the string format of the revision
	return entry.Revision(), fmt.Sprintf("%d", entry.Revision()), nil
}
