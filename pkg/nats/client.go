package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/Telecominfraproject/olg-nats-agent-core/agentcore"
	"github.com/nats-io/nats.go"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/config"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/contracts"
)

type NATSClient struct {
	target      string
	agentClient *agentcore.Client
}

// agentcoreNew is a package-level variable to allow mocking in tests
var agentcoreNew = agentcore.New

func NewNATSClient(agentName string, cfg config.NATSConfig, onStateChange func(contracts.LinkState)) (*NATSClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("nats: invalid configuration: %w", err)
	}
	if err := contracts.ValidateNATSTarget(agentName); err != nil {
		return nil, fmt.Errorf("nats: fatal: %w", err)
	}

	agentCfg := agentcore.Config{
		AgentName: agentName,
		Version:   "1.0",
		NATS: agentcore.NATSConfig{
			Servers:         cfg.Servers,
			CredentialsFile: cfg.CredentialsFile,
			TLS: &agentcore.TLSConfig{
				Enabled:  allServersUseTLS(cfg.Servers),
				CAFile:   cfg.CAFile,
				CertFile: cfg.ClientCertFile,
				KeyFile:  cfg.ClientKeyFile,
			},
		},
		Subjects: agentcore.SubjectConfig{
			ConfigurePattern: "cmd.configure.%s",
			ActionPattern:    "cmd.action.%s.%s",
			ResultPattern:    "result.%s",
			StatusPattern:    "status.%s",
			HealthPattern:    "health.%s",
		},
	}

	clientOpts := []agentcore.Option{
		agentcore.WithReconnectHandler(func() {
			if onStateChange != nil {
				onStateChange(contracts.LinkConnected)
			}
		}),
	}

	agentClient, err := agentcoreNew(agentCfg, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("nats: failed to initialize agentcore client: %w", err)
	}
	if err := agentClient.Start(context.Background()); err != nil {
		return nil, fmt.Errorf("nats: failed to start agentcore client: %w", err)
	}

	if onStateChange != nil {
		onStateChange(contracts.LinkConnected)
	}

	return &NATSClient{
		target:      agentName,
		agentClient: agentClient,
	}, nil
}

func (n *NATSClient) SubmitConfigure(ctx context.Context, cmd *agentcore.ConfigureCommand) error {
	if ctx == nil || ctx.Err() != nil {
		return errors.New("invalid or canceled context")
	}
	if cmd == nil {
		return errors.New("command cannot be nil")
	}
	if cmd.Version != contracts.EnvelopeVersion {
		return fmt.Errorf("invalid envelope version: expected %q, got %q", contracts.EnvelopeVersion, cmd.Version)
	}
	if len(cmd.Payload) == 0 {
		return errors.New("command payload cannot be empty")
	}
	// uCentral Payload Validation
	if err := contracts.ValidateCommandPayload(contracts.CommandConfigure, "", cmd.Payload); err != nil {
		return fmt.Errorf("invalid configure payload: %w", err)
	}

	uuid, err := strconv.ParseInt(cmd.UUID, 10, 64)
	if err != nil || uuid <= 0 {
		return errors.New("uuid must be a positive int64")
	}

	var cfgReq contracts.CloudConfigureRequest
	if err := json.Unmarshal(cmd.Payload, &cfgReq); err == nil {
		if payloadUUID, err := cfgReq.EffectiveUUID(); err == nil {
			if payloadUUID != uuid {
				return fmt.Errorf("envelope UUID %q does not match payload UUID %d", cmd.UUID, payloadUUID)
			}
		}
	}
	// Note: We intentionally do NOT restrict cmd.Target == n.target here.
	// uCentral acts as a 1-to-N gateway for local services.
	// The internal Request Manager is trusted to supply the correct downstream target.
	if err := contracts.ValidateNATSTarget(cmd.Target); err != nil {
		return fmt.Errorf("invalid target: %w", err)
	}

	if n.agentClient == nil {
		return errors.New("agentClient is not initialized")
	}

	_, err = n.agentClient.SubmitConfigure(ctx, *cmd)
	return err
}

func allServersUseTLS(servers []string) bool {
	if len(servers) == 0 {
		return false
	}
	for _, srv := range servers {
		u, err := url.Parse(srv)
		if err != nil || strings.ToLower(u.Scheme) != "tls" {
			return false
		}
	}
	return true
}

func (n *NATSClient) ExecuteAction(ctx context.Context, cmd *agentcore.ActionCommand) error {
	if ctx == nil || ctx.Err() != nil {
		return errors.New("invalid or canceled context")
	}
	if cmd == nil {
		return errors.New("command cannot be nil")
	}
	if len(cmd.Payload) == 0 {
		return errors.New("command payload cannot be empty")
	}
	if cmd.Version != contracts.EnvelopeVersion {
		return fmt.Errorf("invalid envelope version: expected %q, got %q", contracts.EnvelopeVersion, cmd.Version)
	}

	command := contracts.CommandType(cmd.CommandType)
	action := contracts.ActionType(cmd.Action)

	if !contracts.ValidCommandAction(command, action) {
		return fmt.Errorf("invalid command/action combination: command=%q action=%q", command, action)
	}

	// uCentral Payload Validation
	if err := contracts.ValidateCommandPayload(command, action, cmd.Payload); err != nil {
		return fmt.Errorf("invalid action payload: %w", err)
	}
	// Note: We intentionally do NOT restrict cmd.Target == n.target here.
	// uCentral acts as a 1-to-N gateway for local services.
	// The internal Request Manager is trusted to supply the correct downstream target.
	if err := contracts.ValidateNATSTarget(cmd.Target); err != nil {
		return fmt.Errorf("invalid target: %w", err)
	}

	if n.agentClient == nil {
		return errors.New("agentClient is not initialized")
	}

	_, err := n.agentClient.SubmitAction(ctx, *cmd)
	return err
}

func (n *NATSClient) QueryCapabilities(ctx context.Context, query *contracts.CloudCapabilitiesQuery) ([]byte, error) {
	return nil, errors.New("QueryCapabilities not implemented in agentcore")
}

func (n *NATSClient) QueryDeviceStatus(ctx context.Context, query *contracts.CloudDeviceStatusQuery) (*agentcore.StatusEnvelope, error) {
	return nil, errors.New("QueryDeviceStatus not implemented in agentcore")
}

func (n *NATSClient) SubscribeResults(ctx context.Context, target string, handler func(msg agentcore.ResultEnvelope)) error {
	if ctx == nil || ctx.Err() != nil {
		return errors.New("invalid or canceled context")
	}
	if err := contracts.ValidateNATSTarget(target); err != nil {
		return fmt.Errorf("invalid target: %w", err)
	}
	if handler == nil {
		return errors.New("handler cannot be nil")
	}
	if n.agentClient == nil {
		return errors.New("agentClient is not initialized")
	}

	err := n.agentClient.RegisterResultHandler(target, func(ctx context.Context, msg agentcore.ResultEnvelope) error {
		if err := validateResultEnvelope(target, msg); err != nil {
			return err
		}
		handler(msg)
		return nil
	})
	return err
}

func validateResultEnvelope(expectedTarget string, msg agentcore.ResultEnvelope) error {
	if msg.Version != contracts.EnvelopeVersion {
		return fmt.Errorf("invalid envelope version: expected %q, got %q", contracts.EnvelopeVersion, msg.Version)
	}
	if msg.Target != expectedTarget {
		return fmt.Errorf("target mismatch: got %q, expected %q", msg.Target, expectedTarget)
	}
	if msg.RPCID == "" {
		return errors.New("rpc_id cannot be empty")
	}
	if msg.Timestamp.IsZero() {
		return errors.New("timestamp cannot be zero")
	}

	if !contracts.ResultType(msg.Result).Valid() {
		return fmt.Errorf("invalid result state: %q", msg.Result)
	}

	command := contracts.CommandType(msg.CommandType)
	action := contracts.ActionType(msg.Action)

	if !contracts.ValidCommandAction(command, action) {
		return fmt.Errorf("invalid command/action combination: command=%q action=%q", command, action)
	}

	if msg.CommandType == string(contracts.CommandConfigure) {
		uuid, err := strconv.ParseInt(msg.UUID, 10, 64)
		if err != nil || uuid <= 0 {
			return errors.New("uuid must be a positive int64 for configure results")
		}
	}

	if err := contracts.ValidateResultPayload(contracts.CommandType(msg.CommandType), contracts.ActionType(msg.Action), msg.Payload); err != nil {
		return fmt.Errorf("invalid result payload: %w", err)
	}
	return nil
}

func (n *NATSClient) SubscribeTelemetry(ctx context.Context, handler func(msg *nats.Msg)) (*nats.Subscription, error) {
	return nil, errors.New("SubscribeTelemetry not implemented in agentcore")
}

func (n *NATSClient) SubscribeLogs(ctx context.Context, handler func(msg *nats.Msg)) (*nats.Subscription, error) {
	return nil, errors.New("SubscribeLogs not implemented in agentcore")
}

func (n *NATSClient) SubscribeHealth(ctx context.Context, handler func(msg *nats.Msg)) (*nats.Subscription, error) {
	return nil, errors.New("SubscribeHealth not implemented in agentcore")
}

func (n *NATSClient) SubscribeState(ctx context.Context, handler func(msg *nats.Msg)) (*nats.Subscription, error) {
	return nil, errors.New("SubscribeState not implemented in agentcore")
}

func (n *NATSClient) GetDesiredConfigMetadata(ctx context.Context) (uint64, string, error) {
	return 0, "", errors.New("GetDesiredConfigMetadata not implemented in agentcore")
}

func (n *NATSClient) Close(ctx context.Context) error {
	if n.agentClient != nil {
		return n.agentClient.Close(ctx)
	}
	return nil
}
