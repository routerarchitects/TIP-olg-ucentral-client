package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

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

func NewNATSClient(cfg config.NATSConfig, onStateChange func(contracts.LinkState)) (*NATSClient, error) {
	if err := contracts.ValidateNATSTarget(cfg.Target); err != nil {
		return nil, fmt.Errorf("nats: fatal: %w", err)
	}

	agentCfg := agentcore.Config{
		AgentName: cfg.Target,
		Version:   "1.0",
		NATS: agentcore.NATSConfig{
			Servers:         cfg.Servers,
			CredentialsFile: cfg.CredentialsFile,
			TLS: &agentcore.TLSConfig{
				Enabled:  cfg.CAFile != "" || cfg.ClientCertFile != "" || cfg.ClientKeyFile != "",
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
		target:      cfg.Target,
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
	if cmd.Target != n.target {
		return fmt.Errorf("target mismatch: got %q, expected %q", cmd.Target, n.target)
	}

	if n.agentClient == nil {
		return errors.New("agentClient is not initialized")
	}

	_, err = n.agentClient.SubmitConfigure(ctx, *cmd)
	return err
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
	if cmd.Target != n.target {
		return fmt.Errorf("target mismatch: got %q, expected %q", cmd.Target, n.target)
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

func (n *NATSClient) SubscribeResults(ctx context.Context, handler func(agentcore.ResultEnvelope)) error {
	if ctx == nil || ctx.Err() != nil {
		return errors.New("invalid or canceled context")
	}
	if handler == nil {
		return errors.New("handler cannot be nil")
	}
	if n.agentClient == nil {
		return errors.New("agentClient is not initialized")
	}

	err := n.agentClient.RegisterResultHandler(n.target, func(ctx context.Context, msg agentcore.ResultEnvelope) error {
		if err := validateResultEnvelope(msg); err != nil {
			return err
		}
		handler(msg)
		return nil
	})
	return err
}

func validateResultEnvelope(msg agentcore.ResultEnvelope) error {
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
