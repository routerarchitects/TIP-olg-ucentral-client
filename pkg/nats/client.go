package nats

import (
	"context"
	"errors"
	"fmt"

	"github.com/Telecominfraproject/olg-nats-agent-core/agentcore"
	"github.com/nats-io/nats.go"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/config"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/contracts"
)

type NATSClient struct {
	target      string
	agentClient *agentcore.Client
}

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
				Enabled: cfg.CAFile != "" || cfg.CredentialsFile != "",
				CAFile:  cfg.CAFile,
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

	agentClient, err := agentcore.New(agentCfg, clientOpts...)
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
	if cmd.Target != n.target {
		return fmt.Errorf("target mismatch: got %q, expected %q", cmd.Target, n.target)
	}

	if n.agentClient != nil {
		_, err := n.agentClient.SubmitConfigure(ctx, *cmd)
		return err
	}
	return nil
}

func (n *NATSClient) ExecuteAction(ctx context.Context, cmd *agentcore.ActionCommand) error {
	if ctx == nil || ctx.Err() != nil {
		return errors.New("invalid or canceled context")
	}
	if err := contracts.ValidateActionCommand(cmd); err != nil {
		return fmt.Errorf("invalid action command: %w", err)
	}
	if cmd.Target != n.target {
		return fmt.Errorf("target mismatch: got %q, expected %q", cmd.Target, n.target)
	}

	if n.agentClient != nil {
		_, err := n.agentClient.SubmitAction(ctx, *cmd)
		return err
	}
	return nil
}

func (n *NATSClient) QueryCapabilities(ctx context.Context, query *contracts.CloudCapabilitiesQuery) ([]byte, error) {
	return nil, errors.New("QueryCapabilities not implemented in agentcore")
}

func (n *NATSClient) QueryDeviceStatus(ctx context.Context, query *contracts.CloudDeviceStatusQuery) (*agentcore.StatusEnvelope, error) {
	return nil, errors.New("QueryDeviceStatus not implemented in agentcore")
}

func (n *NATSClient) SubscribeResults(ctx context.Context, handler func(agentcore.ResultEnvelope)) (*nats.Subscription, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, errors.New("invalid or canceled context")
	}
	if n.agentClient != nil {
		err := n.agentClient.RegisterResultHandler(n.target, func(ctx context.Context, msg agentcore.ResultEnvelope) error {
			handler(msg)
			return nil
		})
		return nil, err
	}
	return nil, nil
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
