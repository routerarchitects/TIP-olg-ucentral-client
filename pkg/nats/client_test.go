package nats

import (
	"context"
	"testing"
	"time"

	"github.com/Telecominfraproject/olg-nats-agent-core/agentcore"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/contracts"
)

func TestSubmitConfigure_Validation(t *testing.T) {
	client := &NATSClient{target: "target-123"}

	// Test nil context
	err := client.SubmitConfigure(nil, &agentcore.ConfigureCommand{Target: "target-123", Version: contracts.EnvelopeVersion, RPCID: "123", Payload: []byte("{}"), Timestamp: time.Now()})
	if err == nil {
		t.Fatal("expected error for nil context")
	}

	// Test validation failure
	ctx := context.Background()
	err = client.SubmitConfigure(ctx, nil)
	if err == nil {
		t.Fatal("expected error for nil command")
	}

	// Test target mismatch
	err = client.SubmitConfigure(ctx, &agentcore.ConfigureCommand{
		Target: "wrong-target", Version: contracts.EnvelopeVersion, RPCID: "123", Payload: []byte("{}"), Timestamp: time.Now(),
	})
	if err == nil {
		t.Fatal("expected error for target mismatch")
	}
}

func TestExecuteAction_Validation(t *testing.T) {
	client := &NATSClient{target: "target-123"}
	ctx := context.Background()

	// Test validation failure
	err := client.ExecuteAction(ctx, &agentcore.ActionCommand{})
	if err == nil {
		t.Fatal("expected error for invalid action")
	}

	// Test target mismatch
	err = client.ExecuteAction(ctx, &agentcore.ActionCommand{
		Target: "wrong-target", Version: contracts.EnvelopeVersion, RPCID: "123", CommandType: "reboot", Payload: []byte("{}"), Timestamp: time.Now(),
	})
	if err == nil {
		t.Fatal("expected error for target mismatch")
	}
}
