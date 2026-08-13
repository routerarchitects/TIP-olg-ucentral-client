package nats

import (
	"context"
	"encoding/json"
	"strings"
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

func TestSubscribeResults_NilHandler(t *testing.T) {
	client := &NATSClient{
		target: "serial-123",
	}
	err := client.SubscribeResults(context.Background(), nil)
	if err == nil || err.Error() != "handler cannot be nil" {
		t.Errorf("Expected 'handler cannot be nil' error, got: %v", err)
	}
}
func TestSubmitConfigure_UUIDMismatch_Plain(t *testing.T) {
	client := &NATSClient{target: "serial-123", agentClient: &agentcore.Client{}}
	
	// Valid envelope, mismatched payload UUID
	cmd := &agentcore.ConfigureCommand{
		Version:   "1.0",
		RPCID:     "rpc1",
		Target:    "serial-123",
		UUID:      "123",
		Timestamp: time.Now(),
		Payload:   json.RawMessage(`{"serial":"serial-123","uuid":999,"config":{}}`),
	}
	
	err := client.SubmitConfigure(context.Background(), cmd)
	if err == nil || !strings.Contains(err.Error(), "does not match payload UUID") {
		t.Errorf("Expected UUID mismatch error, got: %v", err)
	}
}
