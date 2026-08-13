package nats

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Telecominfraproject/olg-nats-agent-core/agentcore"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/config"
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

	// Test empty target
	err = client.SubmitConfigure(ctx, &agentcore.ConfigureCommand{
		Target:    "",
		Version:   contracts.EnvelopeVersion,
		RPCID:     "123",
		UUID:      "999",
		Payload:   json.RawMessage(`{"serial":"serial-123","uuid":999,"config":{}}`),
		Timestamp: time.Now(),
	})
	if err == nil || !strings.Contains(err.Error(), "target cannot be empty") {
		t.Fatalf("expected empty target error, got: %v", err)
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

	// Test empty target
	err = client.ExecuteAction(ctx, &agentcore.ActionCommand{
		Target:      "",
		Version:     contracts.EnvelopeVersion,
		RPCID:       "123",
		CommandType: "reboot",
		Action:      "reboot",
		Payload:     json.RawMessage(`{"serial":"serial-123","when":0}`),
		Timestamp:   time.Now(),
	})
	if err == nil || !strings.Contains(err.Error(), "target cannot be empty") {
		t.Fatalf("expected empty target error, got: %v", err)
	}
}

func TestSubscribeResults_NilHandler(t *testing.T) {
	client := &NATSClient{}
	err := client.SubscribeResults(context.Background(), "ucentral-test", nil)
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

func TestNewNATSClient_ConfigWiring(t *testing.T) {
	tmpDir := t.TempDir()
	credsFile := filepath.Join(tmpDir, "creds")
	os.WriteFile(credsFile, []byte("-----BEGIN USER NKEY SEED-----\n"), 0644)

	priv, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	template := x509.Certificate{SerialNumber: big.NewInt(1), IsCA: true}
	derBytes, _ := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	caFile := filepath.Join(tmpDir, "ca.pem")
	os.WriteFile(caFile, caPEM, 0644)

	keyBytes, _ := x509.MarshalECPrivateKey(priv)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})
	keyFile := filepath.Join(tmpDir, "key.pem")
	os.WriteFile(keyFile, keyPEM, 0644)

	cfg := config.NATSConfig{
		Servers:         []string{"tls://127.0.0.1:4222"},
		CredentialsFile: credsFile,
		CAFile:          caFile,
		ClientCertFile:  caFile, // using CA as client cert for simplicity
		ClientKeyFile:   keyFile,
	}

	var capturedConfig agentcore.Config

	// Mock the factory
	originalFactory := agentcoreNew
	defer func() { agentcoreNew = originalFactory }()

	agentcoreNew = func(c agentcore.Config, opts ...agentcore.Option) (*agentcore.Client, error) {
		capturedConfig = c
		return nil, errors.New("mock intercept")
	}

	_, err := NewNATSClient("ucentral-agent", cfg, nil)
	if err == nil || err.Error() != "nats: failed to initialize agentcore client: mock intercept" {
		t.Fatalf("expected mock intercept error, got: %v", err)
	}

	// Assert the wiring is correct
	if capturedConfig.AgentName != "ucentral-agent" {
		t.Errorf("expected AgentName %q, got %q", "ucentral-agent", capturedConfig.AgentName)
	}
	if capturedConfig.NATS.TLS == nil {
		t.Fatal("expected TLSConfig to be non-nil")
	}
	if !capturedConfig.NATS.TLS.Enabled {
		t.Error("expected TLS to be enabled")
	}
	if capturedConfig.NATS.TLS.CAFile != cfg.CAFile {
		t.Errorf("expected CAFile %q, got %q", cfg.CAFile, capturedConfig.NATS.TLS.CAFile)
	}

	expectedConfigurePattern := "cmd.configure.%s"
	if capturedConfig.Subjects.ConfigurePattern != expectedConfigurePattern {
		t.Errorf("expected ConfigurePattern %q, got %q", expectedConfigurePattern, capturedConfig.Subjects.ConfigurePattern)
	}
}

func TestSubscribeResults_Validation(t *testing.T) {
	client := &NATSClient{}
	// We can't easily unit test the handler without mocking agentClient,
	// but we can test that calling it with a nil handler fails.
	err := client.SubscribeResults(context.Background(), "target-123", nil)
	if err == nil || err.Error() != "handler cannot be nil" {
		t.Errorf("expected nil handler error, got: %v", err)
	}
}

func TestValidateResultEnvelope_Negative(t *testing.T) {
	tests := []struct {
		name           string
		expectedTarget string
		msg            agentcore.ResultEnvelope
		wantErr        string
	}{
		{
			name:           "invalid configure uuid type",
			expectedTarget: "target-123",
			msg: agentcore.ResultEnvelope{
				Version:     contracts.EnvelopeVersion,
				Target:      "target-123",
				Result:      string(contracts.ResultSuccess),
				CommandType: string(contracts.CommandConfigure),
				UUID:        "not-a-number",
			},
			wantErr: "uuid must be a positive int64 for configure results",
		},
		{
			name:           "negative configure uuid",
			expectedTarget: "target-123",
			msg: agentcore.ResultEnvelope{
				Version:     contracts.EnvelopeVersion,
				Target:      "target-123",
				Result:      string(contracts.ResultSuccess),
				CommandType: string(contracts.CommandConfigure),
				UUID:        "-1",
			},
			wantErr: "uuid must be a positive int64 for configure results",
		},
		{
			name:           "invalid payload for result",
			expectedTarget: "target-123",
			msg: agentcore.ResultEnvelope{
				Version:     contracts.EnvelopeVersion,
				Target:      "target-123",
				Result:      string(contracts.ResultSuccess),
				CommandType: string(contracts.CommandConfigure),
				UUID:        "123",
				Payload:     []byte(`{bad json}`), // Invalid payload
			},
			wantErr: "invalid result payload",
		},
		{
			name:           "invalid version",
			expectedTarget: "target-123",
			msg: agentcore.ResultEnvelope{
				Version:     "2.0",
				Target:      "target-123",
				Result:      string(contracts.ResultSuccess),
				CommandType: string(contracts.CommandConfigure),
			},
			wantErr: "invalid envelope version",
		},
		{
			name:           "invalid result enum",
			expectedTarget: "target-123",
			msg: agentcore.ResultEnvelope{
				Version:     contracts.EnvelopeVersion,
				Target:      "target-123",
				Result:      "mostly_done",
				CommandType: string(contracts.CommandConfigure),
			},
			wantErr: "invalid result state",
		},
		{
			name:           "invalid command/action matrix",
			expectedTarget: "target-123",
			msg: agentcore.ResultEnvelope{
				Version:     contracts.EnvelopeVersion,
				Target:      "target-123",
				Result:      string(contracts.ResultSuccess),
				CommandType: string(contracts.CommandQuery),
				Action:      string(contracts.ActionReboot),
			},
			wantErr: "invalid command/action combination",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateResultEnvelope(tt.expectedTarget, tt.msg)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("expected error containing %q, got: %v", tt.wantErr, err)
			}
		})
	}
}
