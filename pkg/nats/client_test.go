package nats

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Telecominfraproject/olg-nats-agent-core/agentcore"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/config"
)

func TestNewNATSClient_Validation(t *testing.T) {
	tmpDir := t.TempDir()
	validCAFile := filepath.Join(tmpDir, "ca.pem")
	validCredsFile := filepath.Join(tmpDir, "creds.txt")

	// Dummy PEM (not a real CA, but enough to pass file existence, though AppendCertsFromPEM will fail if not valid)
	// We'll write a real-looking PEM so it passes the append check
	dummyPEM := `-----BEGIN CERTIFICATE-----
MIIDdTCCAl2gAwIBAgILBAAAAAABFUtaw5QwDQYJKoZIhvcNAQEFBQAwVzELMAkG
A1UEBhMCQkUxGTAXBgNVBAoTEEdsb2JhbFNpZ24gbnYtc2ExEDAOBgNVBAsTB1Jv
b3QgQ0ExGzAZBgNVBAMTEkdsb2JhbFNpZ24gUm9vdCBDQTAeFw05ODA5MDExMjAw
MDBaFw0yODAxMjgxMjAwMDBaMFcxCzAJBgNVBAYTAkJFMRkwFwYDVQQKExBHbG9i
YWxTaWduIG52LXNhMRAwDgYDVQQLEwdSb290IENBMRswGQYDVQQDExJHbG9iYWxT
aWduIFJvb3QgQ0EwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDaDuaZ
jc6j40+Kfvvxi4Mla+pIH/EqsLmVEQS98GPR4mdmzxzdzxtIK+6NiY6arymAZavp
xy0Sy6scTHAHoT0KMM0VjU/43dSMUBUc71DuxC73/OlS8pNl4q5SmClinPCdQA5l
WGC43onw3b4xUG22LwI0qU1R41aI+8p8n1j8o7/H7H4xL3E1M/3Z3+R4O/I2Jm3H
YlO+n2W+B0Z4K0w4z2M8rW+R2aI8vC3tJ1C2m+7bK2W4L0C/F0cO4u4/j1s3m1Rz
YvO+J+J+N3J3h3V8V5xHq2X3E3K1q+V+L3y3Y+Q1W5t/E5u/R4w8D4Q+eL8H8B4X
K3aT+R1wP3s2R+L5AgMBAAGjQjBAMA4GA1UdDwEB/wQEAwIBBjAPBgNVHRMBAf8E
BTADAQH/MB0GA1UdDgQWBBRge2YaRQ2XyolQL30EzTSo//z9SzANBgkqhkiG9w0B
AQUFAAOCAQEA1nPnfE920I2/7ndTEocwKV8cO3wzOS41i1E8g3hYjQzU1pT4u4U7
R8bK+B2M1q1G+1rU1O6M7C+6z1D/m8V7o6R8aL3fXy1T2uL8y9R7R5V8S7dZz8m+
9Z+s/x+3l+eG2M6V8A2m+Q2r+6l0d5C2v8e1V3L9o8v+P8v4e3Q8X8V6K8b6K1C6
M6H8y+P6b8U9Y7V0L3a7r3O7P1X4a6P9q5e1X5n8B6m0Z+K8R+c8d+C6s+C3r8M+
W4O2v2e+V4M9K5B1z5e+K9S+Z+A+J8m8Z+C1n4o+R7c8W4X9D9y9M6O4+Y9V6X8Q
5c8C+N4G8K8H7E6P5V6B+Q==
-----END CERTIFICATE-----`
	os.WriteFile(validCAFile, []byte(dummyPEM), 0644)
	os.WriteFile(validCredsFile, []byte("some-creds"), 0644)

	invalidCAFile := filepath.Join(tmpDir, "invalid-ca.pem")
	os.WriteFile(invalidCAFile, []byte("-----BEGIN CERTIFICATE-----\nNOT A REAL CERT\n-----END CERTIFICATE-----"), 0644)

	tests := []struct {
		name    string
		cfg     config.NATSConfig
		wantErr bool
	}{
		{
			name: "invalid ca pem",
			cfg: config.NATSConfig{
				Target:          "router-a",
				Servers:         []string{"tls://127.0.0.1:4222"},
				CredentialsFile: validCredsFile,
				CAFile:          invalidCAFile,
			},
			wantErr: true,
		},
		{
			name: "missing ca file",
			cfg: config.NATSConfig{
				Target:          "router-a",
				Servers:         []string{"tls://127.0.0.1:4222"},
				CredentialsFile: validCredsFile,
				CAFile:          "",
			},
			wantErr: true,
		},
		{
			name: "missing creds file",
			cfg: config.NATSConfig{
				Target:          "router-a",
				Servers:         []string{"tls://127.0.0.1:4222"},
				CredentialsFile: "",
				CAFile:          validCAFile,
			},
			wantErr: true,
		},
		{
			name: "missing servers",
			cfg: config.NATSConfig{
				Target:          "router-a",
				Servers:         []string{},
				CredentialsFile: validCredsFile,
				CAFile:          validCAFile,
			},
			wantErr: true,
		},
		{
			name: "insecure server url",
			cfg: config.NATSConfig{
				Target:          "router-a",
				Servers:         []string{"nats://127.0.0.1:4222"},
				CredentialsFile: validCredsFile,
				CAFile:          validCAFile,
			},
			wantErr: true,
		},
		{
			name: "missing target",
			cfg: config.NATSConfig{
				Target:          "",
				Servers:         []string{"tls://127.0.0.1:4222"},
				CredentialsFile: validCredsFile,
				CAFile:          validCAFile,
			},
			wantErr: true,
		},
		{
			name: "malformed target",
			cfg: config.NATSConfig{
				Target:          "router.*.a",
				Servers:         []string{"tls://127.0.0.1:4222"},
				CredentialsFile: validCredsFile,
				CAFile:          validCAFile,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewNATSClient(tt.cfg, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewNATSClient() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExecuteAction_FailsFastWhenDisconnected(t *testing.T) {
	// 1. Start a basic in-memory NATS server on a random port
	opts := &server.Options{
		Host: "127.0.0.1",
		Port: -1,
	}
	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("Failed to create NATS server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatalf("NATS server failed to start")
	}
	defer s.Shutdown()

	// 2. Connect a client manually (bypassing the strict TLS NewNATSClient for testing)
	clientOpts := []nats.Option{
		nats.MaxReconnects(-1),
		nats.ReconnectBufSize(0), // The critical setting for REQ-012
	}
	conn, err := nats.Connect(s.ClientURL(), clientOpts...)
	if err != nil {
		t.Fatalf("Failed to connect to test server: %v", err)
	}
	defer conn.Close()

	client := &NATSClient{
		target: "vyos",
		conn:   conn,
	}

	// 3. Brutally shutdown the server to force the connection to drop
	s.Shutdown()

	// Wait for the client to detect the disconnect and go into RECONNECTING state
	timeout := time.After(2 * time.Second)
	disconnected := false
	for {
		select {
		case <-timeout:
			t.Fatalf("Client did not enter reconnecting state in time")
		default:
			if conn.Status() == nats.RECONNECTING || conn.Status() == nats.DISCONNECTED {
				disconnected = true
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if disconnected {
			break
		}
	}

	// 4. Attempt to publish. Because ReconnectBufSize is 0, this MUST instantly fail-fast.
	cmd := &agentcore.ActionCommand{
		Version:     "1.0",
		CommandType: "reboot",
		RPCID:       "test-123",
		Target:      "vyos",
		Action:      "reboot",
		Timestamp:   time.Now().UTC(),
		Payload:     []byte("{\"serial\":\"vyos\"}"),
	}

	err = client.ExecuteAction(context.Background(), cmd)
	if err == nil {
		t.Fatalf("Expected ExecuteAction to instantly fail-fast while disconnected, but it succeeded (secretly buffered)!")
	}
}

func TestNATSClient_ConcurrentKVInit(t *testing.T) {
	// 1. Start a basic in-memory NATS server with JetStream enabled
	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("Failed to create NATS server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatalf("NATS server failed to start")
	}
	defer s.Shutdown()

	// 2. Connect a client manually
	conn, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect to test server: %v", err)
	}
	defer conn.Close()

	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatalf("Failed to initialize JetStream: %v", err)
	}

	client := &NATSClient{
		target: "vyos",
		conn:   conn,
		js:     js,
	}

	// 3. Concurrently call KV methods to trigger race condition on initialization
	var wg sync.WaitGroup
	errCh := make(chan error, 100)

	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func(idx int) {
			defer wg.Done()
			_, err := client.WriteDesiredConfig(context.Background(), agentcore.DesiredConfigRecord{Target: "vyos"})
			if err != nil {
				errCh <- err
			}
		}(i)

		go func(idx int) {
			defer wg.Done()
			_, _, err := client.GetDesiredConfigMetadata(context.Background())
			if err != nil {
				errCh <- err
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("Concurrent KV operation failed: %v", err)
	}
}

func TestNATSClient_TargetIsolation_TC_SEC_001(t *testing.T) {
	// A live connection isn't strictly necessary since validation happens before publish,
	// but we construct a dummy client here.
	client := &NATSClient{
		target: "router-a",
	}

	// 1. Test ExecuteAction mismatch
	cmdAction := &agentcore.ActionCommand{
		Version:     "1.0",
		RPCID:       "test-rpc-12345",
		Target:      "router-b",
		CommandType: "action",
		Action:      "reboot",
		Timestamp:   time.Now(),
		Payload:     []byte(`{"serial": "router-b"}`),
	}
	err := client.ExecuteAction(context.Background(), cmdAction)
	if err == nil {
		t.Errorf("Expected ExecuteAction to fail with target mismatch, got nil")
	} else if !strings.Contains(err.Error(), "target mismatch") {
		t.Errorf("Expected target mismatch error, got: %v", err)
	}

	// 2. Test PublishConfigTrigger mismatch
	cmdConfig := &agentcore.ConfigureNotification{
		Version:     "1.0",
		RPCID:       "test-rpc-12345",
		Target:      "router-b",
		CommandType: "configure",
		KVBucket:    "cfg_desired",
		KVKey:       "desired.router-b",
		UUID:        "123456789",
		Timestamp:   time.Now(),
	}
	err = client.PublishConfigTrigger(context.Background(), cmdConfig)
	if err == nil {
		t.Errorf("Expected PublishConfigTrigger to fail with target mismatch, got nil")
	} else if !strings.Contains(err.Error(), "target mismatch") {
		t.Errorf("Expected target mismatch error, got: %v", err)
	}

	// 3. Test WriteDesiredConfig mismatch
	record := agentcore.DesiredConfigRecord{
		Version:   "1.0",
		RPCID:     "test-rpc-12345",
		Target:    "router-b",
		UUID:      "123456789",
		Timestamp: time.Now(),
		Payload:   []byte(`{"some":"config"}`),
	}
	_, err = client.WriteDesiredConfig(context.Background(), record)
	if err == nil {
		t.Errorf("Expected WriteDesiredConfig to fail with target mismatch, got nil")
	} else if !strings.Contains(err.Error(), "target mismatch") {
		t.Errorf("Expected target mismatch error, got: %v", err)
	}
}

func TestNewNATSClient_SecurityWiring_Success(t *testing.T) {
	tmpDir := t.TempDir()

	// 1. Generate TLS Certificates
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("Failed to generate private key: %v", err)
	}

	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{Organization: []string{"Test"}},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("Failed to create certificate: %v", err)
	}

	caFile := filepath.Join(tmpDir, "ca.pem")
	certOut, _ := os.Create(caFile)
	pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	certOut.Close()

	keyFile := filepath.Join(tmpDir, "key.pem")
	keyOut, _ := os.Create(keyFile)
	privBytes, _ := x509.MarshalECPrivateKey(priv)
	pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: privBytes})
	keyOut.Close()

	// 2. Start NATS Server with TLS enforced
	cert, err := tls.LoadX509KeyPair(caFile, keyFile)
	if err != nil {
		t.Fatalf("Failed to load key pair: %v", err)
	}
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
	}

	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		TLSConfig: tlsConfig,
		JetStream: true,
		StoreDir:  t.TempDir(),
	}

	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("Failed to start NATS server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatalf("NATS server failed to start")
	}
	defer s.Shutdown()

	// 3. Generate Valid NKey Credentials File
	kp, _ := nkeys.CreateUser()
	seed, _ := kp.Seed()
	credsFile := filepath.Join(tmpDir, "user.creds")
	credsContent := fmt.Sprintf("-----BEGIN USER NKEY SEED-----\n%s\n------END USER NKEY SEED------", string(seed))
	os.WriteFile(credsFile, []byte(credsContent), 0644)

	// 4. Construct Client config
	cfg := config.NATSConfig{
		Target:          "vyos",
		Servers:         []string{fmt.Sprintf("tls://%s", s.Addr().String())},
		CredentialsFile: credsFile,
		CAFile:          caFile,
	}

	// 5. Connect and Validate
	client, err := NewNATSClient(cfg, nil)
	if err != nil {
		t.Fatalf("Expected successful secure connection, got error: %v", err)
	}
	if client == nil {
		t.Fatalf("Expected valid client")
	}
}

func TestNATSClient_ConfigureE2E_TC_INT_001(t *testing.T) {
	// 1. Setup embedded server
	s, err := server.NewServer(&server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	})
	if err != nil {
		t.Fatalf("Failed to create NATS server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatalf("NATS server failed to start")
	}
	defer s.Shutdown()

	// 2. Setup Client
	conn, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect to test server: %v", err)
	}
	defer conn.Close()

	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatalf("Failed to initialize JetStream: %v", err)
	}

	client := &NATSClient{
		target: "vyos",
		conn:   conn,
		js:     js,
	}

	// 3. Define desired configuration record
	configUUID := "123456789"
	record := agentcore.DesiredConfigRecord{
		Version:   "1.0",
		RPCID:     "rpc-123",
		Target:    "vyos",
		UUID:      configUUID,
		Timestamp: time.Now(),
		Payload:   []byte(`{"some":"config"}`),
	}

	// 4. Write Desired Config to KV
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.WriteDesiredConfig(ctx, record)
	if err != nil {
		t.Fatalf("WriteDesiredConfig failed: %v", err)
	}

	// 5. Mock a downstream subscriber (the VyOS agent)
	subTriggered := make(chan struct{})
	sub, err := conn.Subscribe("cmd.configure.vyos", func(msg *nats.Msg) {
		defer close(subTriggered)
		// VyOS received the trigger! Let's parse it.
		var notif agentcore.ConfigureNotification
		if err := json.Unmarshal(msg.Data, &notif); err != nil {
			t.Errorf("Downstream failed to parse ConfigureNotification: %v", err)
			return
		}

		if notif.UUID != configUUID {
			t.Errorf("Downstream received UUID %s, expected %s", notif.UUID, configUUID)
		}
		if notif.KVBucket != "cfg_desired" {
			t.Errorf("Expected bucket cfg_desired, got %s", notif.KVBucket)
		}

		// VyOS reads from KV using the bucket/key from the notification
		kv, err := js.KeyValue(ctx, notif.KVBucket)
		if err != nil {
			t.Errorf("Downstream failed to bind KV: %v", err)
			return
		}

		entry, err := kv.Get(ctx, notif.KVKey)
		if err != nil {
			t.Errorf("Downstream failed to get KV entry %s: %v", notif.KVKey, err)
			return
		}

		var downstreamRecord agentcore.DesiredConfigRecord
		if err := json.Unmarshal(entry.Value(), &downstreamRecord); err != nil {
			t.Errorf("Downstream failed to parse DesiredConfigRecord from KV: %v", err)
			return
		}

		if downstreamRecord.UUID != configUUID {
			t.Errorf("Downstream KV record UUID %s != expected %s", downstreamRecord.UUID, configUUID)
		}
	})
	if err != nil {
		t.Fatalf("Failed to subscribe downstream: %v", err)
	}
	defer sub.Unsubscribe()
	conn.Flush()

	// 6. Publish the Config Trigger
	cmd := &agentcore.ConfigureNotification{
		Version:     "1.0",
		RPCID:       "rpc-123",
		Target:      "vyos",
		CommandType: "configure",
		KVBucket:    "cfg_desired",
		KVKey:       "desired.vyos",
		UUID:        configUUID,
		Timestamp:   time.Now(),
	}
	if err := client.PublishConfigTrigger(ctx, cmd); err != nil {
		t.Fatalf("PublishConfigTrigger failed: %v", err)
	}

	// 7. Wait for downstream verification
	select {
	case <-subTriggered:
		// Success!
	case <-time.After(5 * time.Second):
		t.Fatalf("Timed out waiting for downstream to receive configure trigger")
	}
}
