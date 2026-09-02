package tests

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

var (
	clientBinPath string
)

func TestMain(m *testing.M) {
	buildCmd := exec.Command("go", "build", "-o", "ucentral-client.testbin", "../cmd/ucentral-client")
	if err := buildCmd.Run(); err != nil {

		os.Exit(1)
	}
	clientBinPath, _ = filepath.Abs("./ucentral-client.testbin")
	code := m.Run()
	os.Remove(clientBinPath)
	os.Exit(code)
}

// ---------------------------------------------------------------------------
// Infrastructure helpers
// ---------------------------------------------------------------------------

func startEmbeddedNATS(t *testing.T) *server.Server {
	t.Helper()
	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      server.RANDOM_PORT,
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	ns, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("Failed to create NATS server: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server failed to start")
	}
	t.Cleanup(func() {
		ns.Shutdown()
	})
	return ns
}

type MockCloud struct {
	srv      *httptest.Server
	URL      string
	conn     *websocket.Conn
	mu       sync.Mutex
	messages [][]byte
}

func startMockCloud(t *testing.T) *MockCloud {
	t.Helper()
	upgrader := websocket.Upgrader{}
	mc := &MockCloud{}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		mc.mu.Lock()
		mc.conn = conn
		mc.mu.Unlock()

		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				break
			}

			var rpcReq map[string]interface{}
			if err := json.Unmarshal(msg, &rpcReq); err == nil {
				if method, ok := rpcReq["method"].(string); ok && method == "connect" {
					// The real uCentral Gateway does NOT send a JSON-RPC response
					// to the connect handshake (it's a Notification).
					continue
				}
			}

			mc.mu.Lock()
			mc.messages = append(mc.messages, msg)
			mc.mu.Unlock()
		}
	})

	mc.srv = httptest.NewTLSServer(handler)
	mc.URL = "wss" + mc.srv.URL[5:]
	t.Cleanup(func() {
		mc.srv.Close()
	})
	return mc
}

func (mc *MockCloud) WaitForConnection(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		mc.mu.Lock()
		conn := mc.conn
		mc.mu.Unlock()
		if conn != nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Timeout waiting for WebSocket connection from client")
}

func (mc *MockCloud) SendMessage(t *testing.T, msg string) {
	t.Helper()
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if mc.conn == nil {
		t.Fatalf("No active WebSocket connection to send message")
	}
	if err := mc.conn.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
		t.Fatalf("Failed to write to mock cloud: %v", err)
	}
}

func (mc *MockCloud) WaitForResponse(t *testing.T, timeout time.Duration) []byte {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		mc.mu.Lock()
		if len(mc.messages) > 0 {
			msg := mc.messages[0]
			mc.messages = mc.messages[1:]
			mc.mu.Unlock()
			return msg
		}
		mc.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("Timeout waiting for response from client")
	return nil
}

// WaitForResponseWithID waits for a JSON-RPC response matching the given id.
// Since the client now correctly consumes the connect handshake, we should not
// see unexpected Invalid Request errors.
func (mc *MockCloud) WaitForResponseWithID(t *testing.T, expectedID float64, timeout time.Duration) ([]byte, map[string]interface{}) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		respBytes := mc.WaitForResponse(t, remaining)
		var resp map[string]interface{}
		json.Unmarshal(respBytes, &resp)

		// If it's a request from the client (e.g. telemetry, ping), ignore it
		if _, hasMethod := resp["method"]; hasMethod {
			continue
		}

		if idVal, ok := resp["id"].(float64); ok && idVal == expectedID {
			return respBytes, resp
		}

		t.Fatalf("Received unexpected JSON-RPC response while waiting for id=%v: %s", expectedID, string(respBytes))
	}
	t.Fatalf("Timeout waiting for response with id=%v", expectedID)
	return nil, nil
}

func writeTempConfig(t *testing.T, cfg map[string]interface{}) string {
	t.Helper()
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("Failed to marshal config: %v", err)
	}
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}
	return path
}

func getTestConfig(t *testing.T, mc *MockCloud, ns *server.Server) map[string]interface{} {
	t.Helper()

	certFile := filepath.Join(t.TempDir(), "ca.pem")
	certBytes := mc.srv.TLS.Certificates[0].Certificate[0]
	pemBlock := &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certBytes,
	}
	pemData := pem.EncodeToMemory(pemBlock)
	if err := os.WriteFile(certFile, pemData, 0644); err != nil {
		t.Fatalf("Failed to write CA cert: %v", err)
	}

	dummyClientCert := filepath.Join(t.TempDir(), "client.crt")
	dummyClientKey := filepath.Join(t.TempDir(), "client.key")

	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("Failed to generate RSA key: %v", err)
	}

	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("Failed to create certificate: %v", err)
	}

	certOut := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	if err := os.WriteFile(dummyClientCert, certOut, 0644); err != nil {
		t.Fatalf("Failed to write dummy client cert: %v", err)
	}

	keyOut := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})
	if err := os.WriteFile(dummyClientKey, keyOut, 0600); err != nil {
		t.Fatalf("Failed to write dummy client key: %v", err)
	}

	capFile := filepath.Join(t.TempDir(), "capabilities.json")
	if err := os.WriteFile(capFile, []byte(`{"compatible":"vyos", "version": {"olg": {"major": 1, "minor": 0, "patch": 0}}}`), 0644); err != nil {
		t.Fatalf("Failed to write capabilities.json: %v", err)
	}

	return map[string]interface{}{
		"serial":            "001122334455",
		"capabilities_file": capFile,
		"cloud": map[string]interface{}{
			"url":                              mc.URL,
			"connect_timeout_seconds":          30,
			"write_timeout_seconds":            30,
			"ping_interval_seconds":            30,
			"pong_timeout_seconds":             30,
			"stable_session_threshold_seconds": 30,
			"tls": map[string]interface{}{
				"ca_file":          certFile,
				"client_cert_file": dummyClientCert,
				"client_key_file":  dummyClientKey,
			},
		},
		"nats": map[string]interface{}{
			"servers":                  []string{ns.ClientURL()},
			"target":                   "vyos",
			"allow_insecure_local_dev": true,
		},
		"queues": map[string]interface{}{
			"ws_writer_capacity":      100,
			"emergency_capacity":      100,
			"nats_publish_capacity":   100,
			"command_result_capacity": 100,
			"telemetry_capacity":      100,
			"max_concurrent_requests": 100,
		},
	}
}

// mockNATSResult is a helper that subscribes to a NATS subject and publishes
// a result envelope back on result.vyos when a command arrives.
func mockNATSResult(t *testing.T, nc *nats.Conn, subject string, commandType string, result string, message string, errorCode string) {
	t.Helper()
	_, err := nc.Subscribe(subject, func(m *nats.Msg) {
		var req map[string]interface{}
		if err := json.Unmarshal(m.Data, &req); err != nil {
			t.Errorf("Failed to unmarshal NATS message: %v", err)
			return
		}
		rpcID := ""
		if id, ok := req["rpc_id"].(string); ok {
			rpcID = id
		}
		uuid := ""
		if u, ok := req["uuid"].(string); ok {
			uuid = u
		}
		action := ""
		if a, ok := req["action"].(string); ok {
			action = a
		}

		res := map[string]interface{}{
			"version":      "1.0",
			"rpc_id":       rpcID,
			"target":       "vyos",
			"command_type": commandType,
			"uuid":         uuid,
			"action":       action,
			"result":       result,
			"message":      message,
			"timestamp":    "2026-08-31T18:00:00Z",
		}
		if errorCode != "" {
			res["error_code"] = errorCode
		}
		b, err := json.Marshal(res)
		if err != nil {
			t.Errorf("Failed to marshal NATS response: %v", err)
			return
		}
		if err := nc.Publish("result.vyos", b); err != nil {
			t.Errorf("Failed to publish NATS response: %v", err)
		}
	})
	if err != nil {
		t.Fatalf("Failed to subscribe to %s: %v", subject, err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("Failed to flush NATS connection: %v", err)
	}
	if err := nc.LastError(); err != nil {
		t.Fatalf("NATS connection error after flush: %v", err)
	}
}

// startClientProcess builds the config, sets env, and starts the client binary.
// Returns a cancel func that stops the process.
func startClientProcess(t *testing.T, mc *MockCloud, cfg map[string]interface{}) context.CancelFunc {
	t.Helper()
	os.Setenv("OLG_INSECURE_SKIP_VERIFY", "true")
	t.Cleanup(func() { os.Unsetenv("OLG_INSECURE_SKIP_VERIFY") })

	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, clientBinPath, "-config", writeTempConfig(t, cfg))
	if capFile, ok := cfg["capabilities_file"].(string); ok {
		cmd.Dir = filepath.Dir(capFile)
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start client process: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		cmd.Wait()
	})

	// Wait for the client to connect to NATS + WebSocket
	mc.WaitForConnection(t, 5*time.Second)
	return cancel
}

// ---------------------------------------------------------------------------
// Config validation
// ---------------------------------------------------------------------------

func TestConfigValidation_InvalidConfig(t *testing.T) {
	cfg := getTestConfig(t, startMockCloud(t), startEmbeddedNATS(t))
	cfg["serial"] = ""
	configPath := writeTempConfig(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, clientBinPath, "-config", configPath)
	err := cmd.Run()
	if err == nil {
		t.Fatalf("Expected client to fail on invalid config, but it succeeded")
	}
}

// ---------------------------------------------------------------------------
// Configure: positive & negative
// ---------------------------------------------------------------------------

func TestComponent_ConfigurePositive(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()
	mc := startMockCloud(t)

	mockNATSResult(t, nc, "cmd.configure.vyos", "configure", "success", "applied successfully", "")

	cfg := getTestConfig(t, mc, ns)
	startClientProcess(t, mc, cfg)

	req := `{"jsonrpc":"2.0","method":"configure","id":10,"params":{"serial":"001122334455","uuid":123,"config":{"interfaces":[]}}}`
	mc.SendMessage(t, req)

	_, resp := mc.WaitForResponseWithID(t, 10, 10*time.Second)

	if resp["error"] != nil {
		t.Fatalf("Expected no error, got %v", resp["error"])
	}
	resultObj, ok := resp["result"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected result object, got %v", resp)
	}
	statusObj := resultObj["status"].(map[string]interface{})
	if int(statusObj["error"].(float64)) != 0 {
		t.Errorf("Expected status.error=0 for success, got %v", statusObj["error"])
	}
}

func TestComponent_ConfigureNegative_Failed(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()
	mc := startMockCloud(t)

	mockNATSResult(t, nc, "cmd.configure.vyos", "configure", "failure", "commit failed", "-32603")

	cfg := getTestConfig(t, mc, ns)
	startClientProcess(t, mc, cfg)

	req := `{"jsonrpc":"2.0","method":"configure","id":20,"params":{"serial":"001122334455","uuid":456,"config":{"interfaces":[]}}}`
	mc.SendMessage(t, req)

	_, resp := mc.WaitForResponseWithID(t, 20, 10*time.Second)

	resultObj, ok := resp["result"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected result object, got %v", resp)
	}
	statusObj := resultObj["status"].(map[string]interface{})
	if int(statusObj["error"].(float64)) != -32603 {
		t.Errorf("Expected status.error=-32603, got %v", statusObj["error"])
	}
	if statusObj["text"].(string) != "commit failed" {
		t.Errorf("Expected status.text='commit failed', got %v", statusObj["text"])
	}
}

func TestComponent_ConfigureNegative_Rejected(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()
	mc := startMockCloud(t)

	mockNATSResult(t, nc, "cmd.configure.vyos", "configure", "rejected", "schema validation error", "-32602")

	cfg := getTestConfig(t, mc, ns)
	startClientProcess(t, mc, cfg)

	req := `{"jsonrpc":"2.0","method":"configure","id":21,"params":{"serial":"001122334455","uuid":789,"config":{"interfaces":[]}}}`
	mc.SendMessage(t, req)

	_, resp := mc.WaitForResponseWithID(t, 21, 10*time.Second)

	resultObj := resp["result"].(map[string]interface{})
	statusObj := resultObj["status"].(map[string]interface{})
	if int(statusObj["error"].(float64)) != -32602 {
		t.Errorf("Expected status.error=-32602, got %v", statusObj["error"])
	}
	if statusObj["text"].(string) != "schema validation error" {
		t.Errorf("Expected status.text='schema validation error', got %v", statusObj["text"])
	}
}

// ---------------------------------------------------------------------------
// Trace: positive & negative
// ---------------------------------------------------------------------------

func TestComponent_TracePositive(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()
	mc := startMockCloud(t)

	// Trace is an action → subject is cmd.action.vyos.trace
	mockNATSResult(t, nc, "cmd.action.vyos.trace", "action", "success", "trace completed", "")

	cfg := getTestConfig(t, mc, ns)
	startClientProcess(t, mc, cfg)

	req := `{"jsonrpc":"2.0","method":"trace","id":30,"params":{"serial":"001122334455","duration":5,"network":"lan","interface":"eth0","packets":100}}`
	mc.SendMessage(t, req)

	_, resp := mc.WaitForResponseWithID(t, 30, 10*time.Second)

	if resp["error"] != nil {
		t.Fatalf("Expected no error for trace, got %v", resp["error"])
	}
	resultObj, ok := resp["result"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected result object, got %v", resp)
	}
	statusObj := resultObj["status"].(map[string]interface{})
	if int(statusObj["error"].(float64)) != 0 {
		t.Errorf("Expected status.error=0 for trace success, got %v", statusObj["error"])
	}
}

func TestComponent_TraceNegative_Failed(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()
	mc := startMockCloud(t)

	mockNATSResult(t, nc, "cmd.action.vyos.trace", "action", "failure", "traceroute command not found", "-32603")

	cfg := getTestConfig(t, mc, ns)
	startClientProcess(t, mc, cfg)

	req := `{"jsonrpc":"2.0","method":"trace","id":31,"params":{"serial":"001122334455","duration":5,"network":"lan","interface":"eth0","packets":100}}`
	mc.SendMessage(t, req)

	_, resp := mc.WaitForResponseWithID(t, 31, 10*time.Second)

	resultObj := resp["result"].(map[string]interface{})
	statusObj := resultObj["status"].(map[string]interface{})
	if int(statusObj["error"].(float64)) != -32603 {
		t.Errorf("Expected status.error=-32603, got %v", statusObj["error"])
	}
	if statusObj["text"].(string) != "traceroute command not found" {
		t.Errorf("Expected status.text='traceroute command not found', got %v", statusObj["text"])
	}
}

// ---------------------------------------------------------------------------
// Negative: unknown method
// ---------------------------------------------------------------------------

func TestComponent_UnknownMethod(t *testing.T) {
	ns := startEmbeddedNATS(t)
	mc := startMockCloud(t)

	cfg := getTestConfig(t, mc, ns)
	startClientProcess(t, mc, cfg)

	// Send a method the client doesn't recognize
	req := `{"jsonrpc":"2.0","method":"nonexistent_method","id":40,"params":{"serial":"001122334455"}}`
	mc.SendMessage(t, req)

	_, resp := mc.WaitForResponseWithID(t, 40, 10*time.Second)

	// Should get a JSON-RPC error response
	errObj, ok := resp["error"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected JSON-RPC error for unknown method, got: %v", resp)
	}
	code := int(errObj["code"].(float64))
	if code != -32601 {
		t.Errorf("Expected error code -32601 for unknown method, got %d", code)
	}
}

// ---------------------------------------------------------------------------
// Negative: malformed JSON-RPC (missing method)
// ---------------------------------------------------------------------------

func TestComponent_MalformedRequest_NoMethod(t *testing.T) {
	ns := startEmbeddedNATS(t)
	mc := startMockCloud(t)

	cfg := getTestConfig(t, mc, ns)
	startClientProcess(t, mc, cfg)

	// JSON-RPC request with no method field
	req := `{"jsonrpc":"2.0","id":50,"params":{"serial":"001122334455"}}`
	mc.SendMessage(t, req)

	_, resp := mc.WaitForResponseWithID(t, 50, 10*time.Second)

	errObj, ok := resp["error"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected JSON-RPC error for missing method, got: %v", resp)
	}
	code := int(errObj["code"].(float64))
	// JSON-RPC spec: -32600 = Invalid Request
	if code != -32600 {
		t.Errorf("Expected error code -32600, got %d", code)
	}
}

// ---------------------------------------------------------------------------
// Positive: multiple sequential commands on one session
// ---------------------------------------------------------------------------

func TestComponent_MultipleSequentialCommands(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()
	mc := startMockCloud(t)

	// Mock both configure and trace
	mockNATSResult(t, nc, "cmd.configure.vyos", "configure", "success", "config applied", "")
	mockNATSResult(t, nc, "cmd.action.vyos.trace", "action", "success", "trace done", "")

	cfg := getTestConfig(t, mc, ns)
	startClientProcess(t, mc, cfg)

	// 1. Send configure
	mc.SendMessage(t, `{"jsonrpc":"2.0","method":"configure","id":60,"params":{"serial":"001122334455","uuid":100,"config":{"interfaces":[]}}}`)
	_, resp1 := mc.WaitForResponseWithID(t, 60, 10*time.Second)
	if resp1["error"] != nil {
		t.Fatalf("Configure should succeed, got error: %v", resp1["error"])
	}

	// 2. Send trace on same session
	mc.SendMessage(t, `{"jsonrpc":"2.0","method":"trace","id":61,"params":{"serial":"001122334455","duration":5,"network":"lan","interface":"eth0","packets":50}}`)
	_, resp2 := mc.WaitForResponseWithID(t, 61, 10*time.Second)
	if resp2["error"] != nil {
		t.Fatalf("Trace should succeed, got error: %v", resp2["error"])
	}

	// 3. Send another configure
	mc.SendMessage(t, `{"jsonrpc":"2.0","method":"configure","id":62,"params":{"serial":"001122334455","uuid":200,"config":{"interfaces":[]}}}`)
	_, resp3 := mc.WaitForResponseWithID(t, 62, 10*time.Second)
	if resp3["error"] != nil {
		t.Fatalf("Second configure should succeed, got error: %v", resp3["error"])
	}
}

// ---------------------------------------------------------------------------
// Positive: concurrent commands with out-of-order responses
// ---------------------------------------------------------------------------

func TestComponent_ConcurrentOutOfOrder(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()
	mc := startMockCloud(t)

	reqCh := make(chan map[string]interface{}, 2)

	_, err = nc.Subscribe("cmd.action.vyos.trace", func(m *nats.Msg) {
		var req map[string]interface{}
		if err := json.Unmarshal(m.Data, &req); err != nil {
			t.Errorf("Failed to unmarshal NATS message: %v", err)
			return
		}
		reqCh <- req
	})
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	cfg := getTestConfig(t, mc, ns)
	startClientProcess(t, mc, cfg)

	mc.SendMessage(t, `{"jsonrpc":"2.0","method":"trace","id":101,"params":{"serial":"001122334455","duration":5}}`)
	mc.SendMessage(t, `{"jsonrpc":"2.0","method":"trace","id":102,"params":{"serial":"001122334455","duration":10}}`)

	var req1, req2 map[string]interface{}
	select {
	case req1 = <-reqCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for first NATS request")
	}
	select {
	case req2 = <-reqCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for second NATS request")
	}

	replyToNATS := func(req map[string]interface{}, status, msg string) {
		rpcID, _ := req["rpc_id"].(string)
		action, _ := req["action"].(string)
		uuid, _ := req["uuid"].(string)
		res := map[string]interface{}{
			"version":      "1.0",
			"rpc_id":       rpcID,
			"target":       "vyos",
			"command_type": "action",
			"uuid":         uuid,
			"action":       action,
			"result":       status,
			"message":      msg,
			"timestamp":    "2026-08-31T18:00:00Z",
		}
		b, _ := json.Marshal(res)
		nc.Publish("result.vyos", b)
	}

	// Determine which request is which based on duration
	var req101, req102 map[string]interface{}

	getDuration := func(req map[string]interface{}) float64 {
		payload, _ := req["payload"].(map[string]interface{})
		dur, _ := payload["duration"].(float64)
		return dur
	}

	if getDuration(req1) == 5 {
		req101 = req1
		req102 = req2
	} else {
		req101 = req2
		req102 = req1
	}

	// Reply to 102 first, then 101
	replyToNATS(req102, "success", "result-for-102")
	replyToNATS(req101, "success", "result-for-101")

	respBytes1 := mc.WaitForResponse(t, 10*time.Second)
	respBytes2 := mc.WaitForResponse(t, 10*time.Second)

	var resp1, resp2 map[string]interface{}
	json.Unmarshal(respBytes1, &resp1)
	json.Unmarshal(respBytes2, &resp2)

	id1 := int(resp1["id"].(float64))
	id2 := int(resp2["id"].(float64))

	validateResponse := func(id int, resp map[string]interface{}) {
		resultObj := resp["result"].(map[string]interface{})
		statusObj := resultObj["status"].(map[string]interface{})
		text := statusObj["text"].(string)
		if id == 101 && text != "result-for-101" {
			t.Fatalf("Expected result-for-101 for id 101, got %v", text)
		}
		if id == 102 && text != "result-for-102" {
			t.Fatalf("Expected result-for-102 for id 102, got %v", text)
		}
	}

	if id1 == 101 && id2 == 102 {
		validateResponse(101, resp1)
		validateResponse(102, resp2)
	} else if id1 == 102 && id2 == 101 {
		validateResponse(102, resp1)
		validateResponse(101, resp2)
	} else {
		t.Fatalf("Expected IDs 101 and 102, got %v and %v", id1, id2)
	}
}

// ---------------------------------------------------------------------------
// Positive: concurrent commands with immediate results
// ---------------------------------------------------------------------------

func TestComponent_ConcurrentImmediateResult(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()
	mc := startMockCloud(t)

	_, err = nc.Subscribe("cmd.action.vyos.trace", func(m *nats.Msg) {
		var req map[string]interface{}
		json.Unmarshal(m.Data, &req)
		rpcID, _ := req["rpc_id"].(string)
		action, _ := req["action"].(string)
		uuid, _ := req["uuid"].(string)

		res := map[string]interface{}{
			"version":      "1.0",
			"rpc_id":       rpcID,
			"target":       "vyos",
			"command_type": "action",
			"uuid":         uuid,
			"action":       action,
			"result":       "success",
			"message":      "immediate-result",
			"timestamp":    "2026-08-31T18:00:00Z",
		}
		b, _ := json.Marshal(res)
		nc.Publish("result.vyos", b)
	})
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}
	nc.Flush()

	cfg := getTestConfig(t, mc, ns)
	startClientProcess(t, mc, cfg)

	mc.SendMessage(t, `{"jsonrpc":"2.0","method":"trace","id":201,"params":{"serial":"001122334455","duration":5}}`)
	mc.SendMessage(t, `{"jsonrpc":"2.0","method":"trace","id":202,"params":{"serial":"001122334455","duration":10}}`)

	respBytes1 := mc.WaitForResponse(t, 10*time.Second)
	respBytes2 := mc.WaitForResponse(t, 10*time.Second)

	var resp1, resp2 map[string]interface{}
	json.Unmarshal(respBytes1, &resp1)
	json.Unmarshal(respBytes2, &resp2)

	id1 := int(resp1["id"].(float64))
	id2 := int(resp2["id"].(float64))

	if (id1 == 201 && id2 == 202) || (id1 == 202 && id2 == 201) {
		// success
	} else {
		t.Fatalf("Expected IDs 201 and 202, got %v and %v", id1, id2)
	}
}

func TestComponent_ConfigureNegative_EmptyErrorCode(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()
	mc := startMockCloud(t)

	// Mock an empty error code
	mockNATSResult(t, nc, "cmd.configure.vyos", "configure", "failure", "image validation failed", "")

	cfg := getTestConfig(t, mc, ns)
	startClientProcess(t, mc, cfg)

	req := `{"jsonrpc":"2.0","method":"configure","id":99,"params":{"serial":"001122334455","uuid":789,"config":{"interfaces":[]}}}`
	mc.SendMessage(t, req)

	_, resp := mc.WaitForResponseWithID(t, 99, 10*time.Second)

	resultObj, ok := resp["result"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected result object, got %v", resp)
	}
	statusObj := resultObj["status"].(map[string]interface{})
	if int(statusObj["error"].(float64)) != 1 {
		t.Errorf("Expected status.error=1 for empty NATS error_code, got %v", statusObj["error"])
	}
	if statusObj["text"].(string) != "image validation failed" {
		t.Errorf("Expected text 'image validation failed', got %v", statusObj["text"])
	}
}

func TestComponent_UpgradeLockRelease_OnEmptyErrorCode(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()
	mc := startMockCloud(t)

	// 1. Mock an empty error code for the upgrade action
	mockNATSResult(t, nc, "cmd.action.vyos.upgrade", "action", "failure", "image validation failed", "")

	// 2. Mock a normal successful configure command for later
	mockNATSResult(t, nc, "cmd.configure.vyos", "configure", "success", "commit successful", "")

	cfg := getTestConfig(t, mc, ns)
	startClientProcess(t, mc, cfg)

	// 3. Send the upgrade command.
	reqUpgrade := `{"jsonrpc":"2.0","method":"upgrade","id":100,"params":{"serial":"001122334455","action":"upgrade","uri":"https://example.com/fw.bin"}}`
	mc.SendMessage(t, reqUpgrade)

	// Gateway should immediately return a JSON-RPC response acknowledging the upgrade started
	// We just wait for ANY response for ID 100 to clear it out.
	mc.WaitForResponseWithID(t, 100, 5*time.Second)

	// The mock NATS agent has also concurrently published the `failure` result over NATS.
	// Give the ucentral-client a brief moment to process the NATS result and release the lock.
	time.Sleep(500 * time.Millisecond)

	// 4. Send a subsequent state-changing command (configure)
	reqConfigure := `{"jsonrpc":"2.0","method":"configure","id":101,"params":{"serial":"001122334455","uuid":789,"config":{"interfaces":[]}}}`
	mc.SendMessage(t, reqConfigure)

	// 5. Verify the configure command succeeds (meaning the lock was successfully released by the empty error code logic)
	_, respCfg := mc.WaitForResponseWithID(t, 101, 10*time.Second)
	
	// If the lock wasn't released, we would get an error object with "Device is busy".
	// Since we expect it to succeed, we check for a valid result.
	if respCfg["error"] != nil {
		errObj := respCfg["error"].(map[string]interface{})
		t.Fatalf("Configure failed, which means the upgrade lock was NOT released! Error: %v", errObj["message"])
	}

	resultObj, ok := respCfg["result"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected result object, got %v", respCfg)
	}
	statusObj := resultObj["status"].(map[string]interface{})
	if int(statusObj["error"].(float64)) != 0 {
		t.Errorf("Expected configure success (error=0), got %v", statusObj["error"])
	}
}
