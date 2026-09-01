package tests

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
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
		fmt.Printf("Failed to build client binary for tests: %v\n", err)
		os.Exit(1)
	}
	clientBinPath = "./ucentral-client.testbin"
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
					resp := map[string]interface{}{
						"jsonrpc": "2.0",
						"id":      rpcReq["id"],
						"result":  map[string]interface{}{"status": "connected"},
					}
					b, _ := json.Marshal(resp)
					conn.WriteMessage(websocket.TextMessage, b)
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

// WaitForResponseWithID waits for a JSON-RPC response matching the given id,
// skipping over any intermediate messages (e.g. the "Invalid request" error
// the client sends in response to the connect handshake).
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
		if idVal, ok := resp["id"].(float64); ok && idVal == expectedID {
			return respBytes, resp
		}
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
	os.WriteFile(certFile, pemData, 0644)

	dummyClientCert := filepath.Join(t.TempDir(), "client.crt")
	dummyClientKey := filepath.Join(t.TempDir(), "client.key")
	os.WriteFile(dummyClientCert, pemData, 0644)

	exec.Command("openssl", "req", "-x509", "-newkey", "rsa:2048", "-keyout", dummyClientKey, "-out", dummyClientCert, "-days", "365", "-nodes", "-subj", "/CN=localhost").Run()

	capFile := filepath.Join(t.TempDir(), "capabilities.json")
	os.WriteFile(capFile, []byte(`{"compatible":"vyos", "version": {"olg": {"major": 1, "minor": 0, "patch": 0}}}`), 0644)

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
func mockNATSResult(nc *nats.Conn, subject string, commandType string, result string, message string, errorCode string) {
	nc.Subscribe(subject, func(m *nats.Msg) {
		var req map[string]interface{}
		json.Unmarshal(m.Data, &req)
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
		b, _ := json.Marshal(res)
		nc.Publish("result.vyos", b)
	})
}

// startClientProcess builds the config, sets env, and starts the client binary.
// Returns a cancel func that stops the process.
func startClientProcess(t *testing.T, cfg map[string]interface{}) context.CancelFunc {
	t.Helper()
	os.Setenv("OLG_INSECURE_SKIP_VERIFY", "true")
	t.Cleanup(func() { os.Unsetenv("OLG_INSECURE_SKIP_VERIFY") })

	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, clientBinPath, "-config", writeTempConfig(t, cfg))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Start()
	t.Cleanup(func() {
		cancel()
		cmd.Wait()
	})

	// Wait for the client to connect to NATS + WebSocket
	time.Sleep(2 * time.Second)
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
	nc, _ := nats.Connect(ns.ClientURL())
	defer nc.Close()
	mc := startMockCloud(t)

	mockNATSResult(nc, "cmd.configure.vyos", "configure", "success", "applied successfully", "")

	cfg := getTestConfig(t, mc, ns)
	startClientProcess(t, cfg)

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
	nc, _ := nats.Connect(ns.ClientURL())
	defer nc.Close()
	mc := startMockCloud(t)

	mockNATSResult(nc, "cmd.configure.vyos", "configure", "failed", "commit failed", "-32603")

	cfg := getTestConfig(t, mc, ns)
	startClientProcess(t, cfg)

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
	nc, _ := nats.Connect(ns.ClientURL())
	defer nc.Close()
	mc := startMockCloud(t)

	mockNATSResult(nc, "cmd.configure.vyos", "configure", "rejected", "schema validation error", "-32602")

	cfg := getTestConfig(t, mc, ns)
	startClientProcess(t, cfg)

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
	nc, _ := nats.Connect(ns.ClientURL())
	defer nc.Close()
	mc := startMockCloud(t)

	// Trace is an action → subject is cmd.action.vyos.trace
	mockNATSResult(nc, "cmd.action.vyos.trace", "action", "success", "trace completed", "")

	cfg := getTestConfig(t, mc, ns)
	startClientProcess(t, cfg)

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
	nc, _ := nats.Connect(ns.ClientURL())
	defer nc.Close()
	mc := startMockCloud(t)

	mockNATSResult(nc, "cmd.action.vyos.trace", "action", "failed", "traceroute command not found", "-32603")

	cfg := getTestConfig(t, mc, ns)
	startClientProcess(t, cfg)

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
	startClientProcess(t, cfg)

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
	if code >= 0 {
		t.Errorf("Expected negative error code for unknown method, got %d", code)
	}
}

// ---------------------------------------------------------------------------
// Negative: malformed JSON-RPC (missing method)
// ---------------------------------------------------------------------------

func TestComponent_MalformedRequest_NoMethod(t *testing.T) {
	ns := startEmbeddedNATS(t)
	mc := startMockCloud(t)

	cfg := getTestConfig(t, mc, ns)
	startClientProcess(t, cfg)

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
	nc, _ := nats.Connect(ns.ClientURL())
	defer nc.Close()
	mc := startMockCloud(t)

	// Mock both configure and trace
	mockNATSResult(nc, "cmd.configure.vyos", "configure", "success", "config applied", "")
	mockNATSResult(nc, "cmd.action.vyos.trace", "action", "success", "trace done", "")

	cfg := getTestConfig(t, mc, ns)
	startClientProcess(t, cfg)

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
