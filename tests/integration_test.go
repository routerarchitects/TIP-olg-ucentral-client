package tests

import (
	"context"
	"fmt"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
	"sync"
	"encoding/pem"

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

func startEmbeddedNATS(t *testing.T) *server.Server {
	t.Helper()
	opts := &server.Options{
		Host: "127.0.0.1",
		Port: server.RANDOM_PORT,
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
		"serial": "001122334455",
		"capabilities_file": capFile,
		"cloud": map[string]interface{}{
			"url": mc.URL,
			"connect_timeout_seconds": 30,
			"write_timeout_seconds": 30,
			"ping_interval_seconds": 30,
			"pong_timeout_seconds": 30,
			"stable_session_threshold_seconds": 30,
			"tls": map[string]interface{}{
				"ca_file": certFile,
				"client_cert_file": dummyClientCert,
				"client_key_file": dummyClientKey,
			},
		},
		"nats": map[string]interface{}{
			"servers": []string{ns.ClientURL()},
			"target": "vyos",
			"allow_insecure_local_dev": true,
		},
		"queues": map[string]interface{}{
			"ws_writer_capacity": 100,
			"emergency_capacity": 100,
			"nats_publish_capacity": 100,
			"command_result_capacity": 100,
			"telemetry_capacity": 100,
			"max_concurrent_requests": 100,
		},
	}
}

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

func TestComponent_ConfigurePositive(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc, _ := nats.Connect(ns.ClientURL())
	defer nc.Close()
	mc := startMockCloud(t)

	nc.Subscribe("cmd.configure.vyos", func(m *nats.Msg) {
		fmt.Println("MOCK CALLED!"); var req map[string]interface{}
		json.Unmarshal(m.Data, &req)
		rpcID := ""
		if id, ok := req["rpc_id"].(string); ok { rpcID = id }
		uuid := ""
		if u, ok := req["uuid"].(string); ok { uuid = u }
		
		res := map[string]interface{}{
			"version": "1.0",
			"rpc_id": rpcID,
			"target": "vyos",
			"command_type": "configure",
			"uuid": uuid,
			"result": "success",
			"message": "applied successfully", "timestamp": "2026-08-31T18:00:00Z",
		}
		b, _ := json.Marshal(res)
		nc.Publish("result.vyos", b)
	})

	cfg := getTestConfig(t, mc, ns)
	os.Setenv("OLG_INSECURE_SKIP_VERIFY", "true")
	defer os.Unsetenv("OLG_INSECURE_SKIP_VERIFY")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := exec.CommandContext(ctx, clientBinPath, "-config", writeTempConfig(t, cfg))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Start()
	defer cmd.Wait()

	time.Sleep(2 * time.Second)

	req := `{"jsonrpc":"2.0","method":"configure","id":10,"params":{"serial":"001122334455","uuid":123,"config":{"interfaces":[]}}}`
	mc.SendMessage(t, req)

	respBytes := mc.WaitForResponse(t, 5*time.Second)
	var resp map[string]interface{}
	json.Unmarshal(respBytes, &resp)
	
	
		
	for {
		if idVal, ok := resp["id"].(float64); ok && idVal == 10 {
			break
		}
		respBytes = mc.WaitForResponse(t, 5*time.Second)
		resp = make(map[string]interface{})
		json.Unmarshal(respBytes, &resp)
	}
	if resp["error"] != nil {
		t.Fatalf("Expected no error, got %v", resp["error"])
	}
	
	cancel()
}

func TestComponent_ConfigureNegative_Rollback(t *testing.T) {
	ns := startEmbeddedNATS(t)
	nc, _ := nats.Connect(ns.ClientURL())
	defer nc.Close()
	mc := startMockCloud(t)

	nc.Subscribe("cmd.configure.vyos", func(m *nats.Msg) {
		fmt.Println("MOCK CALLED!"); var req map[string]interface{}
		json.Unmarshal(m.Data, &req)
		rpcID := ""
		if id, ok := req["rpc_id"].(string); ok { rpcID = id }
		uuid := ""
		if u, ok := req["uuid"].(string); ok { uuid = u }
		
		res := map[string]interface{}{
			"version": "1.0",
			"rpc_id": rpcID,
			"target": "vyos",
			"command_type": "configure",
			"uuid": uuid,
			"result": "failed",
			"message": "commit failed",
			"error_code": "-32603", "timestamp": "2026-08-31T18:00:00Z",
		}
		b, _ := json.Marshal(res)
		nc.Publish("result.vyos", b)
	})

	cfg := getTestConfig(t, mc, ns)
	os.Setenv("OLG_INSECURE_SKIP_VERIFY", "true")
	defer os.Unsetenv("OLG_INSECURE_SKIP_VERIFY")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := exec.CommandContext(ctx, clientBinPath, "-config", writeTempConfig(t, cfg))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Start()
	defer cmd.Wait()

	time.Sleep(2 * time.Second)

	req := `{"jsonrpc":"2.0","method":"configure","id":20,"params":{"serial":"001122334455","uuid":456,"config":{"interfaces":[]}}}`
	mc.SendMessage(t, req)

	respBytes := mc.WaitForResponse(t, 5*time.Second)
	var resp map[string]interface{}
	json.Unmarshal(respBytes, &resp)


	for {
		if idVal, ok := resp["id"].(float64); ok && idVal == 20 {
			break
		}
		respBytes = mc.WaitForResponse(t, 5*time.Second)
		resp = make(map[string]interface{})
		json.Unmarshal(respBytes, &resp)
	}
	resultObj, ok := resp["result"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected result object, got %v", string(respBytes))
	}
	statusObj, ok := resultObj["status"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected status object in result, got %v", resultObj)
	}
	
	if int(statusObj["error"].(float64)) != -32603 {
		t.Errorf("Expected error code -32603, got %v", statusObj["error"])
	}
	if statusObj["text"].(string) != "commit failed" {
		t.Errorf("Expected error text 'commit failed', got %v", statusObj["text"])
	}
	
	cancel()
}
