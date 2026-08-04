package websocket

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	gws "github.com/gorilla/websocket"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/config"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/contracts"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/queues"
)

type mockMetadataProvider struct{}

func (m *mockMetadataProvider) ConnectParams(ctx context.Context) (CloudConnectParams, error) {
	return CloudConnectParams{
		Serial:       "SERIAL123",
		Firmware:     "1.0.0",
		Capabilities: map[string]any{"model": "test-router"},
		UUID:         12345,
	}, nil
}

type mockFrameHandler struct {
	mu     sync.Mutex
	frames []InboundFrame
}

func (m *mockFrameHandler) HandleFrame(ctx context.Context, frame InboundFrame) (FrameDisposition, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.frames = append(m.frames, frame)
	return FrameAccepted, nil
}

func TestWSClient_HandshakeSuccess(t *testing.T) {
	upgrader := gws.Upgrader{}

	// Create a mock Cloud Server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		// Read the connect frame sent by the client
		_, payload, err := conn.ReadMessage()
		if err != nil {
			return
		}

		var req map[string]any
		if err := json.Unmarshal(payload, &req); err != nil {
			t.Errorf("failed to parse connect request: %v", err)
		}

		if req["method"] != "connect" {
			t.Errorf("expected connect method, got %v", req["method"])
		}

		// Reply with success
		resp := map[string]any{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"result": map[string]any{
				"status": "accepted",
			},
		}
		conn.WriteJSON(resp)

		// Keep connection open so the read/write loops can start
		time.Sleep(2 * time.Second)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	cfg := &config.CloudConfig{
		URL:                 wsURL,
		PingIntervalSeconds: 1,
		PongTimeoutSeconds:  5,
	}

	sched := queues.NewPriorityScheduler(10, 10)
	meta := &mockMetadataProvider{}

	var states []string
	var mu sync.Mutex

	onState := func(cloud contracts.LinkState, protocol contracts.ProtocolState) {
		mu.Lock()
		defer mu.Unlock()
		states = append(states, string(cloud)+"-"+string(protocol))
	}

	client := NewWSClient(*cfg, sched, meta, onState)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := &mockFrameHandler{}

	// Run the reconnect loop in the background
	go client.ReconnectLoop(ctx, handler)

	// Give the client time to connect and handshake
	time.Sleep(1 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	// Validate the exact state machine transitions
	if len(states) < 3 {
		t.Fatalf("expected at least 3 state transitions, got %v", states)
	}
	if states[0] != "connecting-unknown" {
		t.Errorf("expected connecting-unknown, got %s", states[0])
	}
	if states[1] != "connected-verifying" {
		t.Errorf("expected connected-verifying, got %s", states[1])
	}
	if states[2] != "connected-accepted" {
		t.Errorf("expected connected-accepted, got %s", states[2])
	}
}

func TestWSClient_HandshakeRejected(t *testing.T) {
	upgrader := gws.Upgrader{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		_, payload, err := conn.ReadMessage()
		if err != nil {
			return
		}

		var req map[string]any
		json.Unmarshal(payload, &req)

		// Reply with a rejection error!
		resp := map[string]any{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"error": map[string]any{
				"code":    -32603,
				"message": "protocol version rejected",
			},
		}
		conn.WriteJSON(resp)
		time.Sleep(1 * time.Second)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	cfg := &config.CloudConfig{
		URL: wsURL,
	}

	sched := queues.NewPriorityScheduler(10, 10)
	meta := &mockMetadataProvider{}

	var states []string
	var mu sync.Mutex

	onState := func(cloud contracts.LinkState, protocol contracts.ProtocolState) {
		mu.Lock()
		defer mu.Unlock()
		states = append(states, string(cloud)+"-"+string(protocol))
	}

	client := NewWSClient(*cfg, sched, meta, onState)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := &mockFrameHandler{}

	go client.ReconnectLoop(ctx, handler)

	time.Sleep(1 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	// Wait, the client should transition back to connecting-unknown upon disconnect
	if len(states) < 3 {
		t.Fatalf("expected at least 3 state transitions, got %v", states)
	}
	if states[0] != "connecting-unknown" {
		t.Errorf("expected connecting-unknown, got %s", states[0])
	}
	if states[1] != "connected-verifying" {
		t.Errorf("expected connected-verifying, got %s", states[1])
	}
	if states[2] != "connecting-unknown" {
		t.Errorf("expected connecting-unknown after rejection, got %s", states[2])
	}
}

func TestWSClient_11MBFrameLimit(t *testing.T) {
	upgrader := gws.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, payload, _ := conn.ReadMessage()
		var req map[string]any
		json.Unmarshal(payload, &req)
		resp := map[string]any{"jsonrpc": "2.0", "id": req["id"], "result": map[string]any{"status": "accepted"}}
		conn.WriteJSON(resp)
		time.Sleep(100 * time.Millisecond)

		// Send 12MB frame (exceeds 11MB limit)
		largePayload := make([]byte, 12*1024*1024)
		conn.WriteMessage(gws.TextMessage, largePayload)
		time.Sleep(1 * time.Second)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	cfg := &config.CloudConfig{URL: wsURL}

	var states []string
	var mu sync.Mutex
	stateCh := make(chan string, 10)

	onState := func(cloud contracts.LinkState, protocol contracts.ProtocolState) {
		mu.Lock()
		defer mu.Unlock()
		s := string(cloud) + "-" + string(protocol)
		states = append(states, s)
		stateCh <- s
	}

	client := NewWSClient(*cfg, queues.NewPriorityScheduler(10, 10), &mockMetadataProvider{}, onState)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.ReconnectLoop(ctx, &mockFrameHandler{})

	// We expect: connecting-unknown -> connected-verifying -> connected-accepted -> (crash) -> connecting-unknown
	crashObserved := false
	for i := 0; i < 4; i++ {
		select {
		case s := <-stateCh:
			if i == 3 && s == "connecting-unknown" {
				crashObserved = true
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for state transitions. Current states: %v", states)
		}
	}

	if !crashObserved {
		t.Errorf("expected socket to crash and return to connecting-unknown after 12MB frame. States: %v", states)
	}
}

func TestWSClient_HandshakeTimeout(t *testing.T) {
	upgrader := gws.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		// Server accepts socket, but never replies to JSON-RPC connect!
		time.Sleep(5 * time.Second)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	// Set an aggressive 1-second connect timeout for the test!
	cfg := &config.CloudConfig{URL: wsURL, ConnectTimeoutSeconds: 1}

	var states []string
	var mu sync.Mutex
	stateCh := make(chan string, 10)

	onState := func(cloud contracts.LinkState, protocol contracts.ProtocolState) {
		mu.Lock()
		defer mu.Unlock()
		s := string(cloud) + "-" + string(protocol)
		states = append(states, s)
		stateCh <- s
	}

	client := NewWSClient(*cfg, queues.NewPriorityScheduler(10, 10), &mockMetadataProvider{}, onState)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.ReconnectLoop(ctx, &mockFrameHandler{})

	// Expect: connecting-unknown -> connected-verifying -> (timeout) -> connecting-unknown
	timeoutObserved := false
	for i := 0; i < 3; i++ {
		select {
		case s := <-stateCh:
			if i == 2 && s == "connecting-unknown" {
				timeoutObserved = true
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("test timed out waiting for handshake timeout. States: %v", states)
		}
	}

	if !timeoutObserved {
		t.Errorf("expected client to timeout and revert to connecting-unknown. States: %v", states)
	}
}

func TestWSClient_TLSVerification(t *testing.T) {
	upgrader := gws.Upgrader{}
	// NewTLSServer uses a self-signed certificate!
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err == nil {
			conn.Close()
		}
	}))
	defer server.Close()

	wsURL := "wss" + strings.TrimPrefix(server.URL, "https")
	cfg := &config.CloudConfig{URL: wsURL}

	var mu sync.Mutex
	var states []string
	onState := func(cloud contracts.LinkState, protocol contracts.ProtocolState) {
		mu.Lock()
		defer mu.Unlock()
		states = append(states, string(cloud)+"-"+string(protocol))
	}

	client := NewWSClient(*cfg, queues.NewPriorityScheduler(10, 10), &mockMetadataProvider{}, onState)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.ReconnectLoop(ctx, &mockFrameHandler{})

	time.Sleep(1 * time.Second)

	mu.Lock()
	defer mu.Unlock()
	for _, s := range states {
		if s == "connected-verifying" {
			t.Errorf("client successfully dialed a self-signed TLS server! Expected strict rejection.")
		}
	}
}

func TestWSClient_PingPongHeartbeat(t *testing.T) {
	upgrader := gws.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, payload, _ := conn.ReadMessage()
		var req map[string]any
		json.Unmarshal(payload, &req)
		resp := map[string]any{"jsonrpc": "2.0", "id": req["id"], "result": map[string]any{"status": "accepted"}}
		conn.WriteJSON(resp)

		pingReceived := make(chan bool, 1)
		conn.SetPingHandler(func(appData string) error {
			select {
			case pingReceived <- true:
			default:
			}
			return conn.WriteMessage(gws.PongMessage, []byte(appData))
		})

		// Start a dummy reader to process control frames (like ping)
		go func() {
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		}()

		select {
		case <-pingReceived:
			// Test passes!
			return
		case <-time.After(4 * time.Second):
			t.Errorf("failed to receive ping heartbeat from client within 4 seconds")
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	cfg := &config.CloudConfig{URL: wsURL, PingIntervalSeconds: 1, PongTimeoutSeconds: 5}
	client := NewWSClient(*cfg, queues.NewPriorityScheduler(10, 10), &mockMetadataProvider{}, func(c contracts.LinkState, p contracts.ProtocolState) {})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.ReconnectLoop(ctx, &mockFrameHandler{})
	time.Sleep(3 * time.Second)
}

func TestWSClient_PingDuringVerification(t *testing.T) {
	upgrader := gws.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		
		// Read the connect req first!
		_, connectPayload, _ := conn.ReadMessage()

		// Send a JSON-RPC ping BEFORE replying to connect!
		pingReq := map[string]any{"jsonrpc": "2.0", "method": "ping", "id": 999}
		conn.WriteJSON(pingReq)

		// Wait for the ping reply
		_, payload, _ := conn.ReadMessage()
		var reply map[string]any
		json.Unmarshal(payload, &reply)

		if reply["id"].(float64) != 999 {
			t.Errorf("expected ping reply id 999, got %v", reply["id"])
		}
		result, ok := reply["result"].(map[string]any)
		if !ok || result["serial"] != "SERIAL123" {
			t.Errorf("expected ping reply serial SERIAL123, got %v", reply["result"])
		}

		// Now finally accept the connect handshake using the connect request's ID
		var req map[string]any
		json.Unmarshal(connectPayload, &req)
		resp := map[string]any{"jsonrpc": "2.0", "id": req["id"], "result": map[string]any{"status": "accepted"}}
		conn.WriteJSON(resp)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	cfg := &config.CloudConfig{URL: wsURL}
	client := NewWSClient(*cfg, queues.NewPriorityScheduler(10, 10), &mockMetadataProvider{}, func(c contracts.LinkState, p contracts.ProtocolState) {})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	go client.ReconnectLoop(ctx, &mockFrameHandler{})
	time.Sleep(1 * time.Second) // Let handshake finish
}

func TestWSClient_StalePriority0(t *testing.T) {
	msgReceived := make(chan string, 2)
	upgrader := gws.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		
		conn.ReadMessage() // read connect req
		resp := map[string]any{"jsonrpc": "2.0", "id": 1, "result": map[string]any{"status": "accepted"}}
		conn.WriteJSON(resp)

		// Read outbound messages from the client queue
		for {
			_, payload, err := conn.ReadMessage()
			if err != nil {
				return
			}
			msgReceived <- string(payload)
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	cfg := &config.CloudConfig{URL: wsURL}
	scheduler := queues.NewPriorityScheduler(10, 10)
	client := NewWSClient(*cfg, scheduler, &mockMetadataProvider{}, func(c contracts.LinkState, p contracts.ProtocolState) {})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.ReconnectLoop(ctx, &mockFrameHandler{})
	time.Sleep(1 * time.Second) // wait for connect

	client.mu.Lock()
	activeSess := fmt.Sprintf("sess-%d", client.generation)
	client.mu.Unlock()

	// Push a stale P0 message
	scheduler.Push(queues.OutboundMessage{
		SessionID: "sess-old",
		Priority:  queues.PriorityHighest,
		Payload:   []byte("stale-p0"),
	})

	// Push a valid P1 message
	scheduler.Push(queues.OutboundMessage{
		SessionID: "sess-old", // P1 ignores session ID filtering
		Priority:  queues.PriorityHigh,
		Payload:   []byte("valid-p1"),
	})

	// Push a valid P0 message
	scheduler.Push(queues.OutboundMessage{
		SessionID: activeSess,
		Priority:  queues.PriorityHighest,
		Payload:   []byte("valid-p0"),
	})

	// We expect to only receive "valid-p0" then "valid-p1"
	for i := 0; i < 2; i++ {
		select {
		case msg := <-msgReceived:
			if msg == "stale-p0" {
				t.Errorf("client failed to drop stale Priority-0 message")
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for outbound messages")
		}
	}
}
