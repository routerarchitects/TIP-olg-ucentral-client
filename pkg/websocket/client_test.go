package websocket

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
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

		hsPingReceived := make(chan struct{})
		conn.SetPingHandler(func(appData string) error {
			select {
			case <-hsPingReceived:
			default:
				close(hsPingReceived)
			}
			return conn.WriteControl(gws.PongMessage, []byte(appData), time.Now().Add(time.Second))
		})

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

		// Read the next message in a goroutine so gorilla/websocket can process the Ping control frame
		go func() {
			conn.ReadMessage()
		}()

		<-hsPingReceived
		conn.WriteJSON(map[string]any{"ping": map[string]any{"serialNumber": "SERIAL123"}})

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

	client, _ := NewWSClient(*cfg, sched, meta, onState)

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

func TestWSClient_PingValidation(t *testing.T) {
	cases := []struct {
		name     string
		response any
		valid    bool
	}{
		{
			name:     "valid ping",
			response: map[string]any{"ping": map[string]any{"serialNumber": "SERIAL123"}},
			valid:    true,
		},
		{
			name:     "null ping",
			response: map[string]any{"ping": nil},
			valid:    false,
		},
		{
			name:     "empty ping object",
			response: map[string]any{"ping": map[string]any{}},
			valid:    false,
		},
		{
			name:     "wrong serial",
			response: map[string]any{"ping": map[string]any{"serialNumber": "OTHER"}},
			valid:    false,
		},
		{
			name:     "malformed json",
			response: "not-json", // will write raw text
			valid:    false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			upgrader := gws.Upgrader{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer conn.Close()

				// wait for connect frame
				conn.ReadMessage() // connect
				hsPingReceived := make(chan struct{})
				conn.SetPingHandler(func(appData string) error {
					select {
					case <-hsPingReceived:
					default:
						close(hsPingReceived)
					}
					return conn.WriteControl(10, []byte(appData), time.Now().Add(time.Second))
				})
				go func() { conn.ReadMessage() }()
				<-hsPingReceived
				// Send an unrelated command before the valid ping to prove we skip it
				if tc.name == "valid ping" {
					conn.WriteJSON(map[string]any{"method": "upgrade", "jsonrpc": "2.0"})
				}

				if s, ok := tc.response.(string); ok {
					conn.WriteMessage(gws.TextMessage, []byte(s))
				} else {
					conn.WriteJSON(tc.response)
				}

				// If not valid, don't send anything else. Client will timeout.
				// Let's close the connection explicitly after writing invalid to unblock the reader quickly!
				if !tc.valid {
					conn.Close()
				} else {
					time.Sleep(100 * time.Millisecond)
				}
			}))
			defer server.Close()

			wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
			cfg := &config.CloudConfig{
				URL:                   wsURL,
				ConnectTimeoutSeconds: 1, // fast timeout for the tests
			}

			sched := queues.NewPriorityScheduler(10, 10)
			client, err := NewWSClient(*cfg, sched, &mockMetadataProvider{}, func(c contracts.LinkState, p contracts.ProtocolState) {})
			if err != nil {
				t.Fatalf("failed to create client: %v", err)
			}

			conn, _, err := gws.DefaultDialer.Dial(cfg.URL, nil)
			if err != nil {
				t.Fatalf("failed to dial: %v", err)
			}
			defer conn.Close()

			res := client.performConnectHandshake(context.Background(), conn)

			if tc.valid && res != HandshakeAccepted {
				t.Errorf("expected handshake to be accepted")
			} else if !tc.valid && res == HandshakeAccepted {
				t.Errorf("expected handshake to be rejected")
			}
		})
	}
}

type mockEmptySerialProvider struct{}

func (m *mockEmptySerialProvider) ConnectParams(ctx context.Context) (CloudConnectParams, error) {
	return CloudConnectParams{
		Serial:       "   ", // explicitly whitespace
		Firmware:     "mock-fw",
		UUID:         42,
		Capabilities: map[string]any{"test": true},
	}, nil
}

func TestWSClient_EmptyLocalSerial(t *testing.T) {
	cfg := &config.CloudConfig{
		URL:                   "ws://example.com", // shouldn't even connect
		ConnectTimeoutSeconds: 1,
	}

	sched := queues.NewPriorityScheduler(10, 10)
	client, _ := NewWSClient(*cfg, sched, &mockEmptySerialProvider{}, func(c contracts.LinkState, p contracts.ProtocolState) {})

	res := client.performConnectHandshake(context.Background(), &gws.Conn{})

	if res != HandshakeRetryableFailure {
		t.Errorf("expected handshake to be rejected due to empty serial, got %v", res)
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
		hsPingReceived := make(chan struct{})
		conn.SetPingHandler(func(appData string) error {
			select {
			case <-hsPingReceived:
			default:
				close(hsPingReceived)
			}
			return conn.WriteControl(10, []byte(appData), time.Now().Add(time.Second))
		})
		go func() { conn.ReadMessage() }()
		<-hsPingReceived
		conn.WriteJSON(map[string]any{"ping": map[string]any{"serialNumber": "SERIAL123"}})

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

	client, _ := NewWSClient(*cfg, queues.NewPriorityScheduler(10, 10), &mockMetadataProvider{}, onState)
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

	client, _ := NewWSClient(*cfg, queues.NewPriorityScheduler(10, 10), &mockMetadataProvider{}, onState)
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
	pingVerified := make(chan bool, 1)
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
		hsPingReceived := make(chan struct{})
		pingReceived := make(chan bool, 1)
		
		conn.SetPingHandler(func(appData string) error {
			select {
			case <-hsPingReceived:
				select {
				case pingReceived <- true:
					select {
					case pingVerified <- true:
					default:
					}
				default:
				}
			default:
				close(hsPingReceived)
			}
			return conn.WriteControl(gws.PongMessage, []byte(appData), time.Now().Add(time.Second))
		})
		
		go func() {
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		}()
		
		<-hsPingReceived
		conn.WriteJSON(map[string]any{"ping": map[string]any{"serialNumber": "SERIAL123"}})

		select {
		case <-pingReceived:
			return
		case <-time.After(4 * time.Second):
			return
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	cfg := &config.CloudConfig{URL: wsURL, PingIntervalSeconds: 1, PongTimeoutSeconds: 5}
	client, _ := NewWSClient(*cfg, queues.NewPriorityScheduler(10, 10), &mockMetadataProvider{}, func(c contracts.LinkState, p contracts.ProtocolState) {})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.ReconnectLoop(ctx, &mockFrameHandler{})
	select {
	case <-pingVerified:
		// Test passes!
	case <-time.After(4 * time.Second):
		t.Fatalf("failed to receive ping heartbeat from client within 4 seconds")
	}
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
		conn.WriteJSON(map[string]any{"ping": map[string]any{"serialNumber": "SERIAL123"}})

		conn.ReadMessage() // read connect req
		resp := map[string]any{"jsonrpc": "2.0", "id": 1, "result": map[string]any{"status": map[string]any{"error": 0, "text": "Success"}}}
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
	client, _ := NewWSClient(*cfg, scheduler, &mockMetadataProvider{}, func(c contracts.LinkState, p contracts.ProtocolState) {})
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

func TestWSClient_ReconnectThrottling(t *testing.T) {
	upgrader := gws.Upgrader{}
	var mu sync.Mutex
	var connectTimes []time.Time

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(map[string]any{"ping": map[string]any{"serialNumber": "SERIAL123"}})

		mu.Lock()
		connectTimes = append(connectTimes, time.Now())
		count := len(connectTimes)
		mu.Unlock()

		if count > 3 {
			return
		}

		// Accept handshake
		var req map[string]any
		conn.ReadJSON(&req)

		// Immediately drop connection (less than 60s stable duration)
		conn.Close()
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	cfg := &config.CloudConfig{URL: wsURL}
	client, _ := NewWSClient(*cfg, queues.NewPriorityScheduler(1, 1), &mockMetadataProvider{}, func(c contracts.LinkState, p contracts.ProtocolState) {})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.ReconnectLoop(ctx, &mockFrameHandler{})
	time.Sleep(10 * time.Second) // Wait for at least 3 attempts with backoff (2s + 4s)

	mu.Lock()
	defer mu.Unlock()

	if len(connectTimes) < 3 {
		t.Fatalf("expected at least 3 connections, got %d", len(connectTimes))
	}

	d1 := connectTimes[1].Sub(connectTimes[0])
	d2 := connectTimes[2].Sub(connectTimes[1])

	// First wait should be ~2s (allow 1.5 - 3.0s)
	if d1 < 1500*time.Millisecond || d1 > 3500*time.Millisecond {
		t.Errorf("expected 2s backoff, got %v", d1)
	}
	// Second wait should be ~4s (allow 3.5 - 5.5s)
	if d2 < 3500*time.Millisecond || d2 > 5500*time.Millisecond {
		t.Errorf("expected 4s backoff, got %v", d2)
	}
}

func TestWSClient_PingControlDeadlineRefresh(t *testing.T) {
	upgrader := gws.Upgrader{}
	serverMessageReceived := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(map[string]any{"ping": map[string]any{"serialNumber": "SERIAL123"}})

		var req map[string]any
		conn.ReadJSON(&req)

		// Send pings every 500ms to keep connection alive
		go func() {
			for i := 0; i < 6; i++ {
				time.Sleep(500 * time.Millisecond)
				conn.WriteControl(gws.PingMessage, []byte{}, time.Now().Add(time.Second))
			}
			// Finally send a text message after 3 seconds
			conn.WriteMessage(gws.TextMessage, []byte(`{"method":"ping"}`))
		}()

		_, _, err = conn.ReadMessage()
		if err != nil {
			return
		}
		close(serverMessageReceived)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	cfg := &config.CloudConfig{
		URL:                wsURL,
		PongTimeoutSeconds: 2, // 2s timeout
	}
	client, _ := NewWSClient(*cfg, queues.NewPriorityScheduler(1, 1), &mockMetadataProvider{}, func(c contracts.LinkState, p contracts.ProtocolState) {})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.ReconnectLoop(ctx, &mockFrameHandler{})

	// Wait 4 seconds to ensure the 2s deadline would have tripped if not refreshed
	time.Sleep(4 * time.Second)

	client.mu.Lock()
	connAlive := client.conn != nil
	client.mu.Unlock()

	if !connAlive {
		t.Errorf("client connection was dropped despite ping refresh")
	}
}

func TestWSClient_ConfiguredWriteTimeout(t *testing.T) {
	upgrader := gws.Upgrader{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(map[string]any{"ping": map[string]any{"serialNumber": "SERIAL123"}})

		var req map[string]any
		conn.ReadJSON(&req)

		// Block reads completely so the client TCP buffer fills and writes block
		time.Sleep(10 * time.Second)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	cfg := &config.CloudConfig{
		URL:                 wsURL,
		WriteTimeoutSeconds: 1, // Fail fast on write
	}
	sched := queues.NewPriorityScheduler(10, 10)
	client, _ := NewWSClient(*cfg, sched, &mockMetadataProvider{}, func(c contracts.LinkState, p contracts.ProtocolState) {})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.ReconnectLoop(ctx, &mockFrameHandler{})
	time.Sleep(1 * time.Second)

	client.mu.Lock()
	sessID := fmt.Sprintf("sess-%d", client.generation)
	client.mu.Unlock()

	// Fill the buffer with a huge message
	hugePayload := make([]byte, 10*1024*1024)
	sched.Push(queues.OutboundMessage{
		SessionID: sessID,
		Priority:  queues.PriorityHigh,
		Payload:   hugePayload,
	})

	// Monitor how long the connection takes to drop
	start := time.Now()
	for {
		client.mu.Lock()
		active := client.conn != nil
		client.mu.Unlock()
		if !active {
			break
		}
		if time.Since(start) > 5*time.Second {
			t.Fatalf("write timeout did not drop connection in time")
		}
		time.Sleep(100 * time.Millisecond)
	}

	duration := time.Since(start)
	if duration > 3*time.Second {
		t.Errorf("expected write failure around 1s, took %v", duration)
	}
}

func TestWSClient_TLSInvalidCAFile(t *testing.T) {
	// Create a temp file with invalid PEM data
	tmpfile, err := os.CreateTemp("", "invalid_ca_*.pem")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Write([]byte("this is not a valid pem certificate data"))
	tmpfile.Close()

	cfg := &config.CloudConfig{
		URL: "wss://example.com",
		TLS: config.CloudTLSConfig{
			CAFile: tmpfile.Name(),
		},
	}

	client, _ := NewWSClient(*cfg, queues.NewPriorityScheduler(1, 1), &mockMetadataProvider{}, func(c contracts.LinkState, p contracts.ProtocolState) {})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = client.ReconnectLoop(ctx, &mockFrameHandler{})
	if err == nil {
		t.Fatalf("expected error from ReconnectLoop, got nil")
	}
	if !strings.Contains(err.Error(), "failed to parse any valid certificates from CA file") {
		t.Fatalf("expected parse error, got: %v", err)
	}
}

func TestWSClient_StableSessionThreshold(t *testing.T) {
	upgrader := gws.Upgrader{}
	var mu sync.Mutex
	var connectTimes []time.Time

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(map[string]any{"ping": map[string]any{"serialNumber": "SERIAL123"}})

		mu.Lock()
		connectTimes = append(connectTimes, time.Now())
		count := len(connectTimes)
		mu.Unlock()

		if count > 3 {
			return
		}

		// Accept handshake
		var req map[string]any
		conn.ReadJSON(&req)

		// Sleep for 2 seconds to exceed the stable threshold of 1s
		time.Sleep(2 * time.Second)
		conn.Close()
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	cfg := &config.CloudConfig{
		URL:                           wsURL,
		StableSessionThresholdSeconds: 1,
	}
	client, _ := NewWSClient(*cfg, queues.NewPriorityScheduler(1, 1), &mockMetadataProvider{}, func(c contracts.LinkState, p contracts.ProtocolState) {})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.ReconnectLoop(ctx, &mockFrameHandler{})
	time.Sleep(12 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	if len(connectTimes) < 3 {
		t.Fatalf("expected at least 3 connections, got %d", len(connectTimes))
	}

	d1 := connectTimes[1].Sub(connectTimes[0])
	d2 := connectTimes[2].Sub(connectTimes[1])

	// Both delays should be ~4s (2s server sleep + 2s reset backoff)
	// (allow 3.5 - 5.5s)
	if d1 < 3500*time.Millisecond || d1 > 5500*time.Millisecond {
		t.Errorf("expected ~4s total delay for first reconnect, got %v", d1)
	}
	if d2 < 3500*time.Millisecond || d2 > 5500*time.Millisecond {
		t.Errorf("expected ~4s total delay for second reconnect (backoff reset), got %v", d2)
	}
}
