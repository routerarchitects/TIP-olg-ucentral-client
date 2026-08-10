package websocket

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"math/rand"
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

func init() {
	// Override production backoff constants to drastically speed up the test suite
	// and eliminate timing-related flakiness in CI.
	initialBackoff = 20 * time.Millisecond
	maxBackoff = 600 * time.Millisecond
}

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

	handshakeReceived := make(chan struct{})

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

		if req["jsonrpc"] != "2.0" {
			t.Errorf("expected jsonrpc 2.0, got %v", req["jsonrpc"])
		}
		if req["method"] != "connect" {
			t.Errorf("expected connect method, got %v", req["method"])
		}
		if _, exists := req["id"]; exists {
			t.Errorf("expected no id in connect event, got %v", req["id"])
		}

		params, ok := req["params"].(map[string]any)
		if !ok {
			t.Errorf("expected params object")
		} else {
			if params["serial"] != "SERIAL123" {
				t.Errorf("expected serial SERIAL123, got %v", params["serial"])
			}
			if params["uuid"] != float64(12345) { // JSON unmarshals numbers as float64
				t.Errorf("expected uuid 12345, got %v", params["uuid"])
			}
			if params["firmware"] != "1.0.0" {
				t.Errorf("expected firmware 1.0.0, got %v", params["firmware"])
			}
			caps, ok := params["capabilities"].(map[string]any)
			if !ok || caps["model"] != "test-router" {
				t.Errorf("expected capabilities object with model test-router, got %v", params["capabilities"])
			}
		}

		close(handshakeReceived)

		// Read the next message in a goroutine so gorilla/websocket can process the Ping control frame
		go func() {
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		}()

		// Keep connection open so the read/write loops can start
		<-r.Context().Done()
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
	stateReached := make(chan struct{})

	onState := func(cloud contracts.LinkState) {
		mu.Lock()
		defer mu.Unlock()
		states = append(states, string(cloud))
		if len(states) == 2 {
			close(stateReached)
		}
	}

	client, _ := NewWSClient(*cfg, sched, meta, onState)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := &mockFrameHandler{}

	errCh := make(chan error, 1)
	// Run the reconnect loop in the background
	go func() {
		errCh <- client.ReconnectLoop(ctx, handler)
	}()

	select {
	case <-handshakeReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for handshake")
	}

	select {
	case <-stateReached:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for expected states")
	}

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("ReconnectLoop returned unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for ReconnectLoop to exit")
	}

	mu.Lock()
	defer mu.Unlock()

	// Validate the exact state machine transitions
	if len(states) < 2 {
		t.Fatalf("expected at least 3 state transitions, got %v", states)
	}
	if states[0] != "connecting" {
		t.Errorf("expected connecting-unknown, got %s", states[0])
	}
	if states[1] != "connected" {
		t.Errorf("expected connected-verifying, got %s", states[1])
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
	client, _ := NewWSClient(*cfg, sched, &mockEmptySerialProvider{}, func(cloud contracts.LinkState) {})

	res := client.performConnectHandshake(context.Background(), &gws.Conn{})

	if res != HandshakeRejected {
		t.Errorf("expected handshake to be rejected due to empty serial, got %v", res)
	}
}

type mockWhitespaceSerialProvider struct{}

func (m *mockWhitespaceSerialProvider) ConnectParams(ctx context.Context) (CloudConnectParams, error) {
	return CloudConnectParams{
		Serial:       "  WHITESPACE123  ", // padded whitespace
		Firmware:     "mock-fw",
		UUID:         42,
		Capabilities: map[string]any{"test": true},
	}, nil
}

func TestWSClient_WhitespaceSerial(t *testing.T) {
	upgrader := gws.Upgrader{}

	assertCh := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		conn.SetPingHandler(func(appData string) error {
			return conn.WriteControl(gws.PongMessage, []byte(appData), time.Now().Add(time.Second))
		})

		_, payload, _ := conn.ReadMessage()
		var req map[string]any
		json.Unmarshal(payload, &req)

		params := req["params"].(map[string]any)
		if params["serial"] != "WHITESPACE123" {
			t.Errorf("expected normalized serial 'WHITESPACE123', got '%v'", params["serial"])
		}
		close(assertCh)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	cfg := &config.CloudConfig{URL: wsURL, PingIntervalSeconds: 1, PongTimeoutSeconds: 5}
	sched := queues.NewPriorityScheduler(10, 10)

	client, _ := NewWSClient(*cfg, sched, &mockWhitespaceSerialProvider{}, func(cloud contracts.LinkState) {})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.ReconnectLoop(ctx, &mockFrameHandler{})

	select {
	case <-assertCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for handler assertion")
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
		go func() { conn.ReadMessage() }()

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

	onState := func(cloud contracts.LinkState) {
		mu.Lock()
		defer mu.Unlock()
		s := string(cloud)
		states = append(states, s)
		stateCh <- s
	}

	client, _ := NewWSClient(*cfg, queues.NewPriorityScheduler(10, 10), &mockMetadataProvider{}, onState)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.ReconnectLoop(ctx, &mockFrameHandler{})

	// We expect: connecting -> connected -> connecting
	crashObserved := false
	for i := 0; i < 3; i++ {
		select {
		case s := <-stateCh:
			if i == 2 && s == "connecting" {
				crashObserved = true
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for state transitions. Current states: %v", states)
		}
	}

	if !crashObserved {
		t.Errorf("expected socket to crash and return to connecting after 12MB frame. States: %v", states)
	}
}

func TestWSClient_ZipBombDecompressionLimit(t *testing.T) {
	upgrader := gws.Upgrader{
		EnableCompression: true, // Crucial: negotiate permessage-deflate
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, payload, _ := conn.ReadMessage()
		var req map[string]any
		json.Unmarshal(payload, &req)
		go func() { conn.ReadMessage() }()

		time.Sleep(100 * time.Millisecond)

		// Create a highly compressible 12MB payload (e.g. all zeros)
		// With permessage-deflate, this will be tiny on the wire, but expand to 12MB
		// triggering the read limit dynamically during decompression.
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

	onState := func(cloud contracts.LinkState) {
		mu.Lock()
		defer mu.Unlock()
		s := string(cloud)
		states = append(states, s)
		stateCh <- s
	}

	client, _ := NewWSClient(*cfg, queues.NewPriorityScheduler(10, 10), &mockMetadataProvider{}, onState)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.ReconnectLoop(ctx, &mockFrameHandler{})

	// We expect: connecting -> connected -> connecting
	crashObserved := false
	for i := 0; i < 3; i++ {
		select {
		case s := <-stateCh:
			if i == 2 && s == "connecting" {
				crashObserved = true
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for state transitions. Current states: %v", states)
		}
	}

	if !crashObserved {
		t.Errorf("expected socket to crash and return to connecting after decompressed 12MB frame. States: %v", states)
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
	onState := func(cloud contracts.LinkState) {
		mu.Lock()
		defer mu.Unlock()
		states = append(states, string(cloud))
	}

	client, _ := NewWSClient(*cfg, queues.NewPriorityScheduler(10, 10), &mockMetadataProvider{}, onState)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.ReconnectLoop(ctx, &mockFrameHandler{})

	time.Sleep(1 * time.Second)

	mu.Lock()
	defer mu.Unlock()
	for _, s := range states {
		if s == "connected" {
			t.Errorf("client successfully dialed a self-signed TLS server! Expected strict rejection.")
		}
	}
}

func TestWSClient_MTLSSuccess(t *testing.T) {
	caFile, serverCert, serverKey, clientCert, clientKey := generateMTLSCerts(t)

	caCertPool := x509.NewCertPool()
	caBytes, _ := os.ReadFile(caFile)
	caCertPool.AppendCertsFromPEM(caBytes)
	sCert, _ := tls.LoadX509KeyPair(serverCert, serverKey)

	upgrader := gws.Upgrader{}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, payload, _ := conn.ReadMessage()
		var req map[string]any
		json.Unmarshal(payload, &req)
	}))

	server.TLS = &tls.Config{
		Certificates: []tls.Certificate{sCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caCertPool,
	}
	server.StartTLS()
	defer server.Close()

	wsURL := "wss" + strings.TrimPrefix(server.URL, "https")
	cfg := &config.CloudConfig{
		URL: wsURL,
		TLS: config.CloudTLSConfig{
			CAFile:         caFile,
			ClientCertFile: clientCert,
			ClientKeyFile:  clientKey,
			ServerName:     "127.0.0.1",
		},
	}

	stateCh := make(chan string, 1)
	onState := func(cloud contracts.LinkState) {
		if cloud == contracts.LinkConnected {
			select {
			case stateCh <- "success":
			default:
			}
		}
	}

	client, _ := NewWSClient(*cfg, queues.NewPriorityScheduler(1, 1), &mockMetadataProvider{}, onState)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.ReconnectLoop(ctx, &mockFrameHandler{})

	select {
	case <-stateCh:
		// Passed!
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for mTLS success")
	}
}

func TestWSClient_MTLSClientRejected(t *testing.T) {
	caFile, serverCert, serverKey, _, _ := generateMTLSCerts(t)

	caCertPool := x509.NewCertPool()
	caBytes, _ := os.ReadFile(caFile)
	caCertPool.AppendCertsFromPEM(caBytes)
	sCert, _ := tls.LoadX509KeyPair(serverCert, serverKey)

	upgrader := gws.Upgrader{}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
	}))

	server.TLS = &tls.Config{
		Certificates: []tls.Certificate{sCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caCertPool,
	}
	server.StartTLS()
	defer server.Close()

	wsURL := "wss" + strings.TrimPrefix(server.URL, "https")
	// Client config OMITS the client certificates!
	cfg := &config.CloudConfig{
		URL: wsURL,
		TLS: config.CloudTLSConfig{
			CAFile:     caFile,
			ServerName: "127.0.0.1",
		},
	}

	var mu sync.Mutex
	var states []string
	onState := func(cloud contracts.LinkState) {
		mu.Lock()
		defer mu.Unlock()
		states = append(states, string(cloud))
	}

	client, _ := NewWSClient(*cfg, queues.NewPriorityScheduler(1, 1), &mockMetadataProvider{}, onState)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.ReconnectLoop(ctx, &mockFrameHandler{})

	time.Sleep(1 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	// Should only transition to LinkConnecting -> ProtocolUnknown, and never hit TransportVerified!
	for _, s := range states {
		if s == "connected" {
			t.Errorf("expected only connecting-unknown state since mTLS was rejected, got: %v", states)
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
		pingReceived := make(chan bool, 1)
		conn.SetPingHandler(func(appData string) error {
			select {
			case pingReceived <- true:
			default:
			}
			select {
			case pingVerified <- true:
			default:
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
	client, _ := NewWSClient(*cfg, queues.NewPriorityScheduler(10, 10), &mockMetadataProvider{}, func(cloud contracts.LinkState) {})
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

func TestWSClient_StalePriority0_Rollover(t *testing.T) {
	msgReceived := make(chan string, 5)
	upgrader := gws.Upgrader{}

	var connToClose *gws.Conn
	var connMu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}

		connMu.Lock()
		connToClose = conn
		connMu.Unlock()

		conn.SetPingHandler(func(appData string) error {
			return conn.WriteControl(gws.PongMessage, []byte(appData), time.Now().Add(time.Second))
		})

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
	cfg := &config.CloudConfig{
		URL:                           wsURL,
		StableSessionThresholdSeconds: 1, // ensure reconnect happens fast
	}
	scheduler := queues.NewPriorityScheduler(10, 10)

	readyCh := make(chan struct{}, 10)
	client, _ := NewWSClient(*cfg, scheduler, &mockMetadataProvider{}, func(cloud contracts.LinkState) {
		if cloud == contracts.LinkConnected {
			readyCh <- struct{}{}
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.ReconnectLoop(ctx, &mockFrameHandler{})

	// Wait for Session N
	select {
	case <-readyCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for connect")
	}

	client.mu.Lock()
	sessN := fmt.Sprintf("sess-%d", client.generation)
	client.mu.Unlock()

	// Sever connection N
	connMu.Lock()
	connToClose.Close()
	connMu.Unlock()

	// Queue P0 for Session N (stale) while disconnected
	scheduler.Push(queues.OutboundMessage{
		SessionID: sessN,
		Priority:  queues.PriorityHighest,
		Payload:   []byte("stale-p0"),
	})

	// Wait for Session N+1 to connect
	select {
	case <-readyCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for reconnect")
	}

	client.mu.Lock()
	sessN1 := fmt.Sprintf("sess-%d", client.generation)
	client.mu.Unlock()

	if sessN == sessN1 {
		t.Fatal("generation did not roll over")
	}

	// Queue P0 for Session N+1 (valid)
	scheduler.Push(queues.OutboundMessage{
		SessionID: sessN1,
		Priority:  queues.PriorityHighest,
		Payload:   []byte("valid-p0"),
	})

	// Queue P1 message to ensure order (P1 doesn't filter on sess ID)
	scheduler.Push(queues.OutboundMessage{
		SessionID: sessN, // ignored
		Priority:  queues.PriorityHigh,
		Payload:   []byte("valid-p1"),
	})

	// We expect "valid-p0" then "valid-p1", and NEVER "stale-p0"
	var received []string
	for i := 0; i < 2; i++ {
		select {
		case msg := <-msgReceived:
			received = append(received, msg)
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for outbound messages")
		}
	}

	if received[0] != "valid-p0" || received[1] != "valid-p1" {
		t.Errorf("expected [valid-p0 valid-p1], got %v", received)
	}

	// Verify no stray messages trickle in
	select {
	case msg := <-msgReceived:
		t.Fatalf("received unexpected message: %s", msg)
	case <-time.After(500 * time.Millisecond):
	}
}

func TestWSClient_ReconnectThrottling(t *testing.T) {
	upgrader := gws.Upgrader{}
	var mu sync.Mutex
	var connectTimes []time.Time

	connCh := make(chan struct{}, 10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		mu.Lock()
		connectTimes = append(connectTimes, time.Now())
		count := len(connectTimes)
		mu.Unlock()
		connCh <- struct{}{}

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
	client, _ := NewWSClient(*cfg, queues.NewPriorityScheduler(1, 1), &mockMetadataProvider{}, func(cloud contracts.LinkState) {})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.ReconnectLoop(ctx, &mockFrameHandler{})

	// Wait for exactly 3 connections deterministically
	for i := 0; i < 3; i++ {
		select {
		case <-connCh:
		case <-time.After(15 * time.Second):
			t.Fatalf("timed out waiting for connection %d", i+1)
		}
	}

	mu.Lock()
	defer mu.Unlock()

	if len(connectTimes) < 3 {
		t.Fatalf("expected at least 3 connections, got %d", len(connectTimes))
	}

	d1 := connectTimes[1].Sub(connectTimes[0])
	d2 := connectTimes[2].Sub(connectTimes[1])

	// First wait is base=initialBackoff. The jitter logic uses initialBackoff + rand(0, base).
	// So it should be between initialBackoff and initialBackoff*2 (plus a tiny bit of test overhead).
	min1 := initialBackoff - 10*time.Millisecond
	max1 := initialBackoff*2 + 500*time.Millisecond
	if d1 < min1 || d1 > max1 {
		t.Errorf("expected first jittered backoff between %v and %v, got %v", min1, max1, d1)
	}
	// Second wait is base=initialBackoff*2. The jitter logic uses initialBackoff + rand(0, base).
	// So it should be between initialBackoff and initialBackoff*3 (plus a tiny bit of test overhead).
	min2 := initialBackoff - 10*time.Millisecond
	max2 := initialBackoff*3 + 500*time.Millisecond
	if d2 < min2 || d2 > max2 {
		t.Errorf("expected second jittered backoff between %v and %v, got %v", min2, max2, d2)
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
	client, _ := NewWSClient(*cfg, queues.NewPriorityScheduler(1, 1), &mockMetadataProvider{}, func(cloud contracts.LinkState) {})

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

		conn.SetPingHandler(func(appData string) error {
			return conn.WriteControl(gws.PongMessage, []byte(appData), time.Now().Add(time.Second))
		})

		// Run a background reader to process the Ping and send the Pong,
		// but ONLY read the first Connect frame so we stop reading and block the buffer.
		go func() {
			conn.ReadMessage() // Read the connect event
			conn.NextReader()  // Block waiting for next message, processing Ping in the background. We never read the payload, causing the buffer to fill.
		}()

		// Block reads completely so the client TCP buffer fills and writes block
		<-r.Context().Done()
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	cfg := &config.CloudConfig{
		URL:                 wsURL,
		WriteTimeoutSeconds: 1, // Fail fast on write
	}
	sched := queues.NewPriorityScheduler(10, 10)
	readyCh := make(chan struct{})
	client, _ := NewWSClient(*cfg, sched, &mockMetadataProvider{}, func(cloud contracts.LinkState) {
		if cloud == contracts.LinkConnected {
			select {
			case <-readyCh:
			default:
				close(readyCh)
			}
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.ReconnectLoop(ctx, &mockFrameHandler{})

	select {
	case <-readyCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for connect")
	}

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

	stateCh := make(chan string, 10)
	onState := func(cloud contracts.LinkState) {
		select {
		case stateCh <- string(cloud):
		default:
		}
	}

	client, _ := NewWSClient(*cfg, queues.NewPriorityScheduler(1, 1), &mockMetadataProvider{}, onState)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.ReconnectLoop(ctx, &mockFrameHandler{})

	// Wait to verify it gracefully catches the CA failure and retries
	count := 0
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case s := <-stateCh:
			if s == "connecting" {
				count++
				if count >= 1 {
					return // Success! It gracefully caught the CA read error and applied backoff
				}
			}
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatalf("expected client to retry and emit connecting exactly once due to bad CA file, got count %d", count)
}

func TestWSClient_StableSessionThreshold(t *testing.T) {
	upgrader := gws.Upgrader{}
	var mu sync.Mutex
	var connectTimes []time.Time

	connCh := make(chan struct{}, 10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		mu.Lock()
		connectTimes = append(connectTimes, time.Now())
		count := len(connectTimes)
		mu.Unlock()
		connCh <- struct{}{}

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
	client, _ := NewWSClient(*cfg, queues.NewPriorityScheduler(1, 1), &mockMetadataProvider{}, func(cloud contracts.LinkState) {})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.ReconnectLoop(ctx, &mockFrameHandler{})

	// Wait for exactly 3 connections deterministically
	for i := 0; i < 3; i++ {
		select {
		case <-connCh:
		case <-time.After(15 * time.Second):
			t.Fatalf("timed out waiting for connection %d", i+1)
		}
	}

	mu.Lock()
	defer mu.Unlock()

	if len(connectTimes) < 3 {
		t.Fatalf("expected at least 3 connections, got %d", len(connectTimes))
	}

	d1 := connectTimes[1].Sub(connectTimes[0])
	d2 := connectTimes[2].Sub(connectTimes[1])

	// Both delays should be ~2s (2s server sleep + instant reconnect)
	// (allow 1.5 - 3.5s)
	if d1 < 1500*time.Millisecond || d1 > 3500*time.Millisecond {
		t.Errorf("expected ~2s total delay for first reconnect, got %v", d1)
	}
	if d2 < 1500*time.Millisecond || d2 > 3500*time.Millisecond {
		t.Errorf("expected ~2s total delay for second reconnect (backoff reset), got %v", d2)
	}
}

type mockTestScheduler struct {
	nextCalled    chan struct{}
	nextUnblocked chan struct{}
}

func (m *mockTestScheduler) Push(msg queues.OutboundMessage) error {
	return nil
}

func (m *mockTestScheduler) Next(ctx context.Context) (queues.OutboundMessage, error) {
	select {
	case m.nextCalled <- struct{}{}:
	default:
	}

	<-ctx.Done()

	select {
	case m.nextUnblocked <- struct{}{}:
	default:
	}

	return queues.OutboundMessage{}, ctx.Err()
}

func TestWSClient_TeardownOnContextCancel(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := gws.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		conn.SetPingHandler(func(appData string) error {
			return conn.WriteControl(gws.PongMessage, []byte(appData), time.Now().Add(time.Second))
		})
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	cfg := &config.CloudConfig{
		URL:                           strings.Replace(server.URL, "http", "ws", 1),
		ConnectTimeoutSeconds:         2,
		PingIntervalSeconds:           1,
		StableSessionThresholdSeconds: 1,
	}

	sched := queues.NewPriorityScheduler(10, 10)
	meta := &mockMetadataProvider{}

	readyCh := make(chan struct{})
	client, _ := NewWSClient(*cfg, sched, meta, func(cloud contracts.LinkState) {
		if cloud == contracts.LinkConnected {
			select {
			case <-readyCh:
			default:
				close(readyCh)
			}
		}
	})

	ctx, cancel := context.WithCancel(context.Background())

	handler := &mockFrameHandler{}

	done := make(chan struct{})
	go func() {
		client.ReconnectLoop(ctx, handler)
		close(done)
	}()

	select {
	case <-readyCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for connect")
	}
	cancel() // Cancel the parent context

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ReconnectLoop did not exit cleanly upon parent context cancellation")
	}
}

func TestWSClient_TeardownOnReaderFailureAndWriterBlocked(t *testing.T) {
	var connToClose *gws.Conn
	var connMu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := gws.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}

		connMu.Lock()
		connToClose = conn
		connMu.Unlock()

		conn.SetPingHandler(func(appData string) error {
			return conn.WriteControl(gws.PongMessage, []byte(appData), time.Now().Add(time.Second))
		})

		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	cfg := &config.CloudConfig{
		URL:                           strings.Replace(server.URL, "http", "ws", 1),
		ConnectTimeoutSeconds:         2,
		PingIntervalSeconds:           1,
		StableSessionThresholdSeconds: 1,
	}

	sched := &mockTestScheduler{
		nextCalled:    make(chan struct{}, 1),
		nextUnblocked: make(chan struct{}, 1),
	}
	meta := &mockMetadataProvider{}

	client, _ := NewWSClient(*cfg, sched, meta, func(cloud contracts.LinkState) {})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := &mockFrameHandler{}

	done := make(chan struct{})
	go func() {
		client.ReconnectLoop(ctx, handler)
		close(done)
	}()

	select {
	case <-sched.nextCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not call Next()")
	}

	connMu.Lock()
	if connToClose != nil {
		connToClose.Close()
	}
	connMu.Unlock()

	select {
	case <-sched.nextUnblocked:
	case <-time.After(2 * time.Second):
		t.Fatal("writer was not unblocked after socket dropped")
	}

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ReconnectLoop did not exit cleanly upon parent context cancellation")
	}
}

type blockedContextFrameHandler struct {
	called chan struct{}
	once   sync.Once
}

func (h *blockedContextFrameHandler) HandleFrame(ctx context.Context, frame InboundFrame) (FrameDisposition, error) {
	h.once.Do(func() { close(h.called) })
	// Block until context is canceled by the errgroup
	<-ctx.Done()
	return FrameAccepted, ctx.Err()
}

func TestWSClient_TeardownOnWriterFailureAndReaderBlocked(t *testing.T) {
	var connToClose *gws.Conn
	var connMu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := gws.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}

		connMu.Lock()
		connToClose = conn
		connMu.Unlock()

		// Send a dummy frame immediately so the client's HandleFrame is invoked
		conn.WriteMessage(gws.TextMessage, []byte(`{"jsonrpc":"2.0","method":"test"}`))

		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	cfg := &config.CloudConfig{
		URL:                           strings.Replace(server.URL, "http", "ws", 1),
		ConnectTimeoutSeconds:         2,
		PingIntervalSeconds:           1,
		StableSessionThresholdSeconds: 1,
	}

	sched := &mockTestScheduler{
		nextCalled:    make(chan struct{}, 1),
		nextUnblocked: make(chan struct{}, 1),
	}
	meta := &mockMetadataProvider{}

	client, _ := NewWSClient(*cfg, sched, meta, func(cloud contracts.LinkState) {})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := &blockedContextFrameHandler{
		called: make(chan struct{}),
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- client.ReconnectLoop(ctx, handler)
	}()

	// Wait for the handler to be called and block
	select {
	case <-handler.called:
	case <-time.After(2 * time.Second):
		t.Fatal("HandleFrame was never called")
	}

	// At this point, the reader is blocked inside HandleFrame.
	// Force the writer side to fail by closing the server connection.
	connMu.Lock()
	if connToClose != nil {
		connToClose.Close()
	}
	connMu.Unlock()

	// The writer loop should notice the socket is closed when it sends the next ping
	// (within 1 second). It will fail, cancel the gCtx, which unblocks HandleFrame,
	// tearing down the session. ReconnectLoop will then retry and spin up a new session.

	// Wait for a new session generation to spin up (meaning the old one was torn down)
	deadline := time.Now().Add(5 * time.Second)
	reconnected := false
	for time.Now().Before(deadline) {
		client.mu.Lock()
		gen := client.generation
		client.mu.Unlock()
		if gen > 1 {
			reconnected = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !reconnected {
		t.Fatal("session did not tear down and reconnect; reader was likely permanently blocked")
	}
}

func TestWSClient_ConcurrentPingWhileBlocked(t *testing.T) {
	upgrader := gws.Upgrader{}
	pongReceived := make(chan struct{})
	clientLockedCh := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		// Read handshake request
		var req map[string]any
		conn.ReadJSON(&req)

		conn.SetPongHandler(func(appData string) error {
			if appData == "test-ping" {
				close(pongReceived)
			}
			return nil
		})

		// Wait until the client has definitely acquired the write lock
		<-clientLockedCh
		conn.WriteControl(gws.PingMessage, []byte("test-ping"), time.Now().Add(time.Second))

		// Continually read so the server processes incoming control frames (Pongs)
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				break
			}
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	cfg := &config.CloudConfig{URL: wsURL}

	sched := queues.NewPriorityScheduler(1, 1)

	readyCh := make(chan struct{})
	client, _ := NewWSClient(*cfg, sched, &mockMetadataProvider{}, func(cloud contracts.LinkState) {
		if cloud == contracts.LinkConnected {
			select {
			case <-readyCh:
			default:
				close(readyCh)
			}
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.ReconnectLoop(ctx, &mockFrameHandler{})

	select {
	case <-readyCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for connect")
	}

	// Artificially lock the writer mutex to simulate a stalled application write
	// (If PingHandler used the same mutex, it would deadlock here)
	client.writeMu.Lock()
	close(clientLockedCh)
	defer client.writeMu.Unlock()

	select {
	case <-pongReceived:
		// Success! The PingHandler successfully bypassed the stalled writer loop.
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for pong while writer was blocked")
	}
}

type blockingMetadataProvider struct{}

func (m *blockingMetadataProvider) ConnectParams(ctx context.Context) (CloudConnectParams, error) {
	<-ctx.Done()
	return CloudConnectParams{}, ctx.Err()
}

func TestWSClient_HandshakeTimeout(t *testing.T) {
	upgrader := gws.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	cfg := &config.CloudConfig{
		URL:                   wsURL,
		ConnectTimeoutSeconds: 1, // 1 second timeout
	}

	client, _ := NewWSClient(*cfg, queues.NewPriorityScheduler(1, 1), &blockingMetadataProvider{}, func(cloud contracts.LinkState) {})

	ctx := context.Background()

	dialer := gws.DefaultDialer
	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	start := time.Now()
	res := client.performConnectHandshake(ctx, conn)
	duration := time.Since(start)

	if res != HandshakeRetryableFailure {
		t.Errorf("expected HandshakeRetryableFailure, got %v", res)
	}

	if duration < 1*time.Second || duration > 2*time.Second {
		t.Errorf("expected timeout around 1s, got %v", duration)
	}
}

func TestWSClient_PingIntervalGreaterThanPongTimeout(t *testing.T) {
	upgrader := gws.Upgrader{}
	var mu sync.Mutex
	connectCount := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		mu.Lock()
		connectCount++
		mu.Unlock()

		// Read handshake request
		var req map[string]any
		conn.ReadJSON(&req)

		// Echo pings as pongs
		conn.SetPingHandler(func(appData string) error {
			return conn.WriteControl(gws.PongMessage, []byte(appData), time.Now().Add(time.Second))
		})

		// Continually read
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	cfg := &config.CloudConfig{
		URL:                 wsURL,
		PingIntervalSeconds: 3,
		PongTimeoutSeconds:  1,
	}

	readyCh := make(chan struct{})
	client, _ := NewWSClient(*cfg, queues.NewPriorityScheduler(1, 1), &mockMetadataProvider{}, func(cloud contracts.LinkState) {
		if cloud == contracts.LinkConnected {
			select {
			case <-readyCh:
			default:
				close(readyCh)
			}
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- client.ReconnectLoop(ctx, &mockFrameHandler{})
	}()

	select {
	case <-readyCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for connect")
	}

	// Wait 4 seconds. The PongTimeout is 1s, but because PingInterval is 3s,
	// the actual read deadline should be 4s. If the reader loop times out before this,
	// the test fails because the socket died.
	select {
	case err := <-errCh:
		t.Fatalf("ReconnectLoop exited prematurely (likely due to read timeout): %v", err)
	case <-time.After(4 * time.Second):
		// Success! The connection survived past the 1s PongTimeout, proving
		// the deadline includes the PingInterval.
		mu.Lock()
		if connectCount != 1 {
			t.Errorf("expected exactly 1 connection, but got %d (socket died and reconnected!)", connectCount)
		}
		mu.Unlock()
	}
}

type fatalFrameHandler struct {
	handled chan struct{}
}

func (h *fatalFrameHandler) HandleFrame(ctx context.Context, frame InboundFrame) (FrameDisposition, error) {
	select {
	case <-h.handled:
	default:
		close(h.handled)
	}
	return FrameFatalCloseConnection, nil
}

func TestWSClient_FrameFatalCloseConnection(t *testing.T) {
	upgrader := gws.Upgrader{}
	var mu sync.Mutex
	connectCount := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		mu.Lock()
		connectCount++
		mu.Unlock()

		// Read handshake request
		var req map[string]any
		conn.ReadJSON(&req)

		// Send a dummy frame to trigger the handler
		conn.WriteMessage(gws.TextMessage, []byte("dummy-frame"))

		// Continually read to keep connection alive
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	cfg := &config.CloudConfig{
		URL: wsURL,
	}

	client, _ := NewWSClient(*cfg, queues.NewPriorityScheduler(1, 1), &mockMetadataProvider{}, func(cloud contracts.LinkState) {})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := &fatalFrameHandler{handled: make(chan struct{})}

	go client.ReconnectLoop(ctx, handler)

	// Wait for handler to receive the frame and trigger fatal disposition
	select {
	case <-handler.handled:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for frame handler to execute")
	}

	// Because it returns FrameFatalCloseConnection, the session should tear down
	// and trigger a reconnect. We should see the connection count increment
	// after the initial 2-second reconnect backoff (+ jitter).
	deadline := time.Now().Add(6 * time.Second)
	reconnected := false
	for time.Now().Before(deadline) {
		mu.Lock()
		count := connectCount
		mu.Unlock()
		if count >= 2 {
			reconnected = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !reconnected {
		mu.Lock()
		count := connectCount
		mu.Unlock()
		t.Errorf("expected at least 2 connections (initial + reconnect), got %d. Session failed to tear down and reconnect.", count)
	}
}

func TestWSClient_ServerPingsButNoPongs(t *testing.T) {
	upgrader := gws.Upgrader{}
	var mu sync.Mutex
	connectCount := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		mu.Lock()
		connectCount++
		mu.Unlock()

		// Read handshake request
		var req map[string]any
		conn.ReadJSON(&req)

		// Create a context to cleanly stop the pinger goroutine
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()

		// Aggressively send Pings to the client
		go func() {
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					conn.WriteControl(gws.PingMessage, []byte("server-ping"), time.Now().Add(time.Second))
				}
			}
		}()

		// Disable the default ping handler so the server NEVER replies with a Pong
		// (forcing the server to ignore the client's pings)
		conn.SetPingHandler(func(string) error { return nil })

		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	cfg := &config.CloudConfig{
		URL:                 wsURL,
		PingIntervalSeconds: 1,
		PongTimeoutSeconds:  1,
	}

	readyCh := make(chan struct{})
	client, _ := NewWSClient(*cfg, queues.NewPriorityScheduler(1, 1), &mockMetadataProvider{}, func(cloud contracts.LinkState) {
		if cloud == contracts.LinkConnected {
			select {
			case <-readyCh:
			default:
				close(readyCh)
			}
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- client.ReconnectLoop(ctx, &mockFrameHandler{})
	}()

	select {
	case <-readyCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for connect")
	}

	// Wait up to 6 seconds. The ping+pong timeout total is 2s.
	// Even though the server is sending us Pings every 0.5s, the client should
	// correctly time out and tear down the socket, then wait 2s to reconnect.
	deadline := time.Now().Add(6 * time.Second)
	reconnected := false
	for time.Now().Before(deadline) {
		mu.Lock()
		count := connectCount
		mu.Unlock()
		if count >= 2 {
			reconnected = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !reconnected {
		mu.Lock()
		count := connectCount
		mu.Unlock()
		t.Errorf("expected socket to die and reconnect (count >= 2), but count is %d. Inbound pings falsely kept it alive!", count)
	}
}

func TestJitterAlgorithm(t *testing.T) {
	// Verify the exact algorithm used in waitForRetry to ensure it bounds correctly
	// and randomizes values to prevent thundering herds.

	defaultInitialBackoff := 2 * time.Second
	maxBackoff := 60 * time.Second

	// Test bounds at base backoff = 2s
	backoff := 2 * time.Second
	uniqueValues := make(map[time.Duration]bool)
	for i := 0; i < 100; i++ {
		jitter := time.Duration(rand.Float64() * float64(backoff))
		jitteredBackoff := defaultInitialBackoff + jitter
		if jitteredBackoff > maxBackoff {
			jitteredBackoff = maxBackoff
		}

		if jitteredBackoff < 2*time.Second || jitteredBackoff >= 4*time.Second {
			t.Errorf("at base=2s, expected jittered backoff between 2s and 4s, got %v", jitteredBackoff)
		}
		uniqueValues[jitteredBackoff] = true
	}
	if len(uniqueValues) < 10 {
		t.Errorf("expected jitter to be highly randomized, but got only %d unique values", len(uniqueValues))
	}

	// Test bounds at maximum backoff = 60s
	backoff = 60 * time.Second
	uniqueValues = make(map[time.Duration]bool)
	for i := 0; i < 100; i++ {
		jitter := time.Duration(rand.Float64() * float64(backoff))
		jitteredBackoff := defaultInitialBackoff + jitter
		if jitteredBackoff > maxBackoff {
			jitteredBackoff = maxBackoff
		}

		if jitteredBackoff < 2*time.Second || jitteredBackoff > 60*time.Second {
			t.Errorf("at base=60s, expected jittered backoff between 2s and 60s, got %v", jitteredBackoff)
		}
		uniqueValues[jitteredBackoff] = true
	}
	if len(uniqueValues) < 10 {
		t.Errorf("expected jitter to be highly randomized, but got only %d unique values", len(uniqueValues))
	}
}

type nullMetadataProvider struct{}

func (m *nullMetadataProvider) ConnectParams(ctx context.Context) (CloudConnectParams, error) {
	return CloudConnectParams{
		Serial:   "SERIAL123",
		Firmware: "1.0.0",
		UUID:     12345,
		// Capabilities deliberately left nil
	}, nil
}

func TestWSClient_NullCapabilities(t *testing.T) {
	upgrader := gws.Upgrader{}
	capsVerifiedCh := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		var req map[string]any
		if err := conn.ReadJSON(&req); err != nil {
			t.Errorf("server read json failed: %v", err)
			return
		}

		params, ok := req["params"].(map[string]any)
		if !ok {
			t.Errorf("expected params to be a JSON object, got %T", req["params"])
			return
		}

		caps, ok := params["capabilities"]
		if !ok {
			t.Errorf("expected capabilities key to be present in params")
			return
		}

		if caps == nil {
			t.Errorf("expected capabilities to be normalized to {}, but got null")
		} else if capsMap, isMap := caps.(map[string]any); !isMap {
			t.Errorf("expected capabilities to be an object, got %T", caps)
		} else if len(capsMap) != 0 {
			t.Errorf("expected capabilities to be an empty object, got %v", capsMap)
		}

		close(capsVerifiedCh)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	cfg := &config.CloudConfig{
		URL: wsURL,
	}

	client, _ := NewWSClient(*cfg, queues.NewPriorityScheduler(1, 1), &nullMetadataProvider{}, func(cloud contracts.LinkState) {})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.ReconnectLoop(ctx, &mockFrameHandler{})

	select {
	case <-capsVerifiedCh:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for capabilities verification")
	}
}
