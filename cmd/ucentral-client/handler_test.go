package main

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/contracts"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/nats"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/queues"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/reqmgr"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/websocket"
)

// mockStore implements reqmgr.OperationStore for testing
type mockStore struct{}

func (s *mockStore) Save(ctx context.Context, op *reqmgr.PersistentOperation) error { return nil }
func (s *mockStore) Get(ctx context.Context, opID string) (*reqmgr.PersistentOperation, error) {
	return nil, nil
}
func (s *mockStore) GetActive(ctx context.Context, limit int) ([]*reqmgr.PersistentOperation, error) {
	return nil, nil
}
func (s *mockStore) Delete(ctx context.Context, opID string) error { return nil }

func setupTestHandler(t *testing.T, dispatchBufCap int) (*frameHandler, *reqmgr.DefaultRequestManager, *queues.PriorityScheduler, *systemStateManager) {
	cache := reqmgr.NewTransactionCache()
	store := &mockStore{}
	scheduler := queues.NewPriorityScheduler(100, 100)

	// Create RequestManager with 5 max concurrent requests
	reqMgr, err := reqmgr.NewRequestManager(
		2*time.Second,
		reqmgr.CacheTTLConfig{},
		cache,
		scheduler,
		store,
		5,
		5*time.Minute,
		10,
	)
	if err != nil {
		t.Fatalf("failed to create request manager: %v", err)
	}

	stateMgr := &systemStateManager{
		natsLink: contracts.LinkConnected,
		wsLink:   contracts.LinkConnected,
	}

	dispatchBuffer := make(chan struct{}, dispatchBufCap)

	h := &frameHandler{
		reqMgr:                 reqMgr,
		stateMgr:               stateMgr,
		scheduler:              scheduler,
		serial:                 "001122334455",
		dispatchBuffer:         dispatchBuffer,
		timeoutDispatch:        5 * time.Second,
		timeoutConfigure:       120 * time.Second,
		timeoutActionDefault:   30 * time.Second,
		timeoutActionExtended:  90 * time.Second,
		payloadLimitAbsolute:   12 * 1024 * 1024,
		payloadLimitConfigure:  10 * 1024 * 1024,
		payloadLimitCertUpdate: 2 * 1024 * 1024,
		payloadLimitScript:     1 * 1024 * 1024,
		payloadLimitDefault:    11 * 1024 * 1024,
		target:                 "vyos",
	}

	return h, reqMgr, scheduler, stateMgr
}

func TestGetTransactionTimeoutAllowsTraceToFinish(t *testing.T) {
	h, _, _, _ := setupTestHandler(t, 10)
	tests := []struct {
		name   string
		method string
		params json.RawMessage
		want   time.Duration
	}{
		{name: "default action", method: "ping", want: 30 * time.Second},
		{name: "configure", method: "configure", want: 120 * time.Second},
		{name: "thirty second trace", method: "trace", params: json.RawMessage(`{"duration":30}`), want: 60 * time.Second},
		{name: "maximum trace", method: "trace", params: json.RawMessage(`{"duration":300}`), want: 330 * time.Second},
		{name: "packet limited trace uses capture default", method: "trace", params: json.RawMessage(`{"packets":100}`), want: 90 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := h.getTransactionTimeout(tt.method, tt.params); got != tt.want {
				t.Fatalf("timeout got=%s want=%s", got, tt.want)
			}
		})
	}
}

func TestFrameHandler_ParseAndValidationErrors(t *testing.T) {
	h, _, scheduler, _ := setupTestHandler(t, 10)

	// 1. Invalid JSON parse error
	frame := websocket.InboundFrame{
		SessionID: "sess-1",
		Type:      1,
		Payload:   []byte(`{invalid-json`),
	}
	disp, err := h.HandleFrame(context.Background(), frame)
	if err != nil {
		t.Fatalf("unexpected handle error: %v", err)
	}
	if disp != websocket.FrameRejectedKeepConnection {
		t.Errorf("expected FrameRejectedKeepConnection, got %v", disp)
	}

	// 2. Malformed JSON-RPC structure (missing version)
	frame = websocket.InboundFrame{
		SessionID: "sess-1",
		Type:      1,
		Payload:   []byte(`{"method":"ping","id":1}`),
	}
	disp, err = h.HandleFrame(context.Background(), frame)
	if err != nil {
		t.Fatalf("unexpected handle error: %v", err)
	}
	if disp != websocket.FrameRejectedKeepConnection {
		t.Errorf("expected FrameRejectedKeepConnection, got %v", disp)
	}

	// 3. Size limit exceeded (REQ-020)
	complexScript := strings.Repeat("echo 'Applying system update...'; sleep 1; systemctl restart network; ping -c 3 8.8.8.8; # ", 30000)
	frame = websocket.InboundFrame{
		SessionID: "sess-1",
		Type:      1,
		Payload:   []byte(`{"jsonrpc":"2.0","method":"script","id":1,"params":{"script":"` + complexScript + `"}}`),
	}
	disp, err = h.HandleFrame(context.Background(), frame)
	if err != nil {
		t.Fatalf("unexpected handle error: %v", err)
	}
	if disp != websocket.FrameRejectedKeepConnection {
		t.Errorf("expected FrameRejectedKeepConnection, got %v", disp)
	}

	// Verify first queued message (invalid JSON parse error)
	msg1, err := scheduler.Next(context.Background())
	if err != nil {
		t.Fatalf("failed to pop from scheduler: %v", err)
	}
	var resp1 contracts.JSONRPCResponse
	if err := json.Unmarshal(msg1.Payload, &resp1); err != nil {
		t.Fatalf("failed to unmarshal JSON-RPC response: %v", err)
	}
	if resp1.Error == nil || resp1.Error.Code != contracts.ErrParse {
		t.Errorf("expected ErrParse, got: %+v", resp1.Error)
	}
	if string(resp1.ID) != "null" {
		t.Errorf("expected ID null for parse error, got: %s", string(resp1.ID))
	}

	// Verify second queued message (invalid request due to malformed JSON-RPC missing version)
	msg2, err := scheduler.Next(context.Background())
	if err != nil {
		t.Fatalf("failed to pop from scheduler: %v", err)
	}
	var resp2 contracts.JSONRPCResponse
	if err := json.Unmarshal(msg2.Payload, &resp2); err != nil {
		t.Fatalf("failed to unmarshal JSON-RPC response: %v", err)
	}
	if resp2.Error == nil || resp2.Error.Code != contracts.ErrInvalidRequest {
		t.Errorf("expected ErrInvalidRequest, got: %+v", resp2.Error)
	}

	// Verify third queued message (size limit exceeded)
	msg3, err := scheduler.Next(context.Background())
	if err != nil {
		t.Fatalf("failed to pop from scheduler: %v", err)
	}
	var resp3 contracts.JSONRPCResponse
	if err := json.Unmarshal(msg3.Payload, &resp3); err != nil {
		t.Fatalf("failed to unmarshal JSON-RPC response: %v", err)
	}
	if resp3.Error == nil || resp3.Error.Code != contracts.ErrInvalidParams {
		t.Errorf("expected ErrInvalidParams, got: %+v", resp3.Error)
	}
}

func TestFrameHandler_NATSDegradedRejection(t *testing.T) {
	h, _, scheduler, stateMgr := setupTestHandler(t, 10)

	// Set NATS link state to Connecting (makes overall system state NATSDegraded)
	stateMgr.UpdateNATSLink(contracts.LinkConnecting)

	frame := websocket.InboundFrame{
		SessionID: "sess-1",
		Type:      1,
		Payload:   []byte(`{"jsonrpc":"2.0","method":"reboot","id":42,"params":{"serial":"001122334455"}}`),
	}
	disp, err := h.HandleFrame(context.Background(), frame)
	if err != nil {
		t.Fatalf("unexpected handle error: %v", err)
	}
	if disp != websocket.FrameRejectedKeepConnection {
		t.Errorf("expected FrameRejectedKeepConnection, got %v", disp)
	}

	// Verify that Service Unavailable (-32603, subcode 3) response was pushed
	msg, err := scheduler.Next(context.Background())
	if err != nil {
		t.Fatalf("failed to pop from scheduler: %v", err)
	}
	var resp contracts.JSONRPCResponse
	if err := json.Unmarshal(msg.Payload, &resp); err != nil {
		t.Fatalf("failed to unmarshal JSON-RPC response: %v", err)
	}
	if resp.Error == nil || resp.Error.Code != contracts.ErrInternal {
		t.Fatalf("expected code ErrInternal (-32603), got %d", resp.Error.Code)
	}
	var data map[string]interface{}
	if err := json.Unmarshal(resp.Error.Data, &data); err != nil {
		t.Fatalf("failed to unmarshal error data: %v", err)
	}
	if data["application_code"].(float64) != float64(contracts.ErrServiceUnavailable) {
		t.Errorf("expected application code %d (ServiceUnavailable), got %v", contracts.ErrServiceUnavailable, data["application_code"])
	}
}

func TestFrameHandler_DispatchBufferOverflow(t *testing.T) {
	// Initialize with capacity 0 to trigger immediate overflow
	h, _, scheduler, _ := setupTestHandler(t, 0)

	frame := websocket.InboundFrame{
		SessionID: "sess-1",
		Type:      1,
		Payload:   []byte(`{"jsonrpc":"2.0","method":"reboot","id":100,"params":{"serial":"001122334455"}}`),
	}

	disp, err := h.HandleFrame(context.Background(), frame)
	if err != nil {
		t.Fatalf("unexpected handle error: %v", err)
	}
	if disp != websocket.FrameAccepted {
		t.Errorf("expected FrameAccepted, got %v", disp)
	}

	// Verify overflow error response (local_service_unavailable)
	msg, err := scheduler.Next(context.Background())
	if err != nil {
		t.Fatalf("failed to pop from scheduler: %v", err)
	}
	var resp contracts.JSONRPCResponse
	if err := json.Unmarshal(msg.Payload, &resp); err != nil {
		t.Fatalf("failed to unmarshal JSON-RPC response: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected error object, got nil")
	}
	var data map[string]interface{}
	_ = json.Unmarshal(resp.Error.Data, &data)
	if data["application_code"].(float64) != float64(contracts.ErrServiceUnavailable) {
		t.Errorf("expected application code %d, got %v", contracts.ErrServiceUnavailable, data["application_code"])
	}
}

func TestFrameHandler_DuplicateReplay(t *testing.T) {
	h, reqMgr, scheduler, _ := setupTestHandler(t, 10)

	cloudRPCID := json.RawMessage(`"tx-dup"`)
	tx, err := reqMgr.CreateTransaction("sess-1", cloudRPCID, true, "reboot", 10*time.Second, true)
	if err != nil {
		t.Fatalf("failed to create transaction: %v", err)
	}

	_ = reqMgr.MarkPreparingDispatch(tx.RPCID)
	_ = reqMgr.MarkPendingPublish(tx.RPCID)
	_ = reqMgr.MarkInFlight(tx.RPCID)

	// Complete transaction with a simulated final response payload
	finalResponse := []byte(`{"jsonrpc":"2.0","result":{"status":0},"id":"tx-dup"}`)
	err = reqMgr.Complete(tx.RPCID, finalResponse)
	if err != nil {
		t.Fatalf("failed to complete transaction: %v", err)
	}

	// Submit duplicate request with identical session and Cloud RPC ID
	frame := websocket.InboundFrame{
		SessionID: "sess-1",
		Type:      1,
		Payload:   []byte(`{"jsonrpc":"2.0","method":"reboot","id":"tx-dup","params":{"serial":"001122334455"}}`),
	}

	disp, err := h.HandleFrame(context.Background(), frame)
	if err != nil {
		t.Fatalf("unexpected handle error: %v", err)
	}
	if disp != websocket.FrameAccepted {
		t.Errorf("expected FrameAccepted, got %v", disp)
	}

	// Verify that the exact finalResponse was replayed from cache
	msg, err := scheduler.Next(context.Background())
	if err != nil {
		t.Fatalf("failed to pop from scheduler: %v", err)
	}
	if string(msg.Payload) != string(finalResponse) {
		t.Errorf("expected replayed payload %s, got %s", string(finalResponse), string(msg.Payload))
	}
}

func assertNoQueuedResponse(t *testing.T, scheduler *queues.PriorityScheduler, stage string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	if msg, err := scheduler.Next(ctx); err == nil {
		t.Errorf("unexpected queued response during %s: %s", stage, string(msg.Payload))
	}
}

func TestFrameHandler_Notifications(t *testing.T) {
	h, _, scheduler, _ := setupTestHandler(t, 10)

	// 1. Valid read-only notification (ping has isStateChanging = false)
	frame := websocket.InboundFrame{
		SessionID: "sess-1",
		Type:      1,
		Payload:   []byte(`{"jsonrpc":"2.0","method":"ping","params":{"serial":"001122334455"}}`),
	}
	disp, err := h.HandleFrame(context.Background(), frame)
	if err != nil {
		t.Fatalf("unexpected handle error for read-only notification: %v", err)
	}
	if disp != websocket.FrameAccepted {
		t.Errorf("expected FrameAccepted for read-only notification, got %v", disp)
	}

	// Verify no response is queued in the scheduler
	assertNoQueuedResponse(t, scheduler, "read-only notification")

	// 2. State-changing notification (reboot has isStateChanging = true)
	frame = websocket.InboundFrame{
		SessionID: "sess-1",
		Type:      1,
		Payload:   []byte(`{"jsonrpc":"2.0","method":"reboot","params":{"serial":"001122334455"}}`),
	}
	disp, err = h.HandleFrame(context.Background(), frame)
	if err != nil {
		t.Fatalf("unexpected handle error: %v", err)
	}
	if disp != websocket.FrameRejectedKeepConnection {
		t.Errorf("expected FrameRejectedKeepConnection for state-changing notification, got %v", disp)
	}
	assertNoQueuedResponse(t, scheduler, "state-changing notification")

	// 3. Method not found notification (method does not exist)
	frame = websocket.InboundFrame{
		SessionID: "sess-1",
		Type:      1,
		Payload:   []byte(`{"jsonrpc":"2.0","method":"nonexistent"}`),
	}
	disp, err = h.HandleFrame(context.Background(), frame)
	if err != nil {
		t.Fatalf("unexpected handle error: %v", err)
	}
	if disp != websocket.FrameRejectedKeepConnection {
		t.Errorf("expected FrameRejectedKeepConnection for method not found notification, got %v", disp)
	}
	assertNoQueuedResponse(t, scheduler, "method not found notification")

	// 4. Payload size limit exceeded notification
	complexScript := strings.Repeat("echo 'Applying system update...'; sleep 1; systemctl restart network; ping -c 3 8.8.8.8; # ", 30000)
	frame = websocket.InboundFrame{
		SessionID: "sess-1",
		Type:      1,
		Payload:   []byte(`{"jsonrpc":"2.0","method":"script","params":{"script":"` + complexScript + `"}}`),
	}
	disp, err = h.HandleFrame(context.Background(), frame)
	if err != nil {
		t.Fatalf("unexpected handle error: %v", err)
	}
	if disp != websocket.FrameRejectedKeepConnection {
		t.Errorf("expected FrameRejectedKeepConnection for size-exceeded notification, got %v", disp)
	}
	assertNoQueuedResponse(t, scheduler, "size-exceeded notification")

	// 5. Remote access notification (remote_access is security-sensitive, must be rejected)
	frame = websocket.InboundFrame{
		SessionID: "sess-1",
		Type:      1,
		Payload:   []byte(`{"jsonrpc":"2.0","method":"rtty","params":{"serial":"001122334455","method":"rtty","id":"123","server":"srv","port":123,"token":"tkn"}}`),
	}
	disp, err = h.HandleFrame(context.Background(), frame)
	if err != nil {
		t.Fatalf("unexpected handle error for remote_access notification: %v", err)
	}
	if disp != websocket.FrameRejectedKeepConnection {
		t.Errorf("expected FrameRejectedKeepConnection for remote_access notification, got %v", disp)
	}
	assertNoQueuedResponse(t, scheduler, "remote_access notification")
}

func TestFrameHandler_CompressedConfigureUUID(t *testing.T) {
	// Initialize frame handler with a test buffer capacity of 10
	h, _, scheduler, _ := setupTestHandler(t, 10)


	// Initialize natsClient (without agentClient) so it returns "agentClient is not initialized"
	// if envelope UUID validation passes.
	h.SetNATSClient(&nats.NATSClient{})

	// Generate valid compressed inner JSON configuration
	var buf bytes.Buffer
	zw := zlib.NewWriter(&buf)
	innerJSON := `{"serial":"001122334455","uuid":1724773800,"config":{"uuid":1724773800}}`
	_, _ = zw.Write([]byte(innerJSON))
	_ = zw.Close()
	compress64 := base64.StdEncoding.EncodeToString(buf.Bytes())

	// JSON-RPC Configure frame containing compressed payload
	payloadJSON := fmt.Sprintf(`{
		"jsonrpc": "2.0",
		"method": "configure",
		"id": 42,
		"params": {
			"compress_64": "%s",
			"compress_sz": %d
		}
	}`, compress64, len(innerJSON))

	frame := websocket.InboundFrame{
		SessionID: "sess-configure-compressed",
		Type:      1,
		Payload:   []byte(payloadJSON),
	}

	disp, err := h.HandleFrame(context.Background(), frame)
	if err != nil {
		t.Fatalf("HandleFrame failed: %v", err)
	}
	if disp != websocket.FrameAccepted {
		t.Fatalf("Expected FrameAccepted, got %v", disp)
	}

	// Read transaction response from the priority scheduler
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	outboundMsg, err := scheduler.Next(ctx)
	if err != nil {
		t.Fatalf("failed to pop transaction response from scheduler: %v", err)
	}

	var jsonRPCResponse struct {
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(outboundMsg.Payload, &jsonRPCResponse); err != nil {
		t.Fatalf("failed to unmarshal JSON-RPC response: %v", err)
	}

	if jsonRPCResponse.Error == nil {
		t.Fatal("expected JSON-RPC error response due to uninitialized agentClient, got success")
	}

	// If the UUID was incorrectly parsed as "0", NATS client ValidateCommandPayload will fail with:
	// "envelope UUID \"0\" does not match payload UUID 1724773800".
	// If the UUID is correctly parsed, it should bypass envelope verification and fail with:
	// "agentClient is not initialized".
	expectedError := "agentClient is not initialized"
	if !strings.Contains(jsonRPCResponse.Error.Message, expectedError) {
		t.Errorf("Expected error containing %q, got: %q", expectedError, jsonRPCResponse.Error.Message)
	}
}
