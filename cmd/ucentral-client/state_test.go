package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Telecominfraproject/olg-nats-agent-core/agentcore"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/contracts"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/queues"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/websocket"
)

func newStateTestFrameHandler(t *testing.T, fn func(context.Context, string) (agentcore.StateEnvelope, error)) (*frameHandler, *queues.PriorityScheduler) {
	t.Helper()
	scheduler := queues.NewPriorityScheduler(16, 16)
	stateMgr := &systemStateManager{
		natsLink: contracts.LinkConnected,
		wsLink:   contracts.LinkConnected,
	}
	h := &frameHandler{
		stateMgr:        stateMgr,
		scheduler:       scheduler,
		serial:          "60cf84f22290",
		dispatchBuffer:  make(chan struct{}, 10),
		timeoutDispatch:      2 * time.Second,
		payloadLimitAbsolute: 12 * 1024 * 1024,
		payloadLimitDefault:  1024 * 1024,
		target:               "vyos",
		requestStateFn:  fn,
	}
	return h, scheduler
}

func dualStackStatePayload() json.RawMessage {
	return json.RawMessage(`{
		"serial": "stale-inner",
		"uuid": 999,
		"version": 1,
		"unit": {"uptime": 45123},
		"interfaces": [
			{
				"name": "eth0",
				"ipv4": {
					"addresses": ["192.168.1.1/24"],
					"dhcp_server": "192.168.1.254",
					"leasetime": 43200
				},
				"ipv6_addresses": [
					{
						"address": "2001:db8::1/64",
						"valid": 3600
					}
				],
				"ipv6_leases": [
					{
						"addresses": ["2001:db8::100"],
						"hostname": "client-1"
					}
				],
				"ntp_server": "pool.ntp.org"
			}
		],
		"radios": []
	}`)
}

func TestComponent_OnDemandState_Positive_Ordering(t *testing.T) {
	h, scheduler := newStateTestFrameHandler(t, func(_ context.Context, target string) (agentcore.StateEnvelope, error) {
		return agentcore.StateEnvelope{
			Version:   "1.0",
			Target:    target,
			UUID:      "1793880893",
			Payload:   dualStackStatePayload(),
			Timestamp: time.Now().UTC(),
		}, nil
	})

	reqJSON := []byte(`{"jsonrpc":"2.0","id":101,"method":"request","params":{"message":"state","request_uuid":"req-101"}}`)
	disp, err := h.HandleFrame(context.Background(), websocket.InboundFrame{
		SessionID: "sess-1",
		Payload:   reqJSON,
	})
	if err != nil {
		t.Fatalf("HandleFrame returned unexpected error: %v", err)
	}
	if disp != websocket.FrameAccepted {
		t.Fatalf("expected FrameAccepted, got %v", disp)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Frame 1 MUST be the JSON-RPC acknowledgment
	frame1, err := scheduler.Next(ctx)
	if err != nil {
		t.Fatalf("failed to receive Frame 1: %v", err)
	}
	if frame1.Priority != queues.PriorityHighest {
		t.Fatalf("expected PriorityHighest for Frame 1, got %d", frame1.Priority)
	}
	if frame1.SessionID != "sess-1" {
		t.Fatalf("expected SessionID sess-1, got %q", frame1.SessionID)
	}
	var resp1 map[string]any
	if err := json.Unmarshal(frame1.Payload, &resp1); err != nil {
		t.Fatalf("failed to unmarshal Frame 1: %v", err)
	}
	if int(resp1["id"].(float64)) != 101 {
		t.Fatalf("expected Frame 1 id=101, got %v", resp1["id"])
	}
	status1 := resp1["result"].(map[string]any)["status"].(map[string]any)
	if int(status1["error"].(float64)) != 0 || status1["text"] != "Success" {
		t.Fatalf("unexpected Frame 1 status: %+v", status1)
	}

	// Frame 2 MUST be the asynchronous state notification
	frame2, err := scheduler.Next(ctx)
	if err != nil {
		t.Fatalf("failed to receive Frame 2: %v", err)
	}
	if frame2.Priority != queues.PriorityHighest {
		t.Fatalf("expected PriorityHighest for Frame 2, got %d", frame2.Priority)
	}
	var notif map[string]any
	if err := json.Unmarshal(frame2.Payload, &notif); err != nil {
		t.Fatalf("failed to unmarshal Frame 2: %v", err)
	}
	if notif["method"] != "state" {
		t.Fatalf("expected method state, got %v", notif["method"])
	}
	params := notif["params"].(map[string]any)
	if params["serial"] != "60cf84f22290" || uint64(params["uuid"].(float64)) != 1793880893 {
		t.Fatalf("unexpected outer identity: %+v", params)
	}
	if params["request_uuid"] != "req-101" {
		t.Fatalf("expected request_uuid req-101, got %v", params["request_uuid"])
	}
	stateObj := params["state"].(map[string]any)
	if _, hasInnerSerial := stateObj["serial"]; hasInnerSerial {
		t.Fatalf("inner serial should be stripped")
	}
	if _, hasInnerUUID := stateObj["uuid"]; hasInnerUUID {
		t.Fatalf("inner uuid should be stripped")
	}
	ifaces := stateObj["interfaces"].([]any)
	iface0 := ifaces[0].(map[string]any)
	ipv6 := iface0["ipv6_addresses"].([]any)[0].(map[string]any)
	if ipv6["address"] != "2001:db8::1/64" || uint64(ipv6["valid"].(float64)) != 3600 {
		t.Fatalf("IPv6 address object not preserved: %+v", ipv6)
	}
	leases := iface0["ipv6_leases"].([]any)[0].(map[string]any)
	if leases["hostname"] != "client-1" {
		t.Fatalf("IPv6 lease not preserved: %+v", leases)
	}
}

func TestComponent_OnDemandState_Errors(t *testing.T) {
	cases := []struct {
		name         string
		rpcID        int
		reqErr       error
		wantErrCode  int
		wantErrText  string
	}{
		{
			name:        "Timeout",
			rpcID:       102,
			reqErr:      context.DeadlineExceeded,
			wantErrCode: 1,
			wantErrText: "downstream state collection timed out or unavailable",
		},
		{
			name:        "ServiceBusy",
			rpcID:       103,
			reqErr:      &agentcore.StateGetError{Code: agentcore.CodeServiceBusy, Message: "busy"},
			wantErrCode: 2,
			wantErrText: "service busy: maximum concurrent state requests reached",
		},
		{
			name:        "MalformedReply",
			rpcID:       104,
			reqErr:      agentcore.ErrMalformedStateGetReply,
			wantErrCode: 1,
			wantErrText: "downstream state collection timed out or unavailable",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, scheduler := newStateTestFrameHandler(t, func(_ context.Context, _ string) (agentcore.StateEnvelope, error) {
				return agentcore.StateEnvelope{}, tc.reqErr
			})

			reqJSON, _ := json.Marshal(map[string]any{
				"jsonrpc": "2.0",
				"id":      tc.rpcID,
				"method":  "request",
				"params": map[string]any{
					"message":      "state",
					"request_uuid": "req-err",
				},
			})

			disp, err := h.HandleFrame(context.Background(), websocket.InboundFrame{
				SessionID: "sess-1",
				Payload:   reqJSON,
			})
			if err != nil || disp != websocket.FrameAccepted {
				t.Fatalf("unexpected HandleFrame result: disp=%v err=%v", disp, err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			frame1, err := scheduler.Next(ctx)
			if err != nil {
				t.Fatalf("expected error response frame, got %v", err)
			}
			var resp map[string]any
			if err := json.Unmarshal(frame1.Payload, &resp); err != nil {
				t.Fatalf("unmarshal failed: %v", err)
			}
			if int(resp["id"].(float64)) != tc.rpcID {
				t.Fatalf("expected id=%d, got %v", tc.rpcID, resp["id"])
			}
			status := resp["result"].(map[string]any)["status"].(map[string]any)
			if int(status["error"].(float64)) != tc.wantErrCode || status["text"] != tc.wantErrText {
				t.Fatalf("unexpected status: %+v", status)
			}

			// Verify zero subsequent state notifications emitted
			shortCtx, shortCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer shortCancel()
			if extra, err := scheduler.Next(shortCtx); err == nil {
				t.Fatalf("expected no second frame on failure, got: %s", string(extra.Payload))
			}
		})
	}
}

func TestPeriodicState_HandleNATSState_Lossless_And_PriorityMedium(t *testing.T) {
	scheduler := queues.NewPriorityScheduler(4, 4)
	coalescer := queues.NewStateCoalescer()
	components := &AppComponents{
		Scheduler:      scheduler,
		StateCoalescer: coalescer,
	}

	env := agentcore.StateEnvelope{
		Version:   "1.0",
		Target:    "vyos",
		UUID:      "1793880893",
		Payload:   dualStackStatePayload(),
		Timestamp: time.Now().UTC(),
	}

	handleNATSState(env, components, "60cf84f22290")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	msg, err := scheduler.Next(ctx)
	if err != nil {
		t.Fatalf("expected queued periodic state message, got %v", err)
	}
	if msg.Priority != queues.PriorityMedium {
		t.Fatalf("expected PriorityMedium (2), got %d", msg.Priority)
	}

	var notif map[string]any
	if err := json.Unmarshal(msg.Payload, &notif); err != nil {
		t.Fatalf("unmarshal periodic state failed: %v", err)
	}
	if notif["method"] != "state" {
		t.Fatalf("expected method state, got %v", notif["method"])
	}
	params := notif["params"].(map[string]any)
	if params["serial"] != "60cf84f22290" || uint64(params["uuid"].(float64)) != 1793880893 {
		t.Fatalf("unexpected periodic outer identity: %+v", params)
	}
	if _, hasReqUUID := params["request_uuid"]; hasReqUUID {
		t.Fatalf("periodic state should not include request_uuid")
	}

	// Verify coalescer was committed
	if _, ok := coalescer.Peek(); ok {
		t.Fatalf("expected coalescer to be committed after successful push")
	}
}
