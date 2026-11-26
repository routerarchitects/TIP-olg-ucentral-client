package tests

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Telecominfraproject/olg-nats-agent-core/agentcore"
)

func newDownstreamAgentClient(t *testing.T, natsURL string) *agentcore.Client {
	t.Helper()
	cfg := agentcore.Config{
		AgentName: "vyos",
		Version:   "1.0",
		NATS: agentcore.NATSConfig{
			Servers: []string{natsURL},
		},
		Subjects: agentcore.SubjectConfig{
			ConfigurePattern: "cmd.configure.%s",
			ActionPattern:    "cmd.action.%s.%s",
			ResultPattern:    "result.%s",
			StatusPattern:    "status.%s",
			HealthPattern:    "health.%s",
			StatePattern:     "state.%s",
			StateGetPattern:  "state.get.%s",
		},
		KV: agentcore.KVConfig{
			Bucket:           "cfg_desired",
			AutoCreateBucket: true,
		},
	}
	client, err := agentcore.New(cfg)
	if err != nil {
		t.Fatalf("failed to create downstream agentcore client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Start(ctx); err != nil {
		t.Fatalf("failed to start downstream agentcore client: %v", err)
	}
	t.Cleanup(func() {
		_ = client.Close(context.Background())
	})
	return client
}

func TestE2E_State_DualStack_IPv4_IPv6_Preservation(t *testing.T) {
	ns := startEmbeddedNATS(t)
	mc := startMockCloud(t)
	cfg := getTestConfig(t, mc, ns)

	startClientProcess(t, mc, cfg)

	downstream := newDownstreamAgentClient(t, ns.ClientURL())

	dualStackPayload := json.RawMessage(`{
		"serial": "stale-inner-serial",
		"uuid": 111,
		"version": 1,
		"unit": {"uptime": 12345},
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
				"ntp_server": "time.cloudflare.com"
			}
		],
		"radios": []
	}`)

	// Give the client process a brief moment to complete NATS subscription registration
	time.Sleep(300 * time.Millisecond)

	err := downstream.PublishState(context.Background(), agentcore.StateEnvelope{
		Version:   "1.0",
		Target:    "vyos",
		UUID:      "1793880893",
		Payload:   dualStackPayload,
		Timestamp: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("downstream PublishState failed: %v", err)
	}

	var stateMsg map[string]any
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		raw := mc.WaitForResponse(t, time.Until(deadline))
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			continue
		}
		if m["method"] == "state" {
			stateMsg = m
			break
		}
	}
	if stateMsg == nil {
		t.Fatalf("did not receive state notification on MockCloud")
	}

	params := stateMsg["params"].(map[string]any)
	if uint64(params["uuid"].(float64)) != 1793880893 {
		t.Fatalf("expected outer uuid=1793880893, got %v", params["uuid"])
	}
	stateObj := params["state"].(map[string]any)
	if _, hasSerial := stateObj["serial"]; hasSerial {
		t.Fatalf("expected inner serial to be stripped")
	}
	ifaces := stateObj["interfaces"].([]any)
	iface0 := ifaces[0].(map[string]any)
	ipv6s := iface0["ipv6_addresses"].([]any)
	ipv6Obj := ipv6s[0].(map[string]any)
	if ipv6Obj["address"] != "2001:db8::1/64" || uint64(ipv6Obj["valid"].(float64)) != 3600 {
		t.Fatalf("IPv6 address object not preserved end-to-end: %+v", ipv6Obj)
	}
	leases := iface0["ipv6_leases"].([]any)
	if leases[0].(map[string]any)["hostname"] != "client-1" {
		t.Fatalf("IPv6 lease not preserved end-to-end")
	}
}

func TestE2E_OnDemandState_Positive_Ordering(t *testing.T) {
	ns := startEmbeddedNATS(t)
	mc := startMockCloud(t)
	cfg := getTestConfig(t, mc, ns)

	startClientProcess(t, mc, cfg)

	downstream := newDownstreamAgentClient(t, ns.ClientURL())
	err := downstream.RegisterStateGetHandler("vyos", func(_ context.Context) (agentcore.StateEnvelope, error) {
		return agentcore.StateEnvelope{
			Version:   "1.0",
			Target:    "vyos",
			UUID:      "1793880893",
			Payload:   json.RawMessage(`{"version":1,"unit":{"uptime":555},"interfaces":[],"radios":[]}`),
			Timestamp: time.Now().UTC(),
		}, nil
	})
	if err != nil {
		t.Fatalf("RegisterStateGetHandler failed: %v", err)
	}

	// Give subscription a brief moment to settle
	time.Sleep(200 * time.Millisecond)

	reqCmd := map[string]any{
		"jsonrpc": "2.0",
		"id":      201,
		"method":  "request",
		"params": map[string]any{
			"message":      "state",
			"request_uuid": "e2e-req-201",
		},
	}
	reqBytes, _ := json.Marshal(reqCmd)
	mc.SendMessage(t, string(reqBytes))

	// Frame 1 MUST arrive first: JSON-RPC ack for id=201
	raw1 := mc.WaitForResponse(t, 5*time.Second)
	var frame1 map[string]any
	if err := json.Unmarshal(raw1, &frame1); err != nil {
		t.Fatalf("unmarshal frame1 failed: %v", err)
	}
	if frame1["id"] == nil || int(frame1["id"].(float64)) != 201 {
		t.Fatalf("expected Frame 1 to be JSON-RPC response for id=201, got: %s", string(raw1))
	}
	status := frame1["result"].(map[string]any)["status"].(map[string]any)
	if int(status["error"].(float64)) != 0 {
		t.Fatalf("expected error=0 in Frame 1, got %+v", status)
	}

	// Frame 2 MUST arrive second: state notification with request_uuid
	raw2 := mc.WaitForResponse(t, 5*time.Second)
	var frame2 map[string]any
	if err := json.Unmarshal(raw2, &frame2); err != nil {
		t.Fatalf("unmarshal frame2 failed: %v", err)
	}
	if frame2["method"] != "state" {
		t.Fatalf("expected Frame 2 to be method=state, got: %s", string(raw2))
	}
	params := frame2["params"].(map[string]any)
	if params["request_uuid"] != "e2e-req-201" {
		t.Fatalf("expected request_uuid=e2e-req-201, got %v", params["request_uuid"])
	}
	if uint64(params["uuid"].(float64)) != 1793880893 {
		t.Fatalf("expected uuid=1793880893, got %v", params["uuid"])
	}
}
