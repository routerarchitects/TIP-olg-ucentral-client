package contracts

import (
	"encoding/json"
	"testing"
)

func TestInterfaceIPv6SchemaRoundTrip(t *testing.T) {
	validSec := uint64(3600)
	iface := InterfaceState{
		Name: "eth0",
		IPv4: &InterfaceIPv4{
			Addresses:  []string{"192.168.1.1/24"},
			DHCPServer: "192.168.1.254",
			Leasetime:  43200,
		},
		IPv6Addresses: []InterfaceIPv6Address{
			{Address: "2001:db8::1/64", Valid: &validSec},
			{Address: "fe80::1/64"},
		},
		IPv6Leases: []InterfaceIPv6Lease{
			{Addresses: []string{"2001:db8::100"}, Hostname: "client-1"},
		},
	}

	raw, err := json.Marshal(iface)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var decoded InterfaceState
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if len(decoded.IPv6Addresses) != 2 {
		t.Fatalf("expected 2 IPv6 addresses, got %d", len(decoded.IPv6Addresses))
	}
	if decoded.IPv6Addresses[0].Address != "2001:db8::1/64" {
		t.Fatalf("unexpected IPv6 address: %q", decoded.IPv6Addresses[0].Address)
	}
	if decoded.IPv6Addresses[0].Valid == nil || *decoded.IPv6Addresses[0].Valid != 3600 {
		t.Fatalf("expected valid=3600, got %v", decoded.IPv6Addresses[0].Valid)
	}
	if decoded.IPv6Addresses[1].Valid != nil {
		t.Fatalf("expected omitted valid to remain nil, got %v", *decoded.IPv6Addresses[1].Valid)
	}
	if len(decoded.IPv6Leases) != 1 || decoded.IPv6Leases[0].Hostname != "client-1" {
		t.Fatalf("unexpected IPv6 leases: %+v", decoded.IPv6Leases)
	}
}

func TestCloudRequestCommandRequest_Validate(t *testing.T) {
	valid := CloudRequestCommandRequest{
		Serial:      "60cf84f22290",
		Message:     "state",
		RequestUUID: "req-101",
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	cases := []struct {
		name string
		req  CloudRequestCommandRequest
	}{
		{"empty message", CloudRequestCommandRequest{RequestUUID: "req-1"}},
		{"unsupported message", CloudRequestCommandRequest{Message: "unknown", RequestUUID: "req-1"}},
		{"missing request_uuid", CloudRequestCommandRequest{Message: "state"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.req.Validate(); err == nil {
				t.Fatalf("expected validation error for %s", tc.name)
			}
		})
	}
}

func TestParseStateUUID(t *testing.T) {
	cases := []struct {
		in   string
		want uint64
	}{
		{"1793880893", 1793880893},
		{"", 0},
		{"0", 0},
		{"   ", 0},
		{"invalid-uuid", 0},
		{"-5", 0},
	}
	for _, tc := range cases {
		if got := ParseStateUUID(tc.in); got != tc.want {
			t.Errorf("ParseStateUUID(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestSanitizeAndBuildCloudStateNotification_Lossless(t *testing.T) {
	// Payload includes dual-stack IPv4, schema-compliant IPv6 object array, IPv6 leases,
	// extended unknown fields, AND conflicting inner serial/uuid that must be stripped.
	rawPayload := []byte(`{
		"serial": "stale-inner-serial",
		"uuid": 999999,
		"version": 1,
		"unit": {"uptime": 45123},
		"interfaces": [
			{
				"name": "eth0",
				"ipv4": {"addresses": ["192.168.1.1/24"], "dhcp_server": "192.168.1.254"},
				"ipv6_addresses": [{"address": "2001:db8::1/64", "valid": 3600}],
				"ipv6_leases": [{"addresses": ["2001:db8::100"], "hostname": "client-1"}],
				"custom_vendor_ext": {"foo": "bar"}
			}
		],
		"radios": [],
		"future_extension_field": 42
	}`)

	notifBytes, err := BuildCloudStateNotification("60cf84f22290", "1793880893", rawPayload, "req-101")
	if err != nil {
		t.Fatalf("BuildCloudStateNotification failed: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal(notifBytes, &parsed); err != nil {
		t.Fatalf("unmarshal notification failed: %v", err)
	}

	if parsed["jsonrpc"] != "2.0" || parsed["method"] != "state" {
		t.Fatalf("unexpected envelope headers: %+v", parsed)
	}

	params := parsed["params"].(map[string]any)
	if params["serial"] != "60cf84f22290" {
		t.Fatalf("expected outer serial 60cf84f22290, got %v", params["serial"])
	}
	if uint64(params["uuid"].(float64)) != 1793880893 {
		t.Fatalf("expected outer uuid 1793880893, got %v", params["uuid"])
	}
	if params["request_uuid"] != "req-101" {
		t.Fatalf("expected request_uuid req-101, got %v", params["request_uuid"])
	}

	stateObj := params["state"].(map[string]any)
	if _, exists := stateObj["serial"]; exists {
		t.Fatalf("expected inner serial to be stripped")
	}
	if _, exists := stateObj["uuid"]; exists {
		t.Fatalf("expected inner uuid to be stripped")
	}
	if stateObj["future_extension_field"].(float64) != 42 {
		t.Fatalf("expected future_extension_field to be preserved losslessly")
	}

	ifaces := stateObj["interfaces"].([]any)
	iface0 := ifaces[0].(map[string]any)
	ipv6s := iface0["ipv6_addresses"].([]any)
	ipv6Obj := ipv6s[0].(map[string]any)
	if ipv6Obj["address"] != "2001:db8::1/64" || uint64(ipv6Obj["valid"].(float64)) != 3600 {
		t.Fatalf("expected IPv6 address object preserved losslessly, got %+v", ipv6Obj)
	}
	if iface0["custom_vendor_ext"].(map[string]any)["foo"] != "bar" {
		t.Fatalf("expected nested custom_vendor_ext preserved losslessly")
	}
}
