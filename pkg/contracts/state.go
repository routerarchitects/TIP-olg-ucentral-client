package contracts

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
)

// UnitMemoryState models unit memory metrics in the OpenWiFi state schema.
type UnitMemoryState struct {
	Total    uint64 `json:"total"`
	Free     uint64 `json:"free"`
	Cached   uint64 `json:"cached"`
	Buffered uint64 `json:"buffered"`
}

// UnitState models system-level metrics in the OpenWiFi state schema.
type UnitState struct {
	Load      []float64       `json:"load"`
	CPULoad   []uint64        `json:"cpu_load,omitempty"`
	Localtime int64           `json:"localtime"`
	Memory    UnitMemoryState `json:"memory"`
	Uptime    uint64          `json:"uptime"`
}

// InterfaceCounters models packet/byte counters for an interface.
type InterfaceCounters struct {
	RxBytes    uint64 `json:"rx_bytes"`
	RxPackets  uint64 `json:"rx_packets"`
	RxErrors   uint64 `json:"rx_errors"`
	RxDropped  uint64 `json:"rx_dropped"`
	Multicast  uint64 `json:"multicast,omitempty"`
	Collisions uint64 `json:"collisions,omitempty"`
	TxBytes    uint64 `json:"tx_bytes"`
	TxPackets  uint64 `json:"tx_packets"`
	TxErrors   uint64 `json:"tx_errors"`
	TxDropped  uint64 `json:"tx_dropped"`
}

// InterfaceIPv4 models IPv4 configuration and DHCP lease state on an interface.
type InterfaceIPv4 struct {
	Addresses  []string `json:"addresses"`
	DHCPServer string   `json:"dhcp_server,omitempty"`
	Leasetime  uint64   `json:"leasetime,omitempty"`
}

// InterfaceClient models observed L3 network neighbors deduplicated by MAC.
// Filtered to exclude FAILED/INCOMPLETE states from ip -j neigh.
type InterfaceClient struct {
	MAC           string   `json:"mac"`
	IPv4Addresses []string `json:"ipv4_addresses,omitempty"`
	IPv6Addresses []string `json:"ipv6_addresses,omitempty"`
	Ports         []string `json:"ports,omitempty"`
	LastSeen      *uint64  `json:"last_seen,omitempty"` // Elapsed seconds since last activity; omitted if unmeasurable
}

// InterfaceIPv6Address conforms to the OpenWiFi state schema (#/$defs/interface.ipv6-address)
// representing an IPv6 address/prefix assigned to the interface and its remaining delegation lifetime.
type InterfaceIPv6Address struct {
	Address string  `json:"address"`
	Valid   *uint64 `json:"valid,omitempty"` // Remaining delegation validity in seconds; omitted if permanent/unbounded
}

// InterfaceIPv6Lease conforms to the OpenWiFi state schema (#/$defs/interface.ipv6-lease)
// representing an active DHCPv6/SLAAC lease served by this interface.
type InterfaceIPv6Lease struct {
	Addresses []string `json:"addresses"`
	Hostname  string   `json:"hostname,omitempty"`
}

// InterfaceState models a logical or physical network interface in the OpenWiFi state schema.
type InterfaceState struct {
	Name          string                 `json:"name"`
	Location      string                 `json:"location,omitempty"`
	Uptime        uint64                 `json:"uptime,omitempty"`
	IPv4          *InterfaceIPv4         `json:"ipv4,omitempty"`
	IPv6Addresses []InterfaceIPv6Address `json:"ipv6_addresses,omitempty"`
	IPv6Leases    []InterfaceIPv6Lease   `json:"ipv6_leases,omitempty"`
	DNSServers    []string               `json:"dns_servers,omitempty"`
	NTPServer     string                 `json:"ntp_server,omitempty"`
	Counters      InterfaceCounters      `json:"counters"`
	DeltaCounters InterfaceCounters      `json:"delta_counters"`
	Clients       []InterfaceClient      `json:"clients,omitempty"`
	SSIDs         json.RawMessage        `json:"ssids,omitempty"`
	MeshPath      json.RawMessage        `json:"mesh-path,omitempty"`
}

// PortLinkState models physical link-state metrics for an upstream/downstream port.
type PortLinkState struct {
	Carrier bool   `json:"carrier"`
	Speed   int    `json:"speed,omitempty"`
	Duplex  string `json:"duplex,omitempty"`
}

// LinkStateSection groups physical port link states by WAN/LAN role.
type LinkStateSection struct {
	Upstream   map[string]PortLinkState `json:"upstream"`
	Downstream map[string]PortLinkState `json:"downstream"`
}

// DeviceState models the inner OpenWiFi router telemetry payload (params.state).
// Used for schema-complete structured modeling, local validation, and test assertions.
type DeviceState struct {
	Version    int               `json:"version"`
	Unit       UnitState         `json:"unit"`
	Interfaces []InterfaceState  `json:"interfaces"`
	Radios     []json.RawMessage `json:"radios"` // Initialized to [] for routers without wireless
	LinkState  *LinkStateSection `json:"link-state,omitempty"`
	LLDPPeers  json.RawMessage   `json:"lldp-peers,omitempty"`
}

// CloudStateParams wraps the telemetry payload for WebSocket transmission to the Cloud Gateway.
// State is forwarded as json.RawMessage (lossless validated JSON forwarding) after validating
// well-formed JSON and stripping any rogue inner serial/uuid fields.
type CloudStateParams struct {
	Serial      string          `json:"serial"`
	UUID        uint64          `json:"uuid"`
	State       json.RawMessage `json:"state"`
	RequestUUID string          `json:"request_uuid,omitempty"`
}

// CloudStateNotification models the upstream JSON-RPC 2.0 "state" notification.
type CloudStateNotification struct {
	JSONRPC string           `json:"jsonrpc"`
	Method  string           `json:"method"`
	Params  CloudStateParams `json:"params"`
}

// CloudRequestCommandRequest models the inbound Cloud "request" command parameters.
type CloudRequestCommandRequest struct {
	Serial      string `json:"serial,omitempty"`
	When        int64  `json:"when,omitempty"`
	Message     string `json:"message"`
	RequestUUID string `json:"request_uuid"`
}

// Validate checks that CloudRequestCommandRequest is well-formed for supported request messages.
func (r *CloudRequestCommandRequest) Validate() error {
	if strings.TrimSpace(r.Message) == "" {
		return errors.New("message is required")
	}
	if r.Message != "state" {
		return fmt.Errorf("unsupported request message type: %q", r.Message)
	}
	if strings.TrimSpace(r.RequestUUID) == "" {
		return errors.New("request_uuid is required for state request")
	}
	return nil
}

// ParseStateUUID converts a downstream string UUID from StateEnvelope into a numeric uint64.
// Empty, "0", or invalid non-numeric values safely map to 0.
func ParseStateUUID(rawUUID string) uint64 {
	trimmed := strings.TrimSpace(rawUUID)
	if trimmed == "" || trimmed == "0" {
		return 0
	}
	parsed, err := strconv.ParseUint(trimmed, 10, 64)
	if err != nil {
		log.Printf("[STATE] invalid non-numeric downstream state UUID %q; defaulting params.uuid to 0", rawUUID)
		return 0
	}
	return parsed
}

// SanitizeStatePayload validates that payload is a non-empty JSON object, strips any
// conflicting top-level "serial" or "uuid" fields, and preserves all other fields losslessly.
func SanitizeStatePayload(payload []byte) (json.RawMessage, error) {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 {
		return nil, errors.New("state payload is empty")
	}
	if !json.Valid(trimmed) {
		return nil, errors.New("state payload is not valid JSON")
	}
	if trimmed[0] != '{' {
		return nil, errors.New("state payload must be a JSON object")
	}

	var obj map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &obj); err != nil {
		return nil, fmt.Errorf("failed to parse state payload object: %w", err)
	}

	_, hasSerial := obj["serial"]
	_, hasUUID := obj["uuid"]
	if !hasSerial && !hasUUID {
		// No stripping needed; return validated raw JSON verbatim
		return json.RawMessage(trimmed), nil
	}

	delete(obj, "serial")
	delete(obj, "uuid")

	sanitized, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal sanitized state payload: %w", err)
	}
	return json.RawMessage(sanitized), nil
}

// BuildCloudStateNotification constructs a canonical JSON-RPC 2.0 "state" notification
// with authoritative outer identity (serial, uuid) and lossless validated state forwarding.
func BuildCloudStateNotification(serial, rawUUID string, payload []byte, requestUUID string) ([]byte, error) {
	sanitized, err := SanitizeStatePayload(payload)
	if err != nil {
		return nil, err
	}

	notif := CloudStateNotification{
		JSONRPC: JSONRPCVersion,
		Method:  "state",
		Params: CloudStateParams{
			Serial:      serial,
			UUID:        ParseStateUUID(rawUUID),
			State:       sanitized,
			RequestUUID: requestUUID,
		},
	}

	return json.Marshal(notif)
}
