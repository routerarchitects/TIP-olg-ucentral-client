package contracts

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

// EnvelopeVersion is the required wire protocol version for all NATS envelopes.
const EnvelopeVersion = "1.0"

// ValidateNATSTarget strictly validates that a NATS target string is valid.
func ValidateNATSTarget(target string) error {
	if target == "" {
		return errors.New("nats target is required and cannot be empty")
	}
	if strings.TrimSpace(target) != target {
		return errors.New("nats target must not contain leading or trailing whitespace")
	}
	if strings.ContainsAny(target, ".*> \t\r\n") {
		return errors.New("nats target must not contain wildcards, dots, or internal whitespace")
	}
	return nil
}

// ValidateDesiredConfigRecord verifies that a DesiredConfigRecord is complete and valid.

// ValidateActionCommand strictly validates an incoming ActionCommand envelope.

// ValidateCommandPayload decodes and strictly validates action-specific payloads based on command and action.

// ValidateResultPayload verifies that the downstream agent's result payload
// matches the expected shape of the corresponding cloud status structure.

type DeviceCapabilities struct {
	Capabilities json.RawMessage `json:"capabilities"`
	Firmware     string          `json:"firmware"`
}

type CloudCapabilitiesQuery struct {
	Version   string    `json:"version"`
	RPCID     string    `json:"rpc_id"`
	Target    string    `json:"target"`
	Timestamp time.Time `json:"timestamp"`
}

func (q *CloudCapabilitiesQuery) Validate() error {
	if q.Version != EnvelopeVersion {
		return fmt.Errorf("unsupported envelope version: %q", q.Version)
	}
	if q.RPCID == "" || q.Target == "" || q.Timestamp.IsZero() {
		return errors.New("missing required fields in CloudCapabilitiesQuery")
	}
	return nil
}

type CloudDeviceStatusQuery struct {
	Version   string    `json:"version"`
	RPCID     string    `json:"rpc_id"`
	Target    string    `json:"target"`
	Timestamp time.Time `json:"timestamp"`
}

func (q *CloudDeviceStatusQuery) Validate() error {
	if q.Version != EnvelopeVersion {
		return fmt.Errorf("unsupported envelope version: %q", q.Version)
	}
	if q.RPCID == "" || q.Target == "" || q.Timestamp.IsZero() {
		return errors.New("missing required fields in CloudDeviceStatusQuery")
	}
	return nil
}

type DeviceStatus struct {
	Status json.RawMessage `json:"status"`
}

// ValidateStatusEnvelope verifies a StatusEnvelope is well-formed.

// ValidateCommandPayload decodes and strictly validates action-specific payloads based on command and action.
func ValidateCommandPayload(command CommandType, action ActionType, payload json.RawMessage) error {
	var req interface{ Validate() error }

	switch {
	case command == CommandConfigure:
		req = &CloudConfigureRequest{}
	case action == ActionFactory:
		req = &CloudFactoryRequest{}
	case action == ActionCertupdate:
		req = &CloudCertupdateRequest{}
	case action == ActionReenroll:
		req = &CloudReenrollRequest{}
	case action == ActionRTTY:
		req = &CloudRemoteAccessRequest{}
	case action == ActionLeds:
		req = &CloudLedsRequest{}
	case action == ActionTrace:
		req = &CloudTraceRequest{}
	case action == ActionPing:
		req = &CloudPingRequest{}
	case action == ActionTelemetry:
		req = &CloudTelemetryRequest{}
	case action == ActionReboot || command == CommandReboot:
		req = &CloudRebootRequest{}
	case action == ActionUpgrade || command == CommandUpgrade:
		req = &CloudUpgradeRequest{}
	case action == ActionExecute || command == CommandScript:
		req = &CloudScriptRequest{}
	case action == ActionCapabilitiesGet || action == ActionStatusGet || command == CommandQuery:
		if len(payload) == 0 || bytes.Equal(bytes.TrimSpace(payload), []byte("null")) {
			return nil
		}

		if !json.Valid(payload) {
			return errors.New("query payload contains invalid JSON")
		}

		var queryPayload map[string]json.RawMessage
		if err := json.Unmarshal(payload, &queryPayload); err != nil {
			return errors.New("query payload must be a JSON object")
		}

		if len(queryPayload) != 0 {
			return errors.New("query payload must be empty")
		}

		return nil
	default:
		// Unknown or no-payload action
		if len(payload) > 0 && !json.Valid(payload) {
			return errors.New("payload contains invalid JSON")
		}
		return nil
	}

	if len(payload) == 0 || string(payload) == "null" {
		return fmt.Errorf("payload is required for command %q action %q", command, action)
	}

	decoder := json.NewDecoder(bytes.NewReader(payload))
	if err := decoder.Decode(req); err != nil {
		return fmt.Errorf("malformed payload for command %q action %q: %w", command, action, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("trailing JSON in payload for command %q action %q", command, action)
	}

	if err := req.Validate(); err != nil {
		return fmt.Errorf("invalid payload for command %q action %q: %w", command, action, err)
	}

	return nil
}

// ValidateResultPayload verifies that the downstream agent's result payload
// matches the expected shape of the corresponding cloud status structure.
func ValidateResultPayload(command CommandType, action ActionType, payload json.RawMessage) error {
	if len(payload) == 0 || string(bytes.TrimSpace(payload)) == "null" {
		return nil // Payload is optional for many NATS command results.
	}

	decoder := json.NewDecoder(bytes.NewReader(payload))
	// Note: We intentionally do NOT use DisallowUnknownFields() here to maintain
	// permissive validation for forward compatibility, matching request payload behavior.

	switch command {
	case CommandConfigure:
		var status CloudConfigureResultStatus
		return decoder.Decode(&status)
	case CommandReboot:
		var status CloudRebootStatus
		return decoder.Decode(&status)
	case CommandScript:
		var status CloudScriptStatus
		return decoder.Decode(&status)
	case CommandUpgrade:
		var status CloudUpgradeStatus
		return decoder.Decode(&status)
	case CommandAction:
		switch action {
		case ActionFactory:
			var status CloudFactoryStatus
			return decoder.Decode(&status)
		case ActionTelemetry:
			var status CloudTelemetryStatus
			return decoder.Decode(&status)
		case ActionRTTY:
			var status CloudRemoteAccessStatus
			return decoder.Decode(&status)
		case ActionCertupdate:
			var status CloudCertupdateStatus
			return decoder.Decode(&status)
		case ActionReenroll:
			var status CloudReenrollStatus
			return decoder.Decode(&status)
		case ActionLeds:
			var status CloudLedsStatus
			return decoder.Decode(&status)
		case ActionTrace:
			var status CloudTraceStatus
			return decoder.Decode(&status)
		case ActionPing:
			// Ping does not use a status struct in the payload for NATS
			return nil
		default:
			// If it's an action we don't strictly validate, allow it.
			return nil
		}
	default:
		// Other commands (like query) might not have payload validation defined yet.
		return nil
	}
}
