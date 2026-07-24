package contracts

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/Telecominfraproject/olg-nats-agent-core/agentcore"
)

// EnvelopeVersion is the required wire protocol version for all NATS envelopes.
const EnvelopeVersion = "1.0"

func ValidateConfigureNotification(c *agentcore.ConfigureNotification) error {
	if c.Version != EnvelopeVersion {
		return fmt.Errorf("unsupported envelope version: %q", c.Version)
	}
	if c.RPCID == "" || c.Target == "" || c.KVBucket == "" || c.KVKey == "" || c.Timestamp.IsZero() {
		return errors.New("missing required fields in ConfigureNotification")
	}
	uuid, err := strconv.ParseInt(c.UUID, 10, 64)
	if err != nil || uuid <= 0 {
		return errors.New("uuid must be a positive int64")
	}
	return nil
}

// ValidateActionCommand strictly validates an incoming ActionCommand envelope.
func ValidateActionCommand(c *agentcore.ActionCommand) error {
	if CommandType(c.CommandType) == CommandConfigure {
		return errors.New("command 'configure' must use ConfigureNotification envelope, not ActionCommand")
	}
	if c.Version != EnvelopeVersion {
		return fmt.Errorf("unsupported envelope version: %q", c.Version)
	}
	if c.RPCID == "" || c.Target == "" || c.Timestamp.IsZero() {
		return errors.New("missing required fields in ActionCommand")
	}
	if !CommandType(c.CommandType).Valid() {
		return fmt.Errorf("invalid command_type: %q", c.CommandType)
	}
	if !ValidCommandAction(CommandType(c.CommandType), ActionType(c.Action)) {
		return fmt.Errorf("inconsistent action %q for command_type %q", c.Action, c.CommandType)
	}
	if err := ValidateCommandPayload(CommandType(c.CommandType), ActionType(c.Action), c.Payload); err != nil {
		return err
	}
	return nil
}

// ValidateCommandPayload decodes and strictly validates action-specific payloads based on command and action.
func ValidateCommandPayload(command CommandType, action ActionType, payload json.RawMessage) error {
	var req interface{ Validate() error }

	switch {
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

type DeviceCapabilities struct {
	Capabilities json.RawMessage `json:"capabilities"`
	Firmware     string          `json:"firmware"`
}

// ValidateStatusEnvelope verifies a StatusEnvelope is well-formed.
func ValidateStatusEnvelope(s *agentcore.StatusEnvelope) error {
	if s.Version != EnvelopeVersion {
		return fmt.Errorf("unsupported envelope version: %q", s.Version)
	}
	if s.Target == "" || s.Status == "" || s.Timestamp.IsZero() {
		return errors.New("missing required fields in StatusEnvelope")
	}
	return nil
}

func ValidateResultEnvelope(r *agentcore.ResultEnvelope) error {
	if r.Version != EnvelopeVersion {
		return fmt.Errorf("unsupported envelope version: %q", r.Version)
	}
	if r.RPCID == "" || r.Target == "" || r.CommandType == "" || r.Result == "" || r.Timestamp.IsZero() {
		return errors.New("missing required fields in ResultEnvelope")
	}
	if !CommandType(r.CommandType).Valid() {
		return fmt.Errorf("invalid command_type: %q", r.CommandType)
	}
	if !ResultType(r.Result).Valid() {
		return fmt.Errorf("invalid result: %q", r.Result)
	}
	if len(r.Payload) > 0 && !json.Valid(r.Payload) {
		return errors.New("payload contains invalid JSON")
	}
	if r.CommandType == string(CommandConfigure) {
		uuid, err := strconv.ParseInt(r.UUID, 10, 64)
		if err != nil || uuid <= 0 {
			return errors.New("uuid must be a positive int64 for configure results")
		}
	}

	// For successful or error results that carry a payload, validate the shape.
	// Some operations might not strictly mandate a payload on every error type depending on the Cloud,
	// but the NATS contract generally expects the status block. We enforce shape matching here.
	if err := ValidateResultPayload(CommandType(r.CommandType), ActionType(r.Action), r.Payload); err != nil {
		return fmt.Errorf("invalid result payload for %q (action: %q): %w", r.CommandType, r.Action, err)
	}

	return nil
}
