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

func ValidateConfigureNotification(c *agentcore.ConfigureNotification) error {
	if c.Version == "" || c.RPCID == "" || c.Target == "" || c.KVBucket == "" || c.KVKey == "" || c.Timestamp.IsZero() {
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
	if c.Version == "" || c.RPCID == "" || c.Target == "" || c.Timestamp.IsZero() {
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

type DeviceCapabilities struct {
	Capabilities json.RawMessage `json:"capabilities"`
	Firmware     string          `json:"firmware"`
}

func ValidateDeviceStatus(s *agentcore.StatusEnvelope) error {
	if s.Version == "" || s.Target == "" || s.Status == "" || s.Timestamp.IsZero() {
		return errors.New("missing required fields in StatusEnvelope")
	}
	return nil
}

func ValidateResultEnvelope(r *agentcore.ResultEnvelope) error {
	if r.Version == "" || r.RPCID == "" || r.Target == "" || r.CommandType == "" || r.Result == "" || r.Timestamp.IsZero() {
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
	if r.CommandType == string(CommandConfigure) && r.UUID == "" {
		return errors.New("uuid is required for configure results")
	}
	return nil
}
