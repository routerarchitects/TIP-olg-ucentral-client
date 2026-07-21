package contracts

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

type ConfigureCommand struct {
	Version       string `json:"version"`
	CorrelationID string `json:"correlation_id"`
	Target        string `json:"target"`
	UUID          int64  `json:"uuid"`
	KVKey         string `json:"kv_key"`
	KVRevision    uint64 `json:"kv_revision"`
	Timestamp     string `json:"timestamp"`
}

func (c *ConfigureCommand) Validate() error {
	if c.Version == "" || c.CorrelationID == "" || c.Target == "" || c.KVKey == "" || c.Timestamp == "" {
		return errors.New("missing required fields in ConfigureCommand")
	}
	if c.KVRevision == 0 {
		return errors.New("KVRevision must be > 0")
	}
	if c.UUID <= 0 {
		return errors.New("UUID must be > 0")
	}
	return nil
}

type ActionCommand struct {
	Version       string          `json:"version"`
	CorrelationID string          `json:"correlation_id"`
	OperationID   string          `json:"operation_id,omitempty"`
	Target        string          `json:"target"`
	CommandType   CommandType     `json:"command_type"`
	Action        ActionType      `json:"action"`
	Payload       json.RawMessage `json:"payload"`
	Timestamp     string          `json:"timestamp"`
}

// Validate enforces required envelope fields and requires operation_id
// when Action == "upgrade".
func (c *ActionCommand) Validate() error {
	if c.Version == "" || c.CorrelationID == "" || c.Target == "" || c.Timestamp == "" {
		return errors.New("missing required fields in ActionCommand")
	}
	if !c.CommandType.Valid() {
		return fmt.Errorf("invalid command_type: %q", c.CommandType)
	}
	if !ValidCommandAction(c.CommandType, c.Action) {
		return fmt.Errorf("inconsistent action %q for command_type %q", c.Action, c.CommandType)
	}
	if RequiresOperationID(c.CommandType, c.Action) && c.OperationID == "" {
		return errors.New("operation_id is required for upgrade")
	}
	if err := ValidateCommandPayload(c.CommandType, c.Action, c.Payload); err != nil {
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
		// Queries don't require nested payloads for validation here
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

type DeviceStatus struct {
	Version       string `json:"version"`
	CorrelationID string `json:"correlation_id,omitempty"`
	OperationID   string `json:"operation_id,omitempty"`
	Target        string `json:"target"`
	Operation     string `json:"operation,omitempty"`
	Active        bool   `json:"active,omitempty"`
	Stage         string `json:"stage,omitempty"`
	Status        string `json:"status"`
	Message       string `json:"message,omitempty"`
	Timestamp     string `json:"timestamp"`
}

func (s *DeviceStatus) Validate() error {
	if s.Version == "" || s.Target == "" || s.Status == "" || s.Timestamp == "" {
		return errors.New("missing required fields in DeviceStatus")
	}

	if s.Active && s.OperationID == "" {
		return errors.New("operation_id is required for active operation status")
	}

	if RequiresOperationID(CommandType(s.Operation), ActionType(s.Operation)) && s.OperationID == "" {
		return errors.New("operation_id is required for upgrade")
	}

	return nil
}

type ResultEnvelope struct {
	Version       string          `json:"version"`
	CorrelationID string          `json:"correlation_id"`
	Target        string          `json:"target"`
	CommandType   CommandType     `json:"command_type"`
	Action        ActionType      `json:"action,omitempty"`
	OperationID   string          `json:"operation_id,omitempty"` // Mandatory for upgrade results
	UUID          int64           `json:"uuid,omitempty"`         // Omitted for Action
	Result        ResultType      `json:"result"`
	Message       string          `json:"message"`
	Payload       json.RawMessage `json:"payload,omitempty"` // Command-specific data (e.g. latency, result_64)
	Timestamp     string          `json:"timestamp"`
}

func (r *ResultEnvelope) Validate() error {
	if r.Version == "" || r.CorrelationID == "" || r.Target == "" || r.Result == "" || r.Timestamp == "" {
		return errors.New("missing required fields in ResultEnvelope")
	}
	if !ValidCommandAction(r.CommandType, r.Action) {
		return fmt.Errorf("inconsistent action %q for command_type %q", r.Action, r.CommandType)
	}
	if !r.Result.Valid() {
		return fmt.Errorf("invalid result: %q", r.Result)
	}
	if RequiresOperationID(r.CommandType, r.Action) && r.OperationID == "" {
		return errors.New("operation_id is required for upgrade")
	}
	if len(r.Payload) > 0 && !json.Valid(r.Payload) {
		return errors.New("payload contains invalid JSON")
	}
	return nil
}

type CloudCapabilitiesQuery struct {
	Version       string      `json:"version"`
	CorrelationID string      `json:"correlation_id"`
	Target        string      `json:"target"`
	CommandType   CommandType `json:"command_type"`
	Action        ActionType  `json:"action"`
	Timestamp     string      `json:"timestamp"`
}

func (q *CloudCapabilitiesQuery) Validate() error {
	if q.Version == "" || q.CorrelationID == "" || q.Target == "" || q.Timestamp == "" {
		return errors.New("missing required fields in CloudCapabilitiesQuery")
	}
	if !ValidCommandAction(q.CommandType, q.Action) {
		return fmt.Errorf("inconsistent action %q for command_type %q", q.Action, q.CommandType)
	}
	return nil
}

type CloudDeviceStatusQuery struct {
	Version       string      `json:"version"`
	CorrelationID string      `json:"correlation_id"`
	OperationID   string      `json:"operation_id,omitempty"`
	Target        string      `json:"target"`
	CommandType   CommandType `json:"command_type"`
	Action        ActionType  `json:"action"`
	Timestamp     string      `json:"timestamp"`
}

func (q *CloudDeviceStatusQuery) Validate() error {
	if q.Version == "" || q.CorrelationID == "" || q.Target == "" || q.Timestamp == "" {
		return errors.New("missing required fields in CloudDeviceStatusQuery")
	}
	if !ValidCommandAction(q.CommandType, q.Action) {
		return fmt.Errorf("inconsistent action %q for command_type %q", q.Action, q.CommandType)
	}
	return nil
}
