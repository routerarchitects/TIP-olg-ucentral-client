package contracts

import (
	"encoding/json"
	"errors"
	"fmt"
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
	if err := RequireOperationID(string(c.Action), c.OperationID); err != nil {
		return err
	}
	if len(c.Payload) > 0 && !json.Valid(c.Payload) {
		return errors.New("payload contains invalid JSON")
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

	if err := RequireOperationID(s.Operation, s.OperationID); err != nil {
		return err
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
	if err := RequireOperationID(string(r.CommandType), r.OperationID); err != nil {
		return err
	}
	if err := RequireOperationID(string(r.Action), r.OperationID); err != nil {
		return err
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
