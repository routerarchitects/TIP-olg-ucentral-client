package contracts

import (
	"encoding/json"
	"errors"
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
	return nil
}

type ActionCommand struct {
	Version       string          `json:"version"`
	CorrelationID string          `json:"correlation_id"`
	OperationID   string          `json:"operation_id,omitempty"`
	Target        string          `json:"target"`
	CommandType   string          `json:"command_type"`
	Action        string          `json:"action"`
	Payload       json.RawMessage `json:"payload"`
	Timestamp     string          `json:"timestamp"`
}

// Validate enforces that all required fields are present. If Action == "upgrade", OperationID must be non-empty.
func (c *ActionCommand) Validate() error {
	if c.Version == "" || c.CorrelationID == "" || c.Target == "" || c.CommandType == "" || c.Action == "" || c.Timestamp == "" {
		return errors.New("missing required fields in ActionCommand")
	}
	if c.CommandType == "upgrade" && c.OperationID == "" {
		return errors.New("operation_id is mandatory for upgrade action")
	}
	if c.Payload != nil && !json.Valid(c.Payload) {
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

type ResultEnvelope struct {
	Version       string          `json:"version"`
	CorrelationID string          `json:"correlation_id"`
	Target        string          `json:"target"`
	CommandType   string          `json:"command_type"`
	OperationID   string          `json:"operation_id,omitempty"` // Mandatory for upgrade results
	UUID          int64           `json:"uuid,omitempty"`         // Omitted for Action
	Result        ResultType      `json:"result"`
	Message       string          `json:"message"`
	Payload       json.RawMessage `json:"payload,omitempty"` // Command-specific data (e.g. latency, result_64)
	Timestamp     string          `json:"timestamp"`
}

func (r *ResultEnvelope) Validate() error {
	if r.Version == "" || r.CorrelationID == "" || r.Target == "" || r.CommandType == "" || r.Result == "" || r.Timestamp == "" {
		return errors.New("missing required fields in ResultEnvelope")
	}
	if r.CommandType == "upgrade" && r.OperationID == "" {
		return errors.New("operation_id is mandatory for upgrade results")
	}
	return nil
}

type CloudCapabilitiesQuery struct {
	Version       string `json:"version"`
	CorrelationID string `json:"correlation_id"`
	Target        string `json:"target"`
	CommandType   string `json:"command_type"`
	Action        string `json:"action"`
	Timestamp     string `json:"timestamp"`
}

type CloudDeviceStatusQuery struct {
	Version       string `json:"version"`
	CorrelationID string `json:"correlation_id"`
	OperationID   string `json:"operation_id,omitempty"`
	Target        string `json:"target"`
	CommandType   string `json:"command_type"`
	Action        string `json:"action"`
	Timestamp     string `json:"timestamp"`
}
