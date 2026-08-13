package contracts

import (
	"encoding/json"
	"errors"
	"fmt"
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

