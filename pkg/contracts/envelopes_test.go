package contracts

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestTC_CON_001_EnvelopeSerialization(t *testing.T) {
	t.Run("ActionCommand Payload Handling", func(t *testing.T) {
		// Valid ActionCommand
		validAction := ActionCommand{
			Version:       "1.0",
			CorrelationID: "corr-1",
			Target:        "ap-1",
			CommandType:   "reboot",
			Action:        "execute",
			Payload:       json.RawMessage(`{"delay": 5}`),
			Timestamp:     "2023-10-01T12:00:00Z",
		}

		b, err := json.Marshal(validAction)
		if err != nil {
			t.Fatalf("Failed to marshal ActionCommand: %v", err)
		}

		var parsed map[string]interface{}
		if err := json.Unmarshal(b, &parsed); err != nil {
			t.Fatalf("failed to unmarshal serialized value: %v", err)
		}

		if len(parsed) != 7 {
			t.Errorf("ActionCommand should have exactly 7 keys, got %d", len(parsed))
		}
		expectedKeys := []string{"version", "correlation_id", "target", "command_type", "action", "payload", "timestamp"}
		for _, key := range expectedKeys {
			if _, exists := parsed[key]; !exists {
				t.Errorf("ActionCommand missing key: %s", key)
			}
		}

		// Validation should fail for upgrade without operation_id
		upgradeAction := ActionCommand{
			Version:       "1.0",
			CorrelationID: "corr-1",
			Target:        "ap-1",
			CommandType:   "upgrade",
			Action:        "upgrade",
			Payload:       nil,
			Timestamp:     "2023-10-01T12:00:00Z",
		}

		if err := upgradeAction.Validate(); err == nil {
			t.Error("Expected Validate() to fail for upgrade without operation_id")
		}

		upgradeNilBytes, err := json.Marshal(upgradeAction)
		if err != nil {
			t.Fatalf("failed to marshal upgrade action: %v", err)
		}
		var upgradeNilParsed map[string]interface{}
		if err := json.Unmarshal(upgradeNilBytes, &upgradeNilParsed); err != nil {
			t.Fatalf("failed to unmarshal serialized value: %v", err)
		}
		if upgradeNilParsed["payload"] != nil {
			t.Errorf("ActionCommand with nil payload should serialize as null, got %v", upgradeNilParsed["payload"])
		}

		// Valid Upgrade Action with operation_id
		validUpgrade := ActionCommand{
			Version:       "1.0",
			CorrelationID: "corr-upgrade",
			OperationID:   "operation-123",
			Target:        "ap-1",
			CommandType:   "upgrade",
			Action:        "upgrade",
			Payload:       json.RawMessage(`{}`),
			Timestamp:     "2023-10-01T12:00:00Z",
		}

		upgradeBytes, err := json.Marshal(validUpgrade)
		if err != nil {
			t.Fatalf("failed to marshal valid upgrade: %v", err)
		}
		var upgradeParsed map[string]interface{}
		if err := json.Unmarshal(upgradeBytes, &upgradeParsed); err != nil {
			t.Fatalf("failed to unmarshal serialized value: %v", err)
		}

		if upgradeParsed["operation_id"] != "operation-123" {
			t.Errorf("operation_id was not correctly serialized for upgrade action")
		}
	})

	t.Run("ConfigureCommand Serialization", func(t *testing.T) {
		cmd := ConfigureCommand{
			Version:       "1.0",
			CorrelationID: "corr-1",
			Target:        "ap-1",
			UUID:          12345,
			KVKey:         "cfg",
			KVRevision:    1,
			Timestamp:     "2023-10-01T12:00:00Z",
		}

		b, err := json.Marshal(cmd)
		if err != nil {
			t.Fatalf("Failed to marshal ConfigureCommand: %v", err)
		}

		var parsed map[string]interface{}
		if err := json.Unmarshal(b, &parsed); err != nil {
			t.Fatalf("failed to unmarshal serialized value: %v", err)
		}

		if _, exists := parsed["payload"]; exists {
			t.Error("ConfigureCommand must not serialize a raw payload field")
		}
		if parsed["uuid"].(float64) != 12345 {
			t.Errorf("UUID was not serialized correctly: %v", parsed["uuid"])
		}
	})

	t.Run("ResultEnvelope Serialization", func(t *testing.T) {
		res := ResultEnvelope{
			Version:       "1.0",
			CorrelationID: "corr-1",
			Target:        "ap-1",
			CommandType:   "configure",
			UUID:          999,
			Result:        ResultSuccess,
			Message:       "OK",
			Timestamp:     "2023-10-01T12:00:00Z",
		}

		b, err := json.Marshal(res)
		if err != nil {
			t.Fatalf("Failed to marshal ResultEnvelope: %v", err)
		}

		var parsed map[string]interface{}
		if err := json.Unmarshal(b, &parsed); err != nil {
			t.Fatalf("failed to unmarshal serialized value: %v", err)
		}

		if parsed["uuid"].(float64) != 999 {
			t.Errorf("UUID must be serialized for configure results")
		}
		if _, exists := parsed["operation_id"]; exists {
			t.Error("operation_id must be omitted when empty")
		}
		if _, exists := parsed["payload"]; exists {
			t.Error("payload must be omitted when empty")
		}

		// Non-empty payload test
		resWithPayload := ResultEnvelope{
			Version:       "1.0",
			CorrelationID: "corr-script",
			Target:        "ap-1",
			CommandType:   "script",
			Result:        ResultSuccess,
			Message:       "completed",
			Payload:       json.RawMessage(`{"result_64":"YWJj"}`),
			Timestamp:     "2023-10-01T12:00:00Z",
		}
		payloadBytes, err := json.Marshal(resWithPayload)
		if err != nil {
			t.Fatalf("failed to marshal result envelope: %v", err)
		}
		var payloadParsed map[string]interface{}
		if err := json.Unmarshal(payloadBytes, &payloadParsed); err != nil {
			t.Fatalf("failed to unmarshal serialized value: %v", err)
		}
		if _, exists := payloadParsed["payload"]; !exists {
			t.Error("payload must be serialized when non-empty")
		}

		// Action Result (UUID omitted)
		resAction := ResultEnvelope{
			Version:       "1.0",
			CorrelationID: "corr-action",
			Target:        "ap-1",
			CommandType:   "reboot",
			Result:        ResultSuccess,
			Message:       "rebooting",
			Timestamp:     "2023-10-01T12:00:00Z",
		}
		actionBytes, err := json.Marshal(resAction)
		if err != nil {
			t.Fatalf("failed to marshal action result envelope: %v", err)
		}
		var actionParsed map[string]interface{}
		if err := json.Unmarshal(actionBytes, &actionParsed); err != nil {
			t.Fatalf("failed to unmarshal serialized value: %v", err)
		}
		if _, exists := actionParsed["uuid"]; exists {
			t.Error("uuid must be omitted for action results")
		}
	})
}

func TestTC_CON_001_EnvelopeValidationBoundaries(t *testing.T) {
	// ActionCommand Validation
	invalidPayloadCmd := ActionCommand{
		Version:       "1.0",
		CorrelationID: "1",
		Target:        "ap",
		CommandType:   "reboot",
		Action:        "execute",
		Timestamp:     "time",
		Payload:       json.RawMessage(`{broken`),
	}
	if err := invalidPayloadCmd.Validate(); err == nil {
		t.Error("Expected error for invalid JSON payload in ActionCommand")
	}

	splitUpgradeCmd := ActionCommand{
		Version:       "1.0",
		CorrelationID: "1",
		Target:        "ap",
		CommandType:   "execute",
		Action:        "upgrade",
		Timestamp:     "time",
		Payload:       json.RawMessage(`{}`),
	}
	if err := splitUpgradeCmd.Validate(); err == nil {
		t.Error("Expected error for missing operation_id when action is upgrade")
	}

	// ResultEnvelope Validation
	invalidResultRes := ResultEnvelope{
		Version:       "1.0",
		CorrelationID: "1",
		Target:        "ap",
		CommandType:   "reboot",
		Result:        ResultType("unknown_typo"),
		Timestamp:     "time",
	}
	if err := invalidResultRes.Validate(); err == nil {
		t.Error("Expected error for invalid ResultType")
	}

	invalidPayloadRes := ResultEnvelope{
		Version:       "1.0",
		CorrelationID: "1",
		Target:        "ap",
		CommandType:   "reboot",
		Result:        ResultSuccess,
		Payload:       json.RawMessage(`{broken`),
		Timestamp:     "time",
	}
	if err := invalidPayloadRes.Validate(); err == nil {
		t.Error("Expected error for invalid JSON payload in ResultEnvelope")
	}

	missingOpIdUpgradeRes := ResultEnvelope{
		Version:       "1.0",
		CorrelationID: "1",
		Target:        "ap",
		CommandType:   CommandAction,
		Action:        ActionUpgrade,
		Result:        ResultSuccess,
		Timestamp:     "time",
	}
	if err := missingOpIdUpgradeRes.Validate(); err == nil {
		t.Error("Expected error for upgrade ResultEnvelope missing operation_id")
	}

	// ConfigureCommand Validation
	zeroUUIDCmd := ConfigureCommand{
		Version:       "1.0",
		CorrelationID: "1",
		Target:        "ap",
		UUID:          0,
		KVKey:         "cfg",
		KVRevision:    1,
		Timestamp:     "time",
	}
	if err := zeroUUIDCmd.Validate(); err == nil {
		t.Error("Expected error for UUID <= 0")
	}

	// Payload Validation tests
	emptyPayloadAction := ActionCommand{
		Version:       "1.0",
		CorrelationID: "1",
		Target:        "ap",
		CommandType:   CommandAction,
		Action:        ActionRTTY,
		Payload:       json.RawMessage(""),
		Timestamp:     "time",
	}
	if err := emptyPayloadAction.Validate(); err == nil {
		t.Error("Expected error for missing payload when one is required")
	}
	nullPayloadAction := emptyPayloadAction
	nullPayloadAction.Payload = json.RawMessage("null")
	if err := nullPayloadAction.Validate(); err == nil {
		t.Error("Expected error for null payload when one is required")
	}
	malformedPayloadAction := emptyPayloadAction
	malformedPayloadAction.Payload = json.RawMessage(`{"serial":"123", "method":"rtty"`) // missing brace
	if err := malformedPayloadAction.Validate(); err == nil {
		t.Error("Expected error for invalid json payload")
	}
	trailingPayloadAction := emptyPayloadAction
	trailingPayloadAction.Payload = json.RawMessage(`{"serial":"123", "method":"rtty", "token":"123", "id":"123", "server":"srv", "port":123} {"extra":"trailing"}`)
	if err := trailingPayloadAction.Validate(); err == nil {
		t.Error("Expected error for trailing json payload")
	} else if !strings.Contains(err.Error(), "trailing") {
		t.Errorf("Expected trailing json error, got: %v", err)
	}
	invalidRequestAction := emptyPayloadAction
	invalidRequestAction.Payload = json.RawMessage(`{"serial":"123", "method":"ssh"}`) // invalid method
	if err := invalidRequestAction.Validate(); err == nil {
		t.Error("Expected error from inner request Validate()")
	}
	validPayloadAction := emptyPayloadAction
	validPayloadAction.Payload = json.RawMessage(`{"serial":"123", "method":"rtty", "token":"123", "id":"123", "server":"srv", "port":123}`)
	if err := validPayloadAction.Validate(); err != nil {
		t.Errorf("Expected valid payload to pass, got: %v", err)
	}

	// Payload Bypasses Tests
	bypasses := []struct {
		Name    string
		Command CommandType
		Action  ActionType
	}{
		{"Upgrade with Action", CommandAction, ActionUpgrade},
		{"Upgrade with Command", CommandUpgrade, ""},
		{"Reboot with Action", CommandAction, ActionReboot},
		{"Reboot with Command", CommandReboot, ""},
		{"Script with Command", CommandScript, ""},
	}

	for _, tc := range bypasses {
		t.Run(tc.Name, func(t *testing.T) {
			cmd := ActionCommand{
				Version:       "1.0",
				CorrelationID: "corr-1",
				Target:        "ap-1",
				CommandType:   tc.Command,
				Action:        tc.Action,
				Payload:       json.RawMessage(`{}`), // empty object which misses mandatory fields
				Timestamp:     "2023-10-01T12:00:00Z",
			}
			if tc.Command == CommandUpgrade || tc.Action == ActionUpgrade {
				cmd.OperationID = "upg-1" // Provide valid operation ID to pass envelope validation
			}
			if err := cmd.Validate(); err == nil {
				t.Errorf("Expected {} payload to fail inner validation for %s / %s", tc.Command, tc.Action)
			}
		})
	}
}
