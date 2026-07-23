package contracts

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Telecominfraproject/olg-nats-agent-core/agentcore"
)

func TestTC_CON_001_EnvelopeSerialization(t *testing.T) {
	t.Run("ActionCommand Payload Handling", func(t *testing.T) {
		// Valid ActionCommand
		validAction := agentcore.ActionCommand{
			Version:     "1.0",
			RPCID:       "corr-1",
			Target:      "ap-1",
			CommandType: "reboot",
			Action:      "execute",
			Payload:     json.RawMessage(`{"delay": 5}`),
			Timestamp:   time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC),
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
		expectedKeys := []string{"version", "rpc_id", "target", "command_type", "action", "payload", "timestamp"}
		for _, key := range expectedKeys {
			if _, exists := parsed[key]; !exists {
				t.Errorf("ActionCommand missing key: %s", key)
			}
		}

		// Valid Upgrade Action with operation_id
		validUpgrade := agentcore.ActionCommand{
			Version:     "1.0",
			RPCID:       "corr-upgrade",
			Target:      "ap-1",
			CommandType: "upgrade",
			Action:      "upgrade",
			Payload:     json.RawMessage(`{}`),
			Timestamp:   time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC),
		}

		upgradeBytes, err := json.Marshal(validUpgrade)
		if err != nil {
			t.Fatalf("failed to marshal valid upgrade: %v", err)
		}
		var upgradeParsed map[string]interface{}
		if err := json.Unmarshal(upgradeBytes, &upgradeParsed); err != nil {
			t.Fatalf("failed to unmarshal serialized value: %v", err)
		}

		
	})

	t.Run("ConfigureCommand Serialization", func(t *testing.T) {
		cmd := agentcore.ConfigureNotification{
			Version:    "1.0",
			RPCID:      "corr-1",
			Target:     "ap-1",
			UUID:       "12345",
			KVKey:      "cfg",
			KVBucket:   "cfg",
			Timestamp:  time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC),
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
		if parsed["uuid"].(string) != "12345" {
			t.Errorf("UUID was not serialized correctly: %v", parsed["uuid"])
		}
	})

	t.Run("ResultEnvelope Serialization", func(t *testing.T) {
		res := agentcore.ResultEnvelope{
			Version:     "1.0",
			RPCID:       "corr-1",
			Target:      "ap-1",
			CommandType: "configure",
			UUID:        "999",
			Result:      "success",
			Message:     "OK",
			Timestamp:   time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC),
		}

		b, err := json.Marshal(res)
		if err != nil {
			t.Fatalf("Failed to marshal ResultEnvelope: %v", err)
		}

		var parsed map[string]interface{}
		if err := json.Unmarshal(b, &parsed); err != nil {
			t.Fatalf("failed to unmarshal serialized value: %v", err)
		}

		if parsed["uuid"].(string) != "999" {
			t.Errorf("UUID must be serialized for configure results")
		}

		if _, exists := parsed["payload"]; exists {
			t.Error("payload must be omitted when empty")
		}

		// Non-empty payload test
		resWithPayload := agentcore.ResultEnvelope{
			Version:     "1.0",
			RPCID:       "corr-script",
			Target:      "ap-1",
			CommandType: "script",
			Result:      "success",
			Message:     "completed",
			Payload:     json.RawMessage(`{"result_64":"YWJj"}`),
			Timestamp:   time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC),
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
		resAction := agentcore.ResultEnvelope{
			Version:     "1.0",
			RPCID:       "corr-action",
			Target:      "ap-1",
			CommandType: "reboot",
			Result:      "success",
			Message:     "rebooting",
			Timestamp:   time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC),
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
	invalidPayloadCmd := agentcore.ActionCommand{
		Version:     "1.0",
		RPCID:       "1",
		Target:      "ap",
		CommandType: "reboot",
		Action:      "execute",
		Timestamp:   time.Now(),
		Payload:     json.RawMessage(`{broken`),
	}
	if err := ValidateActionCommand(&invalidPayloadCmd); err == nil {
		t.Error("Expected error for invalid JSON payload in ActionCommand")
	}


	// ResultEnvelope Validation
	missingCommandTypeRes := agentcore.ResultEnvelope{
		Version:   "1.0",
		RPCID:     "1",
		Target:    "ap",
		Result:    "success",
		Timestamp: time.Now(),
	}
	if err := ValidateResultEnvelope(&missingCommandTypeRes); err == nil {
		t.Error("Expected error for missing command_type in ResultEnvelope")
	}

	invalidCommandTypeRes := agentcore.ResultEnvelope{
		Version:     "1.0",
		RPCID:       "1",
		Target:      "ap",
		CommandType: "unknown_cmd",
		Result:      "success",
		Timestamp:   time.Now(),
	}
	if err := ValidateResultEnvelope(&invalidCommandTypeRes); err == nil {
		t.Error("Expected error for invalid command_type in ResultEnvelope")
	}
	invalidResultRes := agentcore.ResultEnvelope{
		Version:     "1.0",
		RPCID:       "1",
		Target:      "ap",
		CommandType: "reboot",
		Result:      "unknown_typo",
		Timestamp:   time.Now(),
	}
	if err := ValidateResultEnvelope(&invalidResultRes); err == nil {
		t.Error("Expected error for invalid ResultType")
	}

	invalidPayloadRes := agentcore.ResultEnvelope{
		Version:     "1.0",
		RPCID:       "1",
		Target:      "ap",
		CommandType: "reboot",
		Result:      "success",
		Payload:     json.RawMessage(`{broken`),
		Timestamp:   time.Now(),
	}
	if err := ValidateResultEnvelope(&invalidPayloadRes); err == nil {
		t.Error("Expected error for invalid JSON payload in ResultEnvelope")
	}

	
	

	// ConfigureCommand Validation
	zeroUUIDCmd := agentcore.ConfigureNotification{
		Version:    "1.0",
		RPCID:      "1",
		Target:     "ap",
		UUID:       "",
		KVKey:      "cfg",
		KVBucket:   "cfg",
		Timestamp:  time.Now(),
	}
	if err := ValidateConfigureNotification(&zeroUUIDCmd); err == nil {
		t.Error("Expected error for missing UUID")
	}


	// Payload Validation tests
	emptyPayloadAction := agentcore.ActionCommand{
		Version:     "1.0",
		RPCID:       "1",
		Target:      "ap",
		CommandType: "action",
		Action: "rtty",
		Payload:     json.RawMessage(""),
		Timestamp:   time.Now(),
	}
	if err := ValidateActionCommand(&emptyPayloadAction); err == nil {
		t.Error("Expected error for missing payload when one is required")
	}
	nullPayloadAction := emptyPayloadAction
	nullPayloadAction.Payload = json.RawMessage("null")
	if err := ValidateActionCommand(&nullPayloadAction); err == nil {
		t.Error("Expected error for null payload when one is required")
	}
	malformedPayloadAction := emptyPayloadAction
	malformedPayloadAction.Payload = json.RawMessage(`{"serial":"123", "method":"rtty"`) // missing brace
	if err := ValidateActionCommand(&malformedPayloadAction); err == nil {
		t.Error("Expected error for invalid json payload")
	}
	trailingPayloadAction := emptyPayloadAction
	trailingPayloadAction.Payload = json.RawMessage(`{"serial":"123", "method":"rtty", "token":"123", "id":"123", "server":"srv", "port":123} {"extra":"trailing"}`)
	if err := ValidateActionCommand(&trailingPayloadAction); err == nil {
		t.Error("Expected error for trailing json payload")
	} else if !strings.Contains(err.Error(), "trailing") {
		t.Errorf("Expected trailing json error, got: %v", err)
	}
	invalidRequestAction := emptyPayloadAction
	invalidRequestAction.Payload = json.RawMessage(`{"serial":"123", "method":"ssh"}`) // invalid method
	if err := ValidateActionCommand(&invalidRequestAction); err == nil {
		t.Error("Expected error from inner request Validate()")
	}
	validPayloadAction := emptyPayloadAction
	validPayloadAction.Payload = json.RawMessage(`{"serial":"123", "method":"rtty", "token":"123", "id":"123", "server":"srv", "port":123}`)
	if err := ValidateActionCommand(&validPayloadAction); err != nil {
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
			cmd := agentcore.ActionCommand{
				Version:     "1.0",
				RPCID:       "corr-1",
				Target:      "ap-1",
				CommandType: string(tc.Command),
				Action:      string(tc.Action),
				Payload:     json.RawMessage(`{}`), // empty object which misses mandatory fields
				Timestamp:   time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC),
			}
			if tc.Command == CommandUpgrade || tc.Action == ActionUpgrade {
			}
			if err := ValidateActionCommand(&cmd); err == nil {
				t.Errorf("Expected {} payload to fail inner validation for %s / %s", tc.Command, tc.Action)
			}
		})
	}

	// Query Payload Tests
	queryCmdTemplate := agentcore.ActionCommand{
		Version:     "1.0",
		RPCID:       "corr-1",
		Target:      "ap-1",
		CommandType: "query",
		Action: "status.get",
		Timestamp:   time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC),
	}

	// Valid queries
	validQueries := []json.RawMessage{
		json.RawMessage(``),
		json.RawMessage(`null`),
		json.RawMessage(`{}`),
		json.RawMessage(`   {}   `),
	}
	for i, payload := range validQueries {
		cmd := queryCmdTemplate
		cmd.Payload = payload
		if err := ValidateActionCommand(&cmd); err != nil {
			t.Errorf("Expected valid query payload test %d to pass, got: %v", i, err)
		}
	}

	// Invalid queries
	invalidQueries := []struct {
		Payload json.RawMessage
		Error   string
	}{
		{json.RawMessage(`{broken`), "invalid JSON"},
		{json.RawMessage(`"string"`), "JSON object"},
		{json.RawMessage(`[]`), "JSON object"},
		{json.RawMessage(`{"unexpected":true}`), "must be empty"},
	}
	for i, tc := range invalidQueries {
		cmd := queryCmdTemplate
		cmd.Payload = tc.Payload
		if err := ValidateActionCommand(&cmd); err == nil {
			t.Errorf("Expected query payload test %d (%q) to fail with %q", i, string(tc.Payload), tc.Error)
		} else if !strings.Contains(err.Error(), tc.Error) {
			t.Errorf("Expected error containing %q, got: %v", tc.Error, err)
		}
	}
}
