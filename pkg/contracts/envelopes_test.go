package contracts

import (
	"encoding/json"
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
		json.Unmarshal(b, &parsed)

		if parsed["version"] != "1.0" || parsed["correlation_id"] != "corr-1" {
			t.Errorf("Missing exact keys in ActionCommand serialization")
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
		json.Unmarshal(b, &parsed)

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
		json.Unmarshal(b, &parsed)

		if parsed["uuid"].(float64) != 999 {
			t.Errorf("UUID must be serialized for configure results")
		}
		if _, exists := parsed["operation_id"]; exists {
			t.Error("operation_id must be omitted when empty")
		}
		if _, exists := parsed["payload"]; exists {
			t.Error("payload must be omitted when empty")
		}
	})
}
