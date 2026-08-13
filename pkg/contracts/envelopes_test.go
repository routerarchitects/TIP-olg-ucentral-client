package contracts

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestValidateNATSTarget(t *testing.T) {
	tests := []struct {
		name    string
		target  string
		wantErr bool
	}{
		{"Valid target", "ap-123", false},
		{"Empty target", "", true},
		{"Whitespace target", "  ap-123  ", true},
		{"Wildcard *", "ap.*", true},
		{"Wildcard >", "ap.>", true},
		{"Internal space", "ap 123", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateNATSTarget(tt.target); (err != nil) != tt.wantErr {
				t.Errorf("ValidateNATSTarget() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateCommandPayload(t *testing.T) {
	t.Run("Empty Payload", func(t *testing.T) {
		err := ValidateCommandPayload(CommandAction, ActionRTTY, json.RawMessage(""))
		if err == nil {
			t.Error("Expected error for missing payload when one is required")
		}
	})

	t.Run("Null Payload", func(t *testing.T) {
		err := ValidateCommandPayload(CommandAction, ActionRTTY, json.RawMessage("null"))
		if err == nil {
			t.Error("Expected error for null payload when one is required")
		}
	})

	t.Run("Malformed JSON", func(t *testing.T) {
		err := ValidateCommandPayload(CommandAction, ActionRTTY, json.RawMessage(`{"serial":"123", "method":"rtty"`))
		if err == nil {
			t.Error("Expected error for invalid json payload")
		}
	})

	t.Run("Trailing JSON", func(t *testing.T) {
		err := ValidateCommandPayload(CommandAction, ActionRTTY, json.RawMessage(`{"serial":"123", "method":"rtty", "token":"123", "id":"123", "server":"srv", "port":123} {"extra":"trailing"}`))
		if err == nil {
			t.Error("Expected error for trailing json payload")
		} else if !strings.Contains(err.Error(), "trailing") {
			t.Errorf("Expected trailing json error, got: %v", err)
		}
	})

	t.Run("Invalid Inner Request", func(t *testing.T) {
		err := ValidateCommandPayload(CommandAction, ActionRTTY, json.RawMessage(`{"serial":"123", "method":"ssh"}`))
		if err == nil {
			t.Error("Expected error from inner request Validate()")
		}
	})

	t.Run("Valid Payload", func(t *testing.T) {
		err := ValidateCommandPayload(CommandAction, ActionRTTY, json.RawMessage(`{"serial":"123", "method":"rtty", "token":"123", "id":"123", "server":"srv", "port":123}`))
		if err != nil {
			t.Errorf("Expected valid payload to pass, got: %v", err)
		}
	})

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
			err := ValidateCommandPayload(tc.Command, tc.Action, json.RawMessage(`{}`)) // missing fields
			if err == nil {
				t.Errorf("Expected {} payload to fail inner validation for %s / %s", tc.Command, tc.Action)
			}
		})
	}

	// Query Payload Tests
	validQueries := []json.RawMessage{
		json.RawMessage(``),
		json.RawMessage(`null`),
		json.RawMessage(`{}`),
		json.RawMessage(`   {}   `),
	}
	for i, payload := range validQueries {
		if err := ValidateCommandPayload(CommandQuery, ActionStatusGet, payload); err != nil {
			t.Errorf("Expected valid query payload test %d to pass, got: %v", i, err)
		}
	}

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
		err := ValidateCommandPayload(CommandQuery, ActionStatusGet, tc.Payload)
		if err == nil {
			t.Errorf("Expected query payload test %d (%q) to fail with %q", i, string(tc.Payload), tc.Error)
		} else if !strings.Contains(err.Error(), tc.Error) {
			t.Errorf("Expected error containing %q, got: %v", tc.Error, err)
		}
	}
}

func TestValidateCommandPayload_Configure(t *testing.T) {
	err := ValidateCommandPayload(CommandConfigure, "", json.RawMessage(`{}`))
	if err == nil {
		t.Error("Expected error for missing fields in configure payload")
	}

	err = ValidateCommandPayload(CommandConfigure, "", json.RawMessage(`{"serial": "123", "uuid": 1, "config": {}}`))
	if err != nil {
		t.Errorf("Expected valid configure payload to pass, got: %v", err)
	}
}
