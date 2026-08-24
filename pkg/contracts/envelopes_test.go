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

func TestValidateResultPayload(t *testing.T) {
	tests := []struct {
		name    string
		command CommandType
		action  ActionType
		payload string
		wantErr bool
	}{
		{
			name:    "Configure Result - Valid",
			command: CommandConfigure,
			payload: `{"error": 0, "text": "Success"}`,
			wantErr: false,
		},
		{
			name:    "Configure Result - Missing Text",
			command: CommandConfigure,
			payload: `{}`,
			wantErr: true,
		},
		{
			name:    "Action Factory - Missing Text",
			command: CommandAction,
			action:  ActionFactory,
			payload: `{}`,
			wantErr: true,
		},
		{
			name:    "Action Upgrade - Missing Text",
			command: CommandAction,
			action:  ActionUpgrade,
			payload: `{}`,
			wantErr: true,
		},
		{
			name:    "Action Factory - Valid",
			command: CommandAction,
			action:  ActionFactory,
			payload: `{"error": 0, "text": "Success", "when": 1234567890}`,
			wantErr: false,
		},
		{
			name:    "Action Factory - Trailing JSON",
			command: CommandAction,
			action:  ActionFactory,
			payload: `{"error": 0, "text": "Success", "when": 1234567890}{"trailing": true}`,
			wantErr: true,
		},
		{
			name:    "Action Ping - Empty",
			command: CommandAction,
			action:  ActionPing,
			payload: ``,
			wantErr: false,
		},
		{
			name:    "Action Ping - Null",
			command: CommandAction,
			action:  ActionPing,
			payload: `null`,
			wantErr: false,
		},
		{
			name:    "Action Ping - Non-Empty",
			command: CommandAction,
			action:  ActionPing,
			payload: `{"error": 0}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateResultPayload(tt.command, tt.action, json.RawMessage(tt.payload))
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateResultPayload() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
