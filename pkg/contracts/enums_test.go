package contracts

import (
	"testing"
)

func TestTC_CON_003_VersionVerificationFallbackAndProtocolState(t *testing.T) {
	tests := []struct {
		name      string
		cloud     LinkState
		nats      LinkState
		wantState ConnectionState
		wantErr   bool
	}{
		{"Connecting/Connecting", LinkConnecting, LinkConnecting, StateConnecting, false},
		{"Connecting/Connected", LinkConnecting, LinkConnected, StateCloudDegraded, false},
		{"Connected/Connecting", LinkConnected, LinkConnecting, StateNATSDegraded, false},
		{"Connected/Connected", LinkConnected, LinkConnected, StateOperational, false},

		{
			name:    "Invalid cloud enum",
			cloud:   LinkState("invalid"),
			nats:    LinkConnected,
			wantErr: true,
		},
		{
			name:    "Invalid NATS enum",
			cloud:   LinkConnected,
			nats:    LinkState("invalid"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DeriveConnectionState(tt.cloud, tt.nats)
			if (err != nil) != tt.wantErr {
				t.Errorf("DeriveConnectionState() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.wantState {
				t.Errorf("DeriveConnectionState() = %v, want %v", got, tt.wantState)
			}
		})
	}
}

func TestValidCommandAction(t *testing.T) {
	tests := []struct {
		name    string
		command CommandType
		action  ActionType
		valid   bool
	}{
		// Generic transport commands
		{"Action with Upgrade", CommandAction, ActionUpgrade, true},
		{"Action with Reboot", CommandAction, ActionReboot, true},
		{"Action with Execute", CommandAction, ActionExecute, true},
		{"Execute with Execute", CommandExecute, ActionExecute, true},
		{"Action with Factory", CommandAction, ActionFactory, true},
		{"Action with Certupdate", CommandAction, ActionCertupdate, true},
		{"Action with Reenroll", CommandAction, ActionReenroll, true},
		{"Action with RTTY", CommandAction, ActionRTTY, true},
		{"Action with Leds", CommandAction, ActionLeds, true},
		{"Action with Trace", CommandAction, ActionTrace, true},
		{"Action with Ping", CommandAction, ActionPing, true},
		{"Action with Telemetry", CommandAction, ActionTelemetry, true},

		// Direct commands
		{"Upgrade with Upgrade", CommandUpgrade, ActionUpgrade, true},
		{"Upgrade with empty", CommandUpgrade, "", false},
		{"Reboot with Reboot", CommandReboot, ActionReboot, true},
		{"Reboot with empty", CommandReboot, "", false},
		{"Configure with empty", CommandConfigure, "", true},
		{"Script with empty", CommandScript, "", false},

		// Invalid combinations
		{"Reboot with Upgrade", CommandReboot, ActionUpgrade, false},
		{"Upgrade with Reboot", CommandUpgrade, ActionReboot, false},
		{"Configure with Action", CommandConfigure, ActionReboot, false},
		{"Script with Execute", CommandScript, ActionExecute, true},
		{"Action with empty", CommandAction, "", false},
		{"Execute with Upgrade", CommandExecute, ActionUpgrade, false},
		{"Execute with Reboot", CommandExecute, ActionReboot, false},

		// Invalid enums
		{"Invalid command", CommandType("unknown"), ActionUpgrade, false},
		{"Invalid action", CommandAction, ActionType("unknown"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ValidCommandAction(tt.command, tt.action)
			if got != tt.valid {
				t.Errorf("ValidCommandAction(%q, %q) = %v, want %v", tt.command, tt.action, got, tt.valid)
			}
		})
	}
}
