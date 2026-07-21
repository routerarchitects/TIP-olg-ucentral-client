package contracts

import (
	"testing"
)

func TestTC_CON_003_VersionVerificationFallbackAndProtocolState(t *testing.T) {
	tests := []struct {
		name      string
		cloud     LinkState
		nats      LinkState
		protocol  ProtocolState
		wantState ConnectionState
		wantErr   bool
	}{
		{"Connecting/Connecting/Verifying", LinkConnecting, LinkConnecting, ProtocolVerifying, StateConnecting, false},
		{"Connecting/Connected/Verifying", LinkConnecting, LinkConnected, ProtocolVerifying, StateCloudDegraded, false},
		{"Connected/Connecting/Accepted", LinkConnected, LinkConnecting, ProtocolAccepted, StateNATSDegraded, false},
		{"Connected/Connected/Accepted", LinkConnected, LinkConnected, ProtocolAccepted, StateOperational, false},
		{"Connected/Connecting/Rejected", LinkConnected, LinkConnecting, ProtocolRejected, StateProtocolFailure, false},
		{"Connected/Connected/Rejected", LinkConnected, LinkConnected, ProtocolRejected, StateProtocolFailure, false},

		// Impossible combinations
		{"Connecting with Protocol Accepted", LinkConnecting, LinkConnected, ProtocolAccepted, "", true},
		{"Connecting with Protocol Rejected", LinkConnecting, LinkConnecting, ProtocolRejected, "", true},
		{"Connected with Protocol Verifying", LinkConnected, LinkConnected, ProtocolVerifying, "", true},
		{"Connected with Protocol Unknown", LinkConnected, LinkConnecting, ProtocolUnknown, "", true},
		{
			name:     "Invalid cloud enum",
			cloud:    LinkState("invalid"),
			nats:     LinkConnected,
			protocol: ProtocolAccepted,
			wantErr:  true,
		},
		{
			name:     "Invalid NATS enum",
			cloud:    LinkConnected,
			nats:     LinkState("invalid"),
			protocol: ProtocolAccepted,
			wantErr:  true,
		},
		{
			name:     "Invalid protocol enum",
			cloud:    LinkConnected,
			nats:     LinkConnected,
			protocol: ProtocolState("invalid"),
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DeriveConnectionState(tt.cloud, tt.nats, tt.protocol)
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
		{"Execute with Upgrade", CommandExecute, ActionUpgrade, true},
		{"Execute with Reboot", CommandExecute, ActionReboot, true},
		{"Execute with Execute", CommandExecute, ActionExecute, true},

		// Direct commands
		{"Upgrade with Upgrade", CommandUpgrade, ActionUpgrade, true},
		{"Upgrade with empty", CommandUpgrade, "", true},
		{"Reboot with Reboot", CommandReboot, ActionReboot, true},
		{"Reboot with empty", CommandReboot, "", true},
		{"Configure with empty", CommandConfigure, "", true},
		{"Script with empty", CommandScript, "", true},

		// Invalid combinations
		{"Reboot with Upgrade", CommandReboot, ActionUpgrade, false},
		{"Upgrade with Reboot", CommandUpgrade, ActionReboot, false},
		{"Configure with Action", CommandConfigure, ActionReboot, false},
		{"Script with Execute", CommandScript, ActionExecute, false},
		{"Action with empty", CommandAction, "", false},

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
