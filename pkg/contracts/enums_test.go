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
