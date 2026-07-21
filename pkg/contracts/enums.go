package contracts

import (
	"errors"
	"fmt"
)

type ResultType string

const (
	ResultSuccess        ResultType = "success"
	ResultRejected       ResultType = "rejected"
	ResultFailed         ResultType = "failed"
	ResultTimeout        ResultType = "timeout"
	ResultRolledBack     ResultType = "rolled_back"
	ResultRollbackFailed ResultType = "rollback_failed"
	ResultStale          ResultType = "stale"
	ResultBusy           ResultType = "busy"
	ResultUnsupported    ResultType = "unsupported"
)

func (r ResultType) Valid() bool {
	switch r {
	case ResultSuccess,
		ResultRejected,
		ResultFailed,
		ResultTimeout,
		ResultRolledBack,
		ResultRollbackFailed,
		ResultStale,
		ResultBusy,
		ResultUnsupported:
		return true
	default:
		return false
	}
}

type CommandType string
type ActionType string
type ScriptType string
type RemoteAccessMethod string

const (
	CommandAction    CommandType = "action"
	CommandConfigure CommandType = "configure"
	CommandExecute   CommandType = "execute"
	CommandUpgrade   CommandType = "upgrade"
	CommandScript    CommandType = "script"
	CommandReboot    CommandType = "reboot"

	ActionUpgrade ActionType = "upgrade"
	ActionReboot  ActionType = "reboot"
	ActionExecute ActionType = "execute"

	ScriptTypeShell ScriptType = "shell"

	RemoteAccessRTTY RemoteAccessMethod = "rtty"
)

// RequireOperationID enforces that an operation ID is present for operations that require it (e.g., upgrade).
func RequireOperationID(operation string, operationID string) error {
	if operation == string(ActionUpgrade) && operationID == "" {
		return errors.New("operation_id is required for upgrade")
	}
	return nil
}

type ConnectionState string

const (
	StateConnecting      ConnectionState = "connecting"
	StateOperational     ConnectionState = "operational"
	StateCloudDegraded   ConnectionState = "cloud_degraded"
	StateNATSDegraded    ConnectionState = "nats_degraded"
	StateProtocolFailure ConnectionState = "protocol_failure"
)

type LinkState string

const (
	LinkConnecting LinkState = "connecting"
	LinkConnected  LinkState = "connected"
)

type ProtocolState string

const (
	ProtocolUnknown   ProtocolState = "unknown"
	ProtocolVerifying ProtocolState = "verifying"
	ProtocolAccepted  ProtocolState = "accepted"
	ProtocolRejected  ProtocolState = "rejected"
)

type ConnectionStatus struct {
	Cloud    LinkState
	NATS     LinkState
	Protocol ProtocolState
}

// DeriveConnectionState evaluates the pure derived status from the independent loops.
func DeriveConnectionState(cloud LinkState, nats LinkState, protocol ProtocolState) (ConnectionState, error) {
	if cloud != LinkConnecting && cloud != LinkConnected {
		return "", fmt.Errorf("invalid cloud state: %v", cloud)
	}
	if nats != LinkConnecting && nats != LinkConnected {
		return "", fmt.Errorf("invalid nats state: %v", nats)
	}
	if protocol != ProtocolUnknown && protocol != ProtocolVerifying && protocol != ProtocolAccepted && protocol != ProtocolRejected {
		return "", fmt.Errorf("invalid protocol state: %v", protocol)
	}

	if cloud == LinkConnecting && (protocol == ProtocolAccepted || protocol == ProtocolRejected) {
		return "", fmt.Errorf("impossible state: cloud is %v, protocol is %v", cloud, protocol)
	}
	if cloud == LinkConnected && (protocol == ProtocolUnknown || protocol == ProtocolVerifying) {
		return "", fmt.Errorf("impossible state: cloud is %v, protocol is %v", cloud, protocol)
	}

	if cloud == LinkConnecting {
		if nats == LinkConnecting {
			return StateConnecting, nil
		}
		if nats == LinkConnected {
			return StateCloudDegraded, nil
		}
	}

	if cloud == LinkConnected && protocol == ProtocolAccepted {
		if nats == LinkConnecting {
			return StateNATSDegraded, nil
		}
		if nats == LinkConnected {
			return StateOperational, nil
		}
	}

	if cloud == LinkConnected && protocol == ProtocolRejected {
		return StateProtocolFailure, nil
	}

	return "", fmt.Errorf("unrecognized state combination: cloud=%v, nats=%v, protocol=%v", cloud, nats, protocol)
}
