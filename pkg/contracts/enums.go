package contracts

import (
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
	CommandQuery     CommandType = "query"

	ActionUpgrade         ActionType = "upgrade"
	ActionReboot          ActionType = "reboot"
	ActionExecute         ActionType = "execute"
	ActionFactory         ActionType = "factory"
	ActionCertupdate      ActionType = "certupdate"
	ActionReenroll        ActionType = "reenroll"
	ActionRTTY            ActionType = "rtty"
	ActionLeds            ActionType = "leds"
	ActionTrace           ActionType = "trace"
	ActionPing            ActionType = "ping"
	ActionTelemetry       ActionType = "telemetry"
	ActionCapabilitiesGet ActionType = "capabilities.get"
	ActionStatusGet       ActionType = "status.get"

	ScriptTypeShell  ScriptType = "shell"
	ScriptTypeUcode  ScriptType = "ucode"
	ScriptTypeBundle ScriptType = "bundle"

	RemoteAccessRTTY RemoteAccessMethod = "rtty"
)

func (c CommandType) Valid() bool {
	switch c {
	case CommandAction, CommandConfigure, CommandExecute, CommandUpgrade, CommandScript, CommandReboot, CommandQuery:
		return true
	default:
		return false
	}
}

func (a ActionType) Valid() bool {
	switch a {
	case ActionUpgrade, ActionReboot, ActionExecute, ActionFactory, ActionCertupdate, ActionReenroll, ActionRTTY, ActionLeds, ActionTrace, ActionPing, ActionTelemetry, ActionCapabilitiesGet, ActionStatusGet:
		return true
	default:
		return false
	}
}

// ValidCommandAction explicitly defines the allowed matrix of CommandType and ActionType combinations.
func ValidCommandAction(command CommandType, action ActionType) bool {
	// If the envelope requires an action, it must be a valid ActionType.
	if action != "" && !action.Valid() {
		return false
	}
	if !command.Valid() {
		return false
	}

	switch command {
	case CommandAction:
		// Generic transport commands can carry any valid operational action except queries
		return action.Valid() && action != ActionCapabilitiesGet && action != ActionStatusGet
	case CommandExecute:
		return action == ActionExecute
	case CommandUpgrade:
		return action == ActionUpgrade || action == ""
	case CommandReboot:
		return action == ActionReboot || action == ""
	case CommandConfigure, CommandScript:
		return action == ""
	case CommandQuery:
		return action == ActionCapabilitiesGet || action == ActionStatusGet
	default:
		return false
	}
}

type ConnectionState string

const (
	StateConnecting    ConnectionState = "connecting"
	StateOperational   ConnectionState = "operational"
	StateCloudDegraded ConnectionState = "cloud_degraded"
	StateNATSDegraded  ConnectionState = "nats_degraded"
)

type LinkState string

const (
	LinkConnecting LinkState = "connecting"
	LinkConnected  LinkState = "connected"
)

type ConnectionStatus struct {
	Cloud LinkState
	NATS  LinkState
}

// DeriveConnectionState evaluates the pure derived status from the independent loops.
func DeriveConnectionState(cloud LinkState, nats LinkState) (ConnectionState, error) {
	if cloud != LinkConnecting && cloud != LinkConnected {
		return "", fmt.Errorf("invalid cloud state: %v", cloud)
	}
	if nats != LinkConnecting && nats != LinkConnected {
		return "", fmt.Errorf("invalid nats state: %v", nats)
	}

	if cloud == LinkConnecting {
		if nats == LinkConnecting {
			return StateConnecting, nil
		}
		if nats == LinkConnected {
			return StateCloudDegraded, nil
		}
	}

	if cloud == LinkConnected {
		if nats == LinkConnecting {
			return StateNATSDegraded, nil
		}
		if nats == LinkConnected {
			return StateOperational, nil
		}
	}

	return "", fmt.Errorf("unrecognized state combination: cloud=%v, nats=%v", cloud, nats)
}
