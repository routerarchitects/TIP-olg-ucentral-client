package contracts

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

type ConnectionState string

const (
	StateCloudDegraded   ConnectionState = "StateCloudDegraded"
	StateNATSDegraded    ConnectionState = "StateNATSDegraded"
	StateOperational     ConnectionState = "StateOperational"
	StateProtocolFailure ConnectionState = "StateProtocolFailure"
)

type LinkState int

const (
	LinkUnknown    LinkState = 0
	LinkConnecting LinkState = 1
	LinkConnected  LinkState = 2
)

type ProtocolState int

const (
	ProtocolUnknown   ProtocolState = 0
	ProtocolVerifying ProtocolState = 1
	ProtocolAccepted  ProtocolState = 2
	ProtocolRejected  ProtocolState = 3
)
