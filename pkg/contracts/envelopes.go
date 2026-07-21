package contracts

type ConfigureCommand struct {
	UUID   uint64         `json:"uuid"`
	Config map[string]any `json:"config"`
}

type ActionCommand struct {
	Action  string         `json:"action"`
	Payload map[string]any `json:"payload,omitempty"`
	When    uint64         `json:"when"`
}

type CommandResult struct {
	Status    *ResultType    `json:"status"`
	ErrorCode int            `json:"error_code"`
	ErrorText string         `json:"error_text"`
	Payload   map[string]any `json:"payload,omitempty"`
}

type StateTelemetry struct {
	State map[string]any `json:"state"`
}

type CloudDeviceStatusQuery struct {
	Version       string `json:"version"`
	CorrelationID string `json:"correlation_id"`
	OperationID   string `json:"operation_id,omitempty"`
	Target        string `json:"target"`
	CommandType   string `json:"command_type"`
	Action        string `json:"action"`
	Timestamp     string `json:"timestamp"`
}
