package contracts

import "encoding/json"

// Standard JSON-RPC 2.0 Error Codes
const (
	ErrParse          = -32700
	ErrInvalidRequest = -32600
	ErrMethodNotFound = -32601
	ErrInvalidParams  = -32602
	ErrInternal       = -32603 // Maps to Internal / Busy
)

// Application Sub-codes (returned in JSON-RPC error.data.application_code)
const (
	ErrAppFailure         = 1
	ErrTimeout            = 2
	ErrServiceUnavailable = 3
	ErrValidationFailed   = 4
	ErrRollbackSuccess    = 5
	ErrRollbackFailed     = 6
)

type JSONRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      json.RawMessage `json:"id,omitempty"`
}

type JSONRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *JSONRPCError   `json:"error,omitempty"`
	ID      json.RawMessage `json:"id,omitempty"`
}

type JSONRPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

type CloudCompressedConfigureRequest struct {
	Compress64 string `json:"compress_64"`
	CompressSz uint32 `json:"compress_sz"`
}

type CloudConfigureRequest struct {
	Serial string          `json:"serial"`
	UUID   int64           `json:"uuid"`
	When   int64           `json:"when,omitempty"`
	Config json.RawMessage `json:"config"`
}

type ConfigureRejectedParameter struct {
	Parameter    json.RawMessage `json:"parameter"`
	Reason       string          `json:"reason"`
	Substitution json.RawMessage `json:"substitution,omitempty"`
}

type CloudConfigureResultStatus struct {
	Error    int                          `json:"error"`
	Text     string                       `json:"text"`
	When     int64                        `json:"when,omitempty"`
	Rejected []ConfigureRejectedParameter `json:"rejected,omitempty"`
}

type CloudConfigureResponse struct {
	Serial string                     `json:"serial"`
	UUID   int64                      `json:"uuid"`
	Status CloudConfigureResultStatus `json:"status"`
}

type CloudRebootRequest struct {
	Serial string `json:"serial"`
	When   int64  `json:"when,omitempty"`
}

type CloudRebootStatus struct {
	Error int    `json:"error"`
	Text  string `json:"text"`
	When  int64  `json:"when"`
}

type CloudRebootResponse struct {
	Serial string            `json:"serial"`
	Status CloudRebootStatus `json:"status"`
}

type CloudFactoryRequest struct {
	Serial         string `json:"serial"`
	KeepRedirector *int   `json:"keep_redirector"`
	When           int64  `json:"when,omitempty"`
}

type CloudFactoryStatus struct {
	Error int    `json:"error"`
	Text  string `json:"text"`
	When  int64  `json:"when"`
}

type CloudFactoryResponse struct {
	Serial string             `json:"serial"`
	Status CloudFactoryStatus `json:"status"`
}

type CloudUpgradeRequest struct {
	Serial      string `json:"serial"`
	URI         string `json:"uri"`
	FWsignature string `json:"FWsignature,omitempty"`
	When        int64  `json:"when,omitempty"`
}

type CloudUpgradeStatus struct {
	Error int    `json:"error"`
	Text  string `json:"text"`
	When  int64  `json:"when"`
}

type CloudUpgradeResponse struct {
	Serial string             `json:"serial"`
	Status CloudUpgradeStatus `json:"status"`
}

type CloudUpgradeProgressNotification struct {
	JSONRPC string                                 `json:"jsonrpc"`
	Method  string                                 `json:"method"`
	Params  CloudUpgradeProgressNotificationParams `json:"params"`
}

type CloudUpgradeProgressNotificationParams struct {
	Serial      string          `json:"serial"`
	ID          json.RawMessage `json:"id"`
	OperationID string          `json:"operation_id"`
	Stage       string          `json:"stage"`
	Status      string          `json:"status"`
	Message     string          `json:"message"`
}

type CloudTraceRequest struct {
	Serial    string `json:"serial"`
	When      int64  `json:"when,omitempty"`
	Duration  *int   `json:"duration,omitempty"`
	Packets   *int   `json:"packets,omitempty"`
	Network   string `json:"network,omitempty"`
	Interface string `json:"interface,omitempty"`
	URI       string `json:"uri,omitempty"`
}

type CloudTraceStatus struct {
	Error int    `json:"error"`
	Text  string `json:"text"`
	When  int64  `json:"when,omitempty"`
}

type CloudTraceResponse struct {
	Serial string           `json:"serial"`
	Status CloudTraceStatus `json:"status"`
}

type CloudPingRequest struct {
	Serial string `json:"serial"`
}

type CloudPingResponse struct {
	Serial        string `json:"serial"`
	UUID          int64  `json:"uuid"`
	DeviceUTCTime int64  `json:"deviceUTCTime"`
}

type CloudLedsRequest struct {
	Serial   string `json:"serial"`
	When     int64  `json:"when,omitempty"`
	Duration *int   `json:"duration,omitempty"`
	Pattern  string `json:"pattern"`
}

type CloudLedsStatus struct {
	Error int    `json:"error"`
	Text  string `json:"text"`
}

type CloudLedsResponse struct {
	Serial string          `json:"serial"`
	Status CloudLedsStatus `json:"status"`
}

type CloudTelemetryRequest struct {
	Serial   string   `json:"serial"`
	Interval *int     `json:"interval,omitempty"`
	Types    []string `json:"types,omitempty"`
}

type CloudTelemetryStatus struct {
	Error int    `json:"error"`
	Text  string `json:"text"`
}

type CloudTelemetryResponse struct {
	Serial string               `json:"serial"`
	Status CloudTelemetryStatus `json:"status"`
}

type CloudTelemetryEvent struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  struct {
		Serial string          `json:"serial"`
		Data   json.RawMessage `json:"data"`
	} `json:"params"`
}

type CloudRemoteAccessRequest struct {
	Method  string `json:"method,omitempty"`
	Serial  string `json:"serial"`
	Token   string `json:"token"`
	ID      string `json:"id"`
	Server  string `json:"server"`
	Port    int    `json:"port"`
	User    string `json:"user,omitempty"`
	Timeout *int   `json:"timeout,omitempty"`
}

type CloudRemoteAccessStatus struct {
	Error int             `json:"error"`
	Text  string          `json:"text"`
	Meta  json.RawMessage `json:"meta,omitempty"`
}

type CloudRemoteAccessResponse struct {
	Serial string                  `json:"serial"`
	Status CloudRemoteAccessStatus `json:"status"`
}

type CloudCertupdateRequest struct {
	Serial       string `json:"serial"`
	Certificates string `json:"certificates"`
}

type CloudCertupdateStatus struct {
	Error int    `json:"error"`
	Txt   string `json:"txt"`
}

type CloudCertupdateResponse struct {
	Serial string                `json:"serial"`
	Status CloudCertupdateStatus `json:"status"`
}

type CloudReenrollRequest struct {
	Serial string `json:"serial"`
	When   int64  `json:"when,omitempty"`
}

type CloudReenrollStatus struct {
	Error int    `json:"error"`
	Txt   string `json:"txt"`
}

type CloudReenrollResponse struct {
	Serial string              `json:"serial"`
	Status CloudReenrollStatus `json:"status"`
}

type CloudScriptRequest struct {
	Serial    string `json:"serial"`
	Type      string `json:"type"`
	Script    string `json:"script,omitempty"`
	Timeout   *int   `json:"timeout,omitempty"`
	URI       string `json:"uri,omitempty"`
	Signature string `json:"signature,omitempty"`
	When      int64  `json:"when,omitempty"`
}

type CloudScriptStatus struct {
	Error    int    `json:"error"`
	Result64 string `json:"result_64,omitempty"`
	ResultSz *int   `json:"result_sz,omitempty"`
	Result   string `json:"result,omitempty"`
}

type CloudScriptResponse struct {
	Serial string            `json:"serial"`
	Status CloudScriptStatus `json:"status"`
}
