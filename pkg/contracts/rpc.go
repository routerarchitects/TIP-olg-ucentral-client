package contracts

import (
	"bytes"
	"compress/zlib"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
)

type Validatable interface {
	Validate() error
}

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

// NewInternalJSONRPCError creates a JSONRPCError struct matching the given internal application code.
func NewInternalJSONRPCError(appCode int, message string) (*JSONRPCError, error) {
	switch appCode {
	case ErrAppFailure,
		ErrTimeout,
		ErrServiceUnavailable,
		ErrValidationFailed,
		ErrRollbackSuccess,
		ErrRollbackFailed:
	default:
		return nil, fmt.Errorf("unsupported application code: %d", appCode)
	}

	dataBytes, err := json.Marshal(map[string]int{"application_code": appCode})
	if err != nil {
		return nil, fmt.Errorf("marshal JSON-RPC error data: %w", err)
	}

	return &JSONRPCError{
		Code:    ErrInternal,
		Message: message,
		Data:    dataBytes,
	}, nil
}

type CloudCompressedConfigureRequest struct {
	Compress64 string `json:"compress_64"`
	CompressSz uint32 `json:"compress_sz"`
}

func (r *CloudCompressedConfigureRequest) DecodeAndValidate() (*CloudConfigureRequest, error) {
	if r.Compress64 == "" {
		return nil, errors.New("compress_64 is required")
	}
	if r.CompressSz == 0 {
		return nil, errors.New("compress_sz must be greater than zero")
	}
	if r.CompressSz > 10*1024*1024 {
		return nil, errors.New("compress_sz exceeds 10 MB limit")
	}

	decoder := base64.NewDecoder(base64.StdEncoding, strings.NewReader(r.Compress64))

	zlibReader, err := zlib.NewReader(decoder)
	if err != nil {
		return nil, fmt.Errorf("invalid zlib data: %w", err)
	}
	defer zlibReader.Close()

	limitReader := io.LimitReader(zlibReader, int64(r.CompressSz)+1)

	bytesRead, err := io.ReadAll(limitReader)
	if err != nil {
		return nil, fmt.Errorf("decompression error: %w", err)
	}

	if uint32(len(bytesRead)) != r.CompressSz {
		return nil, errors.New("decompressed size does not match compress_sz")
	}

	var req CloudConfigureRequest
	if err := json.Unmarshal(bytesRead, &req); err != nil {
		return nil, fmt.Errorf("invalid JSON in decompressed data: %w", err)
	}

	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("invalid decompressed configure request: %w", err)
	}

	return &req, nil
}

type CloudConfigureRequest struct {
	Serial string          `json:"serial"`
	UUID   int64           `json:"uuid"`
	When   int64           `json:"when,omitempty"`
	Config json.RawMessage `json:"config"`
}

func (r *CloudConfigureRequest) Validate() error {
	if r.Serial == "" {
		return errors.New("serial is required")
	}
	if r.UUID <= 0 {
		return errors.New("uuid must be greater than zero")
	}
	if r.When != 0 {
		return errors.New("when must be zero for configure")
	}
	if len(r.Config) == 0 || !json.Valid(r.Config) {
		return errors.New("config must contain valid JSON")
	}
	return nil
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

func (r *CloudRebootRequest) Validate() error {
	if r.Serial == "" {
		return errors.New("serial is required")
	}
	if r.When != 0 {
		return errors.New("when must be zero for reboot")
	}
	return nil
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

// Validate enforces the factory request constraints.
func (r *CloudFactoryRequest) Validate() error {
	if r.Serial == "" {
		return errors.New("serial is required")
	}
	if r.KeepRedirector == nil {
		return errors.New("missing keep_redirector")
	}
	if *r.KeepRedirector != 0 && *r.KeepRedirector != 1 {
		return fmt.Errorf("invalid keep_redirector: %d", *r.KeepRedirector)
	}
	if r.When != 0 {
		return errors.New("when must be zero for factory")
	}
	return nil
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

func (r *CloudUpgradeRequest) Validate() error {
	if r.Serial == "" {
		return errors.New("serial is required")
	}
	if r.URI == "" {
		return errors.New("uri is required")
	}
	u, err := url.ParseRequestURI(r.URI)
	if err != nil || u.Host == "" {
		return errors.New("invalid upgrade URI")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("upgrade uri scheme must be http or https, got %q", u.Scheme)
	}
	if r.When != 0 {
		return errors.New("when must be zero for upgrade")
	}
	return nil
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

func (r *CloudTraceRequest) Validate() error {
	if r.Serial == "" {
		return errors.New("serial is required")
	}
	if r.When != 0 {
		return errors.New("when must be zero for trace")
	}
	return nil
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

func (r *CloudPingRequest) Validate() error {
	if r.Serial == "" {
		return errors.New("serial is required")
	}
	return nil
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

func (r *CloudLedsRequest) Validate() error {
	if r.Serial == "" {
		return errors.New("serial is required")
	}
	if r.Pattern != "on" && r.Pattern != "off" && r.Pattern != "blink" {
		return errors.New("pattern must be on, off, or blink")
	}
	if r.When != 0 {
		return errors.New("when must be zero for leds")
	}
	return nil
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

// Validate enforces telemetry constraints.
func (r *CloudTelemetryRequest) Validate() error {
	if r.Serial == "" {
		return errors.New("serial is required")
	}
	if r.Interval == nil || *r.Interval < 0 || *r.Interval > 60 {
		return fmt.Errorf("invalid interval")
	}
	if len(r.Types) != 1 || r.Types[0] != "dhcp" {
		return fmt.Errorf("invalid types")
	}
	return nil
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
	Method  RemoteAccessMethod `json:"method,omitempty"`
	Serial  string             `json:"serial"`
	Token   string             `json:"token"`
	ID      string             `json:"id"`
	Server  string             `json:"server"`
	Port    int                `json:"port"`
	User    string             `json:"user,omitempty"`
	Timeout *int               `json:"timeout,omitempty"`
}

func (r *CloudRemoteAccessRequest) Validate() error {
	if r.Method != RemoteAccessRTTY {
		return fmt.Errorf("invalid method for remote access: %q", r.Method)
	}
	if r.Serial == "" {
		return errors.New("serial is required")
	}
	if r.Token == "" {
		return errors.New("token is required")
	}
	if r.ID == "" {
		return errors.New("id is required")
	}
	if r.Server == "" {
		return errors.New("server is required")
	}
	if r.Port < 1 || r.Port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535")
	}
	return nil
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

func (r *CloudCertupdateRequest) Validate() error {
	if r.Serial == "" {
		return errors.New("serial is required")
	}
	if r.Certificates == "" {
		return errors.New("certificates payload is required")
	}

	decoder := base64.NewDecoder(base64.StdEncoding, strings.NewReader(r.Certificates))
	limitReader := io.LimitReader(decoder, 2*1024*1024+1)

	bytesRead, err := io.ReadAll(limitReader)
	if err != nil {
		return errors.New("certificates must be valid base64")
	}
	if len(bytesRead) > 2*1024*1024 {
		return errors.New("certificates exceed 2 MB decoded limit")
	}
	return nil
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

func (r *CloudReenrollRequest) Validate() error {
	if r.Serial == "" {
		return errors.New("serial is required")
	}
	if r.When != 0 {
		return errors.New("when must be zero for reenroll")
	}
	return nil
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
	Serial    string     `json:"serial"`
	Type      ScriptType `json:"type"`
	Script    string     `json:"script,omitempty"`
	Timeout   *int       `json:"timeout,omitempty"`
	URI       string     `json:"uri,omitempty"`
	Signature string     `json:"signature,omitempty"`
	When      int64      `json:"when,omitempty"`
}

func (r *CloudScriptRequest) UnmarshalJSON(b []byte) error {
	type Alias CloudScriptRequest
	aux := (*Alias)(r)
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.DisallowUnknownFields()
	return decoder.Decode(&aux)
}

func (r *CloudScriptRequest) Validate() error {
	if r.Serial == "" {
		return errors.New("serial is required")
	}
	if r.Type != ScriptTypeShell && r.Type != ScriptTypeUcode && r.Type != ScriptTypeBundle {
		return fmt.Errorf("invalid script type: %q", r.Type)
	}
	if r.When != 0 {
		return errors.New("when must be zero for script")
	}
	if (r.Script == "") == (r.URI == "") {
		return errors.New("exactly one of script or uri must be provided")
	}

	if r.Script != "" {
		decoder := base64.NewDecoder(base64.StdEncoding, strings.NewReader(r.Script))
		limitReader := io.LimitReader(decoder, 1024*1024+1)

		bytesRead, err := io.ReadAll(limitReader)
		if err != nil {
			return errors.New("script must be valid base64")
		}
		if len(bytesRead) > 1024*1024 {
			return errors.New("script exceeds 1 MB decoded limit")
		}
	}

	if r.URI != "" {
		u, err := url.ParseRequestURI(r.URI)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return errors.New("invalid script URI")
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return fmt.Errorf("script uri scheme must be http or https, got %q", u.Scheme)
		}
	}

	return nil
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
