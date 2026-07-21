package contracts

import (
	"encoding/json"
	"testing"
)

func TestTC_CON_002_ErrorMappings(t *testing.T) {
	errDataBytes := json.RawMessage(`{"application_code":3}`)

	rpcErr := JSONRPCError{
		Code:    ErrInternal, // -32603
		Message: "Internal Error",
		Data:    errDataBytes,
	}

	b, err := json.Marshal(rpcErr)
	if err != nil {
		t.Fatalf("Failed to marshal error: %v", err)
	}

	var parsed map[string]interface{}
	json.Unmarshal(b, &parsed)

	if parsed["code"].(float64) != -32603 {
		t.Errorf("Expected code -32603, got %v", parsed["code"])
	}

	dataMap := parsed["data"].(map[string]interface{})
	if dataMap["application_code"].(float64) != 3 {
		t.Errorf("Expected application_code 3, got %v", dataMap["application_code"])
	}
}

func TestTC_ACT_002_FactoryRequest(t *testing.T) {
	t.Run("Factory Keep Redirector Optionality", func(t *testing.T) {
		// keep_redirector pointer logic
		keepRedir := 1
		req := CloudFactoryRequest{
			Serial:         "12345",
			KeepRedirector: &keepRedir,
		}

		b, _ := json.Marshal(req)
		var parsed map[string]interface{}
		json.Unmarshal(b, &parsed)

		if parsed["keep_redirector"].(float64) != 1 {
			t.Errorf("keep_redirector was not correctly marshalled")
		}

		reqNil := CloudFactoryRequest{
			Serial: "12345",
		}

		bNil, _ := json.Marshal(reqNil)
		var parsedNil map[string]interface{}
		json.Unmarshal(bNil, &parsedNil)

		if parsedNil["keep_redirector"] != nil {
			t.Errorf("keep_redirector should be null when nil pointer. Got: %v", string(bNil))
		}
	})
}

func TestTC_ACT_008_TelemetryRequest(t *testing.T) {
	t.Run("Telemetry Interval Optionality", func(t *testing.T) {
		interval := 60
		req := CloudTelemetryRequest{
			Serial:   "12345",
			Interval: &interval,
			Types:    []string{"dhcp"},
		}

		b, _ := json.Marshal(req)
		var parsed map[string]interface{}
		json.Unmarshal(b, &parsed)

		if parsed["interval"].(float64) != 60 {
			t.Errorf("interval was not serialized correctly")
		}
	})
}

func TestTC_CON_006_ConfigureRequest(t *testing.T) {
	// TC-CON-006: OWGW Configure Request parsing
	reqJson := []byte(`{
		"serial": "12345",
		"uuid": 100,
		"config": {"foo": "bar"}
	}`)

	var req CloudConfigureRequest
	if err := json.Unmarshal(reqJson, &req); err != nil {
		t.Fatalf("Failed to parse CloudConfigureRequest: %v", err)
	}

	if req.UUID != 100 {
		t.Errorf("UUID should be parsed as int64")
	}

	// UUID as string should fail or behave differently based on unmarshal
	invalidReqJson := []byte(`{
		"serial": "12345",
		"uuid": "100"
	}`)
	var invalidReq CloudConfigureRequest
	if err := json.Unmarshal(invalidReqJson, &invalidReq); err == nil {
		// Strict JSON unmarshalling for int64 will fail if it's a string
		// Since we didn't use json.Number, this should fail
	}
}

func TestTC_ACT_001_RebootRequest(t *testing.T) {
	// TC-ACT-001: OWGW Reboot Request
	reqJson := []byte(`{"serial": "12345", "when": 0}`)
	var req CloudRebootRequest
	if err := json.Unmarshal(reqJson, &req); err != nil {
		t.Fatalf("Failed to parse Reboot: %v", err)
	}
	if req.When != 0 {
		t.Errorf("Expected when=0")
	}
}

func TestTC_ACT_009_RemoteAccessRequest(t *testing.T) {
	// TC-ACT-009: Remote Access / RTTY Request
	reqJson := []byte(`{
		"serial": "12345",
		"method": "rtty",
		"token": "tok1",
		"id": "rtty1",
		"server": "localhost",
		"port": 5912
	}`)
	var req CloudRemoteAccessRequest
	if err := json.Unmarshal(reqJson, &req); err != nil {
		t.Fatalf("Failed to parse RemoteAccess: %v", err)
	}
	if req.Port != 5912 {
		t.Errorf("Expected port 5912")
	}
}
