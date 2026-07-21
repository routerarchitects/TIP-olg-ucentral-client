package contracts

import (
	"bytes"
	"compress/zlib"
	"encoding/base64"
	"encoding/json"
	"testing"
)

func TestTC_CON_002_ErrorMappings(t *testing.T) {
	rpcErr, err := NewInternalJSONRPCError(ErrServiceUnavailable, "Internal Error")
	if err != nil {
		t.Fatalf("Failed to create error: %v", err)
	}

	_, err = NewInternalJSONRPCError(999999, "Invalid")
	if err == nil {
		t.Error("Expected error for invalid application code, got nil")
	}

	b, err := json.Marshal(rpcErr)
	if err != nil {
		t.Fatalf("Failed to marshal error: %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(b, &parsed); err != nil {
		t.Fatalf("failed to unmarshal serialized value: %v", err)
	}

	if parsed["code"].(float64) != -32603 {
		t.Errorf("Expected code -32603, got %v", parsed["code"])
	}

	dataMap := parsed["data"].(map[string]interface{})
	if dataMap["application_code"].(float64) != 3 {
		t.Errorf("Expected application_code 3, got %v", dataMap["application_code"])
	}
}

func TestTC_ACT_002_FactoryRequest(t *testing.T) {
	t.Run("Factory Validations", func(t *testing.T) {
		keepRedir := 1
		req := CloudFactoryRequest{
			Serial:         "12345",
			KeepRedirector: &keepRedir,
		}
		if err := req.Validate(); err != nil {
			t.Errorf("expected valid request to pass, got: %v", err)
		}

		keepRedirZero := 0
		reqZero := CloudFactoryRequest{
			Serial:         "12345",
			KeepRedirector: &keepRedirZero,
		}
		if err := reqZero.Validate(); err != nil {
			t.Errorf("expected keep_redirector=0 to pass, got: %v", err)
		}

		reqMissing := CloudFactoryRequest{Serial: "12345"}
		if err := reqMissing.Validate(); err == nil {
			t.Errorf("expected missing keep_redirector to fail")
		}

		keepRedirInvalid := 2
		reqInvalidVal := CloudFactoryRequest{
			Serial:         "12345",
			KeepRedirector: &keepRedirInvalid,
		}
		if err := reqInvalidVal.Validate(); err == nil {
			t.Errorf("expected invalid keep_redirector to fail")
		}

		reqNonZeroWhen := CloudFactoryRequest{
			Serial:         "12345",
			KeepRedirector: &keepRedir,
			When:           100,
		}
		if err := reqNonZeroWhen.Validate(); err == nil {
			t.Errorf("expected non-zero when to fail")
		}
	})
}

func TestTC_ACT_008_TelemetryRequest(t *testing.T) {
	t.Run("Telemetry Validations", func(t *testing.T) {
		interval := 60
		reqValid := CloudTelemetryRequest{
			Serial:   "12345",
			Interval: &interval,
			Types:    []string{"dhcp"},
		}
		if err := reqValid.Validate(); err != nil {
			t.Errorf("expected valid request to pass, got: %v", err)
		}

		intervalZero := 0
		reqZero := CloudTelemetryRequest{Serial: "12345", Interval: &intervalZero, Types: []string{"dhcp"}}
		if err := reqZero.Validate(); err != nil {
			t.Errorf("expected interval=0 to pass, got: %v", err)
		}

		reqEmptyTypes := CloudTelemetryRequest{Serial: "12345", Interval: &interval}
		if err := reqEmptyTypes.Validate(); err == nil {
			t.Errorf("expected empty types to fail")
		}

		invalidInterval1 := 61
		reqInvalid1 := CloudTelemetryRequest{Serial: "12345", Interval: &invalidInterval1, Types: []string{"dhcp"}}
		if err := reqInvalid1.Validate(); err == nil {
			t.Errorf("expected interval > 60 to fail")
		}

		invalidInterval2 := -1
		reqInvalid2 := CloudTelemetryRequest{Serial: "12345", Interval: &invalidInterval2, Types: []string{"dhcp"}}
		if err := reqInvalid2.Validate(); err == nil {
			t.Errorf("expected interval < 0 to fail")
		}

		reqInvalidTypes1 := CloudTelemetryRequest{Serial: "12345", Interval: &interval, Types: []string{"dhcp", "dhcp"}}
		if err := reqInvalidTypes1.Validate(); err == nil {
			t.Errorf("expected multiple types to fail")
		}

		reqInvalidTypes2 := CloudTelemetryRequest{Serial: "12345", Interval: &interval, Types: []string{"other"}}
		if err := reqInvalidTypes2.Validate(); err == nil {
			t.Errorf("expected non-dhcp type to fail")
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
		t.Fatal("expected string UUID to be rejected")
	}
}

func TestTC_CON_007_CompressedConfigureRequest(t *testing.T) {
	// Generate valid compressed data
	var b bytes.Buffer
	zw := zlib.NewWriter(&b)
	validJSON := `{"serial":"123","uuid":1,"config":{}}`
	zw.Write([]byte(validJSON))
	zw.Close()

	validB64 := base64.StdEncoding.EncodeToString(b.Bytes())

	t.Run("Valid compressed config", func(t *testing.T) {
		req := CloudCompressedConfigureRequest{
			Compress64: validB64,
			CompressSz: uint32(len(validJSON)),
		}
		decoded, err := req.DecodeAndValidate()
		if err != nil {
			t.Fatalf("expected valid decode, got: %v", err)
		}
		if decoded.Serial != "123" {
			t.Errorf("expected serial 123, got %s", decoded.Serial)
		}
	})

	t.Run("Invalid base64", func(t *testing.T) {
		req := CloudCompressedConfigureRequest{Compress64: "invalid base64!", CompressSz: 10}
		_, err := req.DecodeAndValidate()
		if err == nil {
			t.Error("expected error for invalid base64")
		}
	})

	t.Run("Invalid zlib", func(t *testing.T) {
		invalidZlib := base64.StdEncoding.EncodeToString([]byte("not zlib data"))
		req := CloudCompressedConfigureRequest{Compress64: invalidZlib, CompressSz: 10}
		_, err := req.DecodeAndValidate()
		if err == nil {
			t.Error("expected error for invalid zlib")
		}
	})

	t.Run("Incorrect compress_sz", func(t *testing.T) {
		req := CloudCompressedConfigureRequest{Compress64: validB64, CompressSz: 999}
		_, err := req.DecodeAndValidate()
		if err == nil {
			t.Error("expected error for incorrect compress_sz")
		}
	})

	t.Run("Exceeds 10MB limit", func(t *testing.T) {
		req := CloudCompressedConfigureRequest{Compress64: validB64, CompressSz: 11 * 1024 * 1024}
		_, err := req.DecodeAndValidate()
		if err == nil {
			t.Error("expected error for size exceeding 10MB limit")
		}
	})

	t.Run("Invalid inner JSON", func(t *testing.T) {
		var bad bytes.Buffer
		zwBad := zlib.NewWriter(&bad)
		invalidJSON := `{broken json`
		zwBad.Write([]byte(invalidJSON))
		zwBad.Close()

		badB64 := base64.StdEncoding.EncodeToString(bad.Bytes())
		req := CloudCompressedConfigureRequest{Compress64: badB64, CompressSz: uint32(len(invalidJSON))}
		_, err := req.DecodeAndValidate()
		if err == nil {
			t.Error("expected error for invalid inner JSON")
		}
	})
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

func TestValidation_EdgeCases(t *testing.T) {
	// Configure
	cfgReq := CloudConfigureRequest{Serial: "123", UUID: 1, Config: []byte(`{}`), When: 1}
	if err := cfgReq.Validate(); err == nil {
		t.Error("Expected error for non-zero when in Configure")
	}
	cfgReqEmpty := CloudConfigureRequest{}
	if err := cfgReqEmpty.Validate(); err == nil {
		t.Error("Expected error for empty Configure")
	}

	// Reboot
	rebReq := CloudRebootRequest{Serial: "123", When: 1}
	if err := rebReq.Validate(); err == nil {
		t.Error("Expected error for non-zero when in Reboot")
	}
	rebReqEmpty := CloudRebootRequest{}
	if err := rebReqEmpty.Validate(); err == nil {
		t.Error("Expected error for empty Reboot")
	}

	// Upgrade
	upgReqNoUri := CloudUpgradeRequest{Serial: "123"}
	if err := upgReqNoUri.Validate(); err == nil {
		t.Error("Expected error for empty URI in Upgrade")
	}
	upgReqBadUri := CloudUpgradeRequest{Serial: "123", URI: "not-a-url"}
	if err := upgReqBadUri.Validate(); err == nil {
		t.Error("Expected error for malformed URI in Upgrade")
	}
	upgReqBadScheme := CloudUpgradeRequest{Serial: "123", URI: "ftp://example.com/fw.bin"}
	if err := upgReqBadScheme.Validate(); err == nil {
		t.Error("Expected error for non-http/https URI in Upgrade")
	}
	upgReqNonZeroWhen := CloudUpgradeRequest{Serial: "123", URI: "https://example.com/fw.bin", When: 1}
	if err := upgReqNonZeroWhen.Validate(); err == nil {
		t.Error("Expected error for non-zero when in Upgrade")
	}

	// Remote Access
	raReq := CloudRemoteAccessRequest{Method: "ssh"}
	if err := raReq.Validate(); err == nil {
		t.Error("Expected error for non-rtty method in Remote Access")
	}
	raReqEmpty := CloudRemoteAccessRequest{Method: "rtty"}
	if err := raReqEmpty.Validate(); err == nil {
		t.Error("Expected error for empty Remote Access fields")
	}

	// Certupdate
	certReq := CloudCertupdateRequest{Serial: "1", Certificates: "invalid_base64"}
	if err := certReq.Validate(); err == nil {
		t.Error("Expected error for invalid base64 in Certupdate")
	}
	largeDecoded := make([]byte, 2*1024*1024+1)
	largeEncoded := base64.StdEncoding.EncodeToString(largeDecoded)
	certReqLarge := CloudCertupdateRequest{Serial: "1", Certificates: largeEncoded}
	if err := certReqLarge.Validate(); err == nil {
		t.Error("expected decoded certificate bundle over 2 MiB to fail")
	}
	certReqEmpty := CloudCertupdateRequest{}
	if err := certReqEmpty.Validate(); err == nil {
		t.Error("Expected error for empty Certupdate")
	}

	// Reenroll
	renReq := CloudReenrollRequest{Serial: "123", When: 1}
	if err := renReq.Validate(); err == nil {
		t.Error("Expected error for non-zero when in Reenroll")
	}
	renReqEmpty := CloudReenrollRequest{}
	if err := renReqEmpty.Validate(); err == nil {
		t.Error("Expected error for empty Reenroll")
	}

	// Script
	scriptReqType := CloudScriptRequest{Serial: "1", Type: "python"}
	if err := scriptReqType.Validate(); err == nil {
		t.Error("Expected error for invalid script type")
	}
	scriptReqMissing := CloudScriptRequest{Serial: "1", Type: "shell"}
	if err := scriptReqMissing.Validate(); err == nil {
		t.Error("Expected error for missing script and uri")
	}
	scriptReqBoth := CloudScriptRequest{Serial: "1", Type: "shell", Script: "YQ==", URI: "http://example.com"}
	if err := scriptReqBoth.Validate(); err == nil {
		t.Error("Expected error for both script and uri")
	}
	scriptReqInvalidB64 := CloudScriptRequest{Serial: "1", Type: "shell", Script: "invalid_base64!"}
	if err := scriptReqInvalidB64.Validate(); err == nil {
		t.Error("Expected error for invalid base64 script")
	}
	scriptReqEmpty := CloudScriptRequest{}
	if err := scriptReqEmpty.Validate(); err == nil {
		t.Error("Expected error for empty Script")
	}
	scriptReqBadScheme := CloudScriptRequest{Serial: "1", Type: "shell", URI: "ftp://example.com/script.sh"}
	if err := scriptReqBadScheme.Validate(); err == nil {
		t.Error("Expected error for non-http/https URI in Script")
	}

	// Unknown scriptId rejection test
	scriptJsonWithUnknown := []byte(`{
		"serial": "123",
		"type": "shell",
		"script": "YQ==",
		"scriptId": "unexpected"
	}`)
	var sReq CloudScriptRequest
	if err := json.Unmarshal(scriptJsonWithUnknown, &sReq); err == nil {
		t.Error("Expected error for unknown field scriptId during JSON parsing")
	}
}

func TestValidation_PositiveCases(t *testing.T) {
	// Configure
	cfgReq := CloudConfigureRequest{Serial: "123", UUID: 1, Config: []byte(`{"foo":"bar"}`)}
	if err := cfgReq.Validate(); err != nil {
		t.Errorf("Expected Configure to be valid, got: %v", err)
	}

	// Reboot
	rebReq := CloudRebootRequest{Serial: "123"}
	if err := rebReq.Validate(); err != nil {
		t.Errorf("Expected Reboot to be valid, got: %v", err)
	}

	// Upgrade
	upgReq := CloudUpgradeRequest{Serial: "123", URI: "https://example.com/fw.bin"}
	if err := upgReq.Validate(); err != nil {
		t.Errorf("Expected Upgrade to be valid, got: %v", err)
	}

	// Remote Access
	raReq := CloudRemoteAccessRequest{
		Method: "rtty",
		Serial: "123",
		Token:  "tok",
		ID:     "id1",
		Server: "srv",
		Port:   1234,
	}
	if err := raReq.Validate(); err != nil {
		t.Errorf("Expected Remote Access to be valid, got: %v", err)
	}

	// Certupdate
	validBase64 := base64.StdEncoding.EncodeToString([]byte("testcert"))
	certReq := CloudCertupdateRequest{Serial: "1", Certificates: validBase64}
	if err := certReq.Validate(); err != nil {
		t.Errorf("Expected Certupdate to be valid, got: %v", err)
	}

	// Reenroll
	renReq := CloudReenrollRequest{Serial: "123"}
	if err := renReq.Validate(); err != nil {
		t.Errorf("Expected Reenroll to be valid, got: %v", err)
	}

	// Script (inline shell)
	exact1MB := make([]byte, 1024*1024)
	scriptEncoded := base64.StdEncoding.EncodeToString(exact1MB)
	scriptReq := CloudScriptRequest{Serial: "1", Type: "shell", Script: scriptEncoded}
	if err := scriptReq.Validate(); err != nil {
		t.Errorf("Expected exactly 1MB shell Script to be valid, got: %v", err)
	}

	// Script (inline ucode)
	scriptUcodeReq := CloudScriptRequest{Serial: "1", Type: "ucode", Script: scriptEncoded}
	if err := scriptUcodeReq.Validate(); err != nil {
		t.Errorf("Expected ucode Script to be valid, got: %v", err)
	}

	// Script (inline bundle)
	scriptBundleReq := CloudScriptRequest{Serial: "1", Type: "bundle", Script: scriptEncoded}
	if err := scriptBundleReq.Validate(); err != nil {
		t.Errorf("Expected bundle Script to be valid, got: %v", err)
	}

	// Script (URI)
	scriptURIReq := CloudScriptRequest{Serial: "1", Type: "shell", URI: "https://example.com/script.sh"}
	if err := scriptURIReq.Validate(); err != nil {
		t.Errorf("Expected Script URI to be valid, got: %v", err)
	}
}
