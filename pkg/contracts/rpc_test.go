package contracts

import (
	"bytes"
	"compress/zlib"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
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
	// Generate a valid compressed payload to use in tests
	var b bytes.Buffer
	zw := zlib.NewWriter(&b)
	validInnerJSON := `{"serial":"123","uuid":1,"config":{}}`
	zw.Write([]byte(validInnerJSON))
	zw.Close()
	validB64 := base64.StdEncoding.EncodeToString(b.Bytes())
	validSz := uint32(len(validInnerJSON))

	tests := []struct {
		name      string
		req       CloudConfigureRequest
		wantError bool
	}{
		{
			name:      "Valid uncompressed",
			req:       CloudConfigureRequest{Serial: "123", UUID: 1, Config: []byte(`{"foo": "bar"}`)},
			wantError: false,
		},
		{
			name:      "Valid compressed outer params",
			req:       CloudConfigureRequest{Serial: "123", UUID: 1, Compress64: validB64, CompressSz: validSz},
			wantError: false,
		},
		{
			name:      "Both config and compress_64 present",
			req:       CloudConfigureRequest{Serial: "123", UUID: 1, Config: []byte(`{}`), Compress64: validB64, CompressSz: validSz},
			wantError: true,
		},
		{
			name:      "Neither field present",
			req:       CloudConfigureRequest{Serial: "123", UUID: 1},
			wantError: true,
		},
		{
			name:      "Missing compress_sz",
			req:       CloudConfigureRequest{Serial: "123", UUID: 1, Compress64: validB64},
			wantError: true,
		},
		{
			name:      "Missing compress_64",
			req:       CloudConfigureRequest{Serial: "123", UUID: 1, CompressSz: validSz},
			wantError: true,
		},
		{
			name:      "Invalid config JSON",
			req:       CloudConfigureRequest{Serial: "123", UUID: 1, Config: []byte(`{broken`)},
			wantError: true,
		},
		{
			name:      "Invalid config array",
			req:       CloudConfigureRequest{Serial: "123", UUID: 1, Config: []byte(`[]`)},
			wantError: true,
		},
		{
			name:      "Invalid config scalar",
			req:       CloudConfigureRequest{Serial: "123", UUID: 1, Config: []byte(`"hello"`)},
			wantError: true,
		},
		{
			name:      "Nonzero when",
			req:       CloudConfigureRequest{Serial: "123", UUID: 1, When: 12345, Config: []byte(`{}`)},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.req.Validate()
			if (err != nil) != tt.wantError {
				t.Errorf("Validate() error = %v, wantError %v", err, tt.wantError)
			}
		})
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

	t.Run("Nested compressed config", func(t *testing.T) {
		var nested bytes.Buffer
		zwNested := zlib.NewWriter(&nested)
		nestedJSON := fmt.Sprintf(`{"serial":"123","uuid":1,"compress_64":"%s","compress_sz":%d}`, validB64, len(validJSON))
		zwNested.Write([]byte(nestedJSON))
		zwNested.Close()

		nestedB64 := base64.StdEncoding.EncodeToString(nested.Bytes())
		req := CloudCompressedConfigureRequest{Compress64: nestedB64, CompressSz: uint32(len(nestedJSON))}
		_, err := req.DecodeAndValidate()
		if err == nil {
			t.Error("expected error for nested compression")
		} else if !strings.Contains(err.Error(), "nested compression") {
			t.Errorf("expected nested compression error, got: %v", err)
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
	cfgReqEmpty := CloudConfigureRequest{Serial: "123", UUID: 1}
	if err := cfgReqEmpty.Validate(); err == nil {
		t.Error("Expected error for Configure missing both config and compress_64")
	}
	cfgReqSimultaneous := CloudConfigureRequest{
		Serial:     "123",
		UUID:       1,
		Config:     []byte(`{}`),
		Compress64: "base64...",
		CompressSz: 10,
	}
	if err := cfgReqSimultaneous.Validate(); err == nil {
		t.Error("Expected error for Configure with both config and compress_64")
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

	// Leds
	ledsBadPattern := CloudLedsRequest{Serial: "1", Pattern: "invalid"}
	if err := ledsBadPattern.Validate(); err == nil {
		t.Error("Expected error for invalid pattern in Leds")
	}
	validDur := 60
	ledsReq := CloudLedsRequest{Serial: "1", Pattern: "blink", Duration: &validDur}
	if err := ledsReq.Validate(); err != nil {
		t.Errorf("Expected Leds to be valid, got: %v", err)
	}

	validDurMin := 1
	ledsReqMin := CloudLedsRequest{Serial: "1", Pattern: "blink", Duration: &validDurMin}
	if err := ledsReqMin.Validate(); err != nil {
		t.Errorf("Expected Leds to be valid with duration 1, got: %v", err)
	}

	validDurMax := 300
	ledsReqMax := CloudLedsRequest{Serial: "1", Pattern: "blink", Duration: &validDurMax}
	if err := ledsReqMax.Validate(); err != nil {
		t.Errorf("Expected Leds to be valid with duration 300, got: %v", err)
	}

	ledsReqNil := CloudLedsRequest{Serial: "1", Pattern: "blink", Duration: nil}
	if err := ledsReqNil.Validate(); err != nil {
		t.Errorf("Expected Leds to be valid with nil duration, got: %v", err)
	}
	negDur := -1
	ledsBadDur := CloudLedsRequest{Serial: "1", Pattern: "blink", Duration: &negDur}
	if err := ledsBadDur.Validate(); err == nil {
		t.Error("Expected error for negative duration in Leds")
	}
	zeroDur := 0
	ledsZeroDur := CloudLedsRequest{Serial: "1", Pattern: "blink", Duration: &zeroDur}
	if err := ledsZeroDur.Validate(); err == nil {
		t.Error("Expected error for zero duration in Leds")
	}
	traceBadUri := CloudTraceRequest{Serial: "1", URI: "not-a-uri"}
	if err := traceBadUri.Validate(); err == nil {
		t.Error("Expected error for malformed URI in Trace")
	}
	traceBadScheme := CloudTraceRequest{Serial: "1", URI: "ftp://example.com/output"}
	if err := traceBadScheme.Validate(); err == nil {
		t.Error("Expected error for non-http/https URI in Trace")
	}
	traceFileScheme := CloudTraceRequest{Serial: "1", URI: "file:///etc/passwd"}
	if err := traceFileScheme.Validate(); err == nil {
		t.Error("Expected error for file URI in Trace")
	}

	tooHighDur := 86401
	ledsTooHighDur := CloudLedsRequest{Serial: "1", Pattern: "blink", Duration: &tooHighDur}
	if err := ledsTooHighDur.Validate(); err == nil {
		t.Error("Expected error for >300 duration in Leds")
	}

	// Trace duration and packets
	traceNegDur := CloudTraceRequest{Serial: "1", Duration: &negDur}
	if err := traceNegDur.Validate(); err == nil {
		t.Error("Expected error for negative duration in Trace")
	}
	traceTooHighDur := CloudTraceRequest{Serial: "1", Duration: &tooHighDur}
	if err := traceTooHighDur.Validate(); err == nil {
		t.Error("Expected error for >300 duration in Trace")
	}
	traceZeroDur := CloudTraceRequest{Serial: "1", Duration: &zeroDur}
	if err := traceZeroDur.Validate(); err == nil {
		t.Error("Expected error for zero duration in Trace")
	}
	traceNegPackets := CloudTraceRequest{Serial: "1", Packets: &negDur}
	if err := traceNegPackets.Validate(); err == nil {
		t.Error("Expected error for negative packets in Trace")
	}
	traceZeroPackets := CloudTraceRequest{Serial: "1", Packets: &zeroDur}
	if err := traceZeroPackets.Validate(); err == nil {
		t.Error("Expected error for zero packets in Trace")
	}
	tooHighPackets := 1000001
	traceTooHighPackets := CloudTraceRequest{Serial: "1", Packets: &tooHighPackets}
	if err := traceTooHighPackets.Validate(); err == nil {
		t.Error("Expected error for >1000000 packets in Trace")
	}

	// Remote Access Timeout
	raNegTimeout := CloudRemoteAccessRequest{Method: RemoteAccessRTTY, Serial: "1", Token: "1", ID: "1", Server: "1", Port: 22, Timeout: &negDur}
	if err := raNegTimeout.Validate(); err == nil {
		t.Error("Expected error for negative timeout in RemoteAccess")
	}
	raZeroTimeout := CloudRemoteAccessRequest{Method: RemoteAccessRTTY, Serial: "1", Token: "1", ID: "1", Server: "1", Port: 22, Timeout: &zeroDur}
	if err := raZeroTimeout.Validate(); err == nil {
		t.Error("Expected error for zero timeout in RemoteAccess")
	}
	raTooHighTimeout := CloudRemoteAccessRequest{Method: RemoteAccessRTTY, Serial: "1", Token: "1", ID: "1", Server: "1", Port: 22, Timeout: &tooHighDur}
	if err := raTooHighTimeout.Validate(); err == nil {
		t.Error("Expected error for >300 timeout in RemoteAccess")
	}

	// Script Timeout
	scriptNegTimeout := CloudScriptRequest{Serial: "1", Type: "shell", Script: "YQ==", Timeout: &negDur}
	if err := scriptNegTimeout.Validate(); err == nil {
		t.Error("Expected error for negative timeout in Script")
	}
	scriptZeroTimeout := CloudScriptRequest{Serial: "1", Type: "shell", Script: "YQ==", Timeout: &zeroDur}
	if err := scriptZeroTimeout.Validate(); err == nil {
		t.Error("Expected error for zero timeout in Script")
	}
	scriptTooHighTimeout := CloudScriptRequest{Serial: "1", Type: "shell", Script: "YQ==", Timeout: &tooHighDur}
	if err := scriptTooHighTimeout.Validate(); err == nil {
		t.Error("Expected error for >300 timeout in Script")
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
	// Trace boundary tests
	validDurMin := 1
	validDurMax := 300
	traceValidDurMax := 300
	validPacketsMin := 1
	traceValidPacketsMax := 10000

	traceReqMin := CloudTraceRequest{Serial: "1", Duration: &validDurMin, Packets: &validPacketsMin}
	if err := traceReqMin.Validate(); err != nil {
		t.Errorf("Expected trace min duration and packets to be valid, got: %v", err)
	}
	traceReqMax := CloudTraceRequest{Serial: "1", Duration: &traceValidDurMax, Packets: &traceValidPacketsMax}
	if err := traceReqMax.Validate(); err != nil {
		t.Errorf("Expected trace max duration and packets to be valid, got: %v", err)
	}

	// Remote Access Timeout bounds
	raReqMin := CloudRemoteAccessRequest{Method: RemoteAccessRTTY, Serial: "1", Token: "tok", ID: "id1", Server: "srv", Port: 1234, Timeout: &validDurMin}
	if err := raReqMin.Validate(); err != nil {
		t.Errorf("Expected remote access min timeout to be valid, got: %v", err)
	}
	raReqMax := CloudRemoteAccessRequest{Method: RemoteAccessRTTY, Serial: "1", Token: "tok", ID: "id1", Server: "srv", Port: 1234, Timeout: &validDurMax}
	if err := raReqMax.Validate(); err != nil {
		t.Errorf("Expected remote access max timeout to be valid, got: %v", err)
	}

	// Script Timeout bounds
	scriptReqMin := CloudScriptRequest{Serial: "1", Type: "shell", Script: scriptEncoded, Timeout: &validDurMin}
	if err := scriptReqMin.Validate(); err != nil {
		t.Errorf("Expected script min timeout to be valid, got: %v", err)
	}
	scriptReqMax := CloudScriptRequest{Serial: "1", Type: "shell", Script: scriptEncoded, Timeout: &validDurMax}
	if err := scriptReqMax.Validate(); err != nil {
		t.Errorf("Expected script max timeout to be valid, got: %v", err)
	}
}
