//go:build e2e

package system_e2e

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestSystemE2E_TraceUp(t *testing.T) {
	cfg := loadConfig(t)
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}, Timeout: 15 * time.Second}
	token := getAuthToken(t, cfg, client)

	payload := map[string]interface{}{
		"serialNumber": cfg.DeviceSerial,
		"duration":     5,
		"network":      "up",
	}
	b, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Failed to marshal trace payload: %v", err)
	}
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/api/v1/device/%s/trace", cfg.GwAPIURL, cfg.DeviceSerial), bytes.NewReader(b))
	if err != nil {
		t.Fatalf("Failed to create trace request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Cloud API request failed: %v", err)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read Cloud API response: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		t.Fatalf("Cloud API rejected request (Status %d): %s", resp.StatusCode, string(body))
	}
	t.Logf("Cloud API accepted request successfully.")

	var apiResponse map[string]interface{}
	if err := json.Unmarshal(body, &apiResponse); err != nil {
		t.Fatalf("Failed to parse Cloud API response: %v", err)
	}

	// OpenWiFi trace API might block and return the result directly or return a command UUID to poll.
	// We'll verify that it either returned completed results directly, or fetch it if needed.
	uuidVal, ok := apiResponse["UUID"].(string)
	if !ok {
		t.Fatalf("Response missing UUID: %s", string(body))
	}

	status, _ := apiResponse["status"].(string)

	// If the API didn't block and wait for completion, we poll the command status
	if status != "completed" {
		t.Logf("Command %s is %s, polling for completion...", uuidVal, status)

		deadline := time.Now().Add(30 * time.Second)
		completed := false

		for time.Now().Before(deadline) {
			time.Sleep(2 * time.Second)

			pollReq, err := http.NewRequest("GET", fmt.Sprintf("%s/api/v1/command/%s", cfg.GwAPIURL, uuidVal), nil)
			if err != nil {
				continue
			}
			pollReq.Header.Set("Authorization", "Bearer "+token)

			pollResp, err := client.Do(pollReq)
			if err != nil {
				continue
			}
			pollBody, err := io.ReadAll(pollResp.Body)
			if err != nil {
				pollResp.Body.Close()
				continue
			}
			pollResp.Body.Close()

			if pollResp.StatusCode == 200 {
				var pollResult map[string]interface{}
				json.Unmarshal(pollBody, &pollResult)

				if pollStatus, ok := pollResult["status"].(string); ok && pollStatus == "completed" {
					apiResponse = pollResult
					completed = true
					break
				}
			}
		}

		if !completed {
			t.Fatalf("Timeout waiting for trace command %s to complete", uuidVal)
		}
	}

	results, ok := apiResponse["results"].(map[string]interface{})
	if !ok || len(results) == 0 {
		t.Fatalf("Trace command completed but contains no results object: %v", apiResponse)
	}

	t.Logf("SUCCESS! Trace action completed and results returned: %v", results)
}
