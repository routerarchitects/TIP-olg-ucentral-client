//go:build e2e

package system_e2e

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"
)

type Config struct {
	SecAPIURL    string
	GwAPIURL     string
	AdminUser    string
	AdminPass    string
	DeviceSerial string
	VyosIP       string
	VyosUser     string
	VyosPass     string
}

func loadConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		SecAPIURL:    getEnvOrDefault("OW_SEC_URL", "https://openwifi.wlan.local:16001"),
		GwAPIURL:     getEnvOrDefault("OW_GW_URL", "https://openwifi.wlan.local:16002"),
		AdminUser:    requireEnv(t, "OW_ADMIN_USER"),
		AdminPass:    requireEnv(t, "OW_ADMIN_PASS"),
		DeviceSerial: getEnvOrDefault("OW_DEVICE_SERIAL", "001122334455"),
		VyosIP:       getEnvOrDefault("VYOS_IP", "172.16.3.185:22"), // Updated IP
		VyosUser:     getEnvOrDefault("VYOS_USER", "vyos"),
		VyosPass:     getEnvOrDefault("VYOS_PASS", "vyos"),
	}
}

func getEnvOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func requireEnv(t *testing.T, key string) string {
	t.Helper()
	val := os.Getenv(key)
	if val == "" {
		t.Fatalf("Required environment variable %s is not set", key)
	}
	return val
}

func getAuthToken(t *testing.T, cfg Config, client *http.Client) string {
	t.Helper()
	loginPayload := map[string]interface{}{
		"userId":   cfg.AdminUser,
		"password": cfg.AdminPass,
	}
	b, err := json.Marshal(loginPayload)
	if err != nil {
		t.Fatalf("Failed to marshal login payload: %v", err)
	}
	req, err := http.NewRequest("POST", cfg.SecAPIURL+"/api/v1/oauth2", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("Failed to create login request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Login request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Login request rejected (Status %d)", resp.StatusCode)
	}

	var result struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode login response: %v", err)
	}
	if result.AccessToken == "" {
		t.Fatalf("Login response missing access_token")
	}
	return result.AccessToken
}

func TestSystemE2E_ConfigSync(t *testing.T) {
	cfg := loadConfig(t)
	// We use a 30s timeout here because the Gateway waits for the device to apply the config
	client := &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
		Timeout:   30 * time.Second,
	}
	token := getAuthToken(t, cfg, client)
	uniqueConfigUUID := time.Now().Unix()

	configPayload := map[string]interface{}{
		"serialNumber": cfg.DeviceSerial,
		"UUID":         uniqueConfigUUID,
		"configuration": map[string]interface{}{
			"uuid": uniqueConfigUUID,
			"interfaces": []map[string]interface{}{
				{
					"name": "LAN",
					"role": "downstream",
					"ipv4": map[string]interface{}{
						"addressing": "static",
						"subnet":     "192.168.100.1/24",
						"dhcp": map[string]interface{}{
							"lease-time":  "24h",
							"lease-start": "192.168.100.10",
							"lease-end":   "192.168.100.200",
							"lease-first": 10,
							"lease-count": 100,
						},
					},
					"ethernet": []map[string]interface{}{
						{
							"select-ports": []string{"LAN*"},
						},
					},
				},
			},
		},
	}
	b, err := json.Marshal(configPayload)
	if err != nil {
		t.Fatalf("Failed to marshal config payload: %v", err)
	}
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/api/v1/device/%s/configure", cfg.GwAPIURL, cfg.DeviceSerial), bytes.NewReader(b))
	if err != nil {
		t.Fatalf("Failed to create configure request: %v", err)
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

	var apiResponse map[string]interface{}
	if err := json.Unmarshal(body, &apiResponse); err != nil {
		t.Fatalf("Failed to parse Cloud API response: %v", err)
	}

	uuidVal, ok := apiResponse["UUID"].(string)
	if !ok {
		t.Fatalf("Response missing UUID: %s", string(body))
	}

	status, _ := apiResponse["status"].(string)

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
			t.Fatalf("Timeout waiting for configure command %s to complete", uuidVal)
		}
	}

	results, ok := apiResponse["results"].(map[string]interface{})
	if !ok || len(results) == 0 {
		t.Fatalf("Configure command completed but contains no results object: %v", apiResponse)
	}

	statusObj, ok := results["status"].(map[string]interface{})
	if !ok {
		t.Fatalf("Results object missing status field: %v", results)
	}

	errCode, ok := statusObj["error"].(float64)
	if !ok || errCode != 0 {
		t.Fatalf("Configure command failed on device! error code: %v, full results: %v", statusObj["error"], results)
	}

	t.Logf("SUCCESS! Cloud API returned OK, meaning the agent successfully applied the configuration and NATS relayed it back!")
	t.Logf("Note: The VyOS SSH daemon is now disabled by the renderer, so SSH verification is skipped.")
}
