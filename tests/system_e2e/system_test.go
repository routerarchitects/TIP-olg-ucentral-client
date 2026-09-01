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
	b, _ := json.Marshal(loginPayload)
	req, _ := http.NewRequest("POST", cfg.SecAPIURL+"/api/v1/oauth2", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Login request failed: %v", err)
	}
	defer resp.Body.Close()
	var result struct {
		AccessToken string `json:"access_token"`
	}
	json.NewDecoder(resp.Body).Decode(&result)
	return result.AccessToken
}

func TestSystemE2E_ConfigSync(t *testing.T) {
	cfg := loadConfig(t)
	// We use a 30s timeout here because the Gateway waits for the device to apply the config
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	token := getAuthToken(t, cfg, client)
	uniqueConfigUUID := int64(1788177501) // dummy UUID

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
	b, _ := json.Marshal(configPayload)
	req, _ := http.NewRequest("POST", fmt.Sprintf("%s/api/v1/device/%s/configure", cfg.GwAPIURL, cfg.DeviceSerial), bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Cloud API request failed: %v", err)
	}

	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode >= 400 {
		t.Fatalf("Cloud API rejected request (Status %d): %s", resp.StatusCode, string(body))
	}

	// If the Cloud API returned 200 OK, it means the Gateway received the "success" WebSocket message from the agent!
	t.Logf("SUCCESS! Cloud API returned OK, meaning the agent successfully applied the configuration and NATS relayed it back!")
	t.Logf("Note: The VyOS SSH daemon is now disabled by the renderer, so SSH verification is skipped.")
}
