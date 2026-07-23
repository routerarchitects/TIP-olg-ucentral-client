package config

import (
	"os"
	"testing"
	"time"
)

func TestTC_RM_005_OperationSpecificCacheTTLs(t *testing.T) {
	// Clean up environment variables after test
	t.Cleanup(func() {
		os.Unsetenv("OLG_CACHE_TTL_CONFIGURE")
		os.Unsetenv("OLG_CACHE_TTL_REMOTE_ACCESS")
		os.Unsetenv("OLG_CACHE_TTL_INVALID")
	})

	// Test Defaults
	cfg, err := LoadCacheTTLConfigFromEnv()
	if err != nil {
		t.Fatalf("LoadCacheTTLConfigFromEnv (defaults) failed: %v", err)
	}
	if cfg.Configure != 5*time.Minute { // 5 minutes
		t.Errorf("Expected Configure TTL 5m, got %v", cfg.Configure)
	}
	if cfg.LEDs != 5*time.Minute {
		t.Errorf("Expected LEDs TTL 5m, got %v", cfg.LEDs)
	}
	if cfg.Reboot != 10*time.Minute { // 10 minutes
		t.Errorf("Expected Reboot TTL 10m, got %v", cfg.Reboot)
	}
	if cfg.RemoteAccess != 10*time.Minute {
		t.Errorf("Expected RemoteAccess TTL 10m, got %v", cfg.RemoteAccess)
	}
	if cfg.Factory != 30*time.Minute { // 30 minutes
		t.Errorf("Expected Factory TTL 30m, got %v", cfg.Factory)
	}
	if cfg.Certupdate != 30*time.Minute {
		t.Errorf("Expected Certupdate TTL 30m, got %v", cfg.Certupdate)
	}
	if cfg.Reenroll != 30*time.Minute {
		t.Errorf("Expected Reenroll TTL 30m, got %v", cfg.Reenroll)
	}
	if cfg.Script != 30*time.Minute {
		t.Errorf("Expected Script TTL 30m, got %v", cfg.Script)
	}
	if cfg.Upgrade != 60*time.Minute { // 60 minutes
		t.Errorf("Expected Upgrade TTL 60m, got %v", cfg.Upgrade)
	}
	if cfg.Default != 2*time.Minute { // 2 minutes
		t.Errorf("Expected Default TTL 2m, got %v", cfg.Default)
	}

	// Test Overrides
	os.Setenv("OLG_CACHE_TTL_CONFIGURE", "1h")
	os.Setenv("OLG_CACHE_TTL_REMOTE_ACCESS", "30m")

	cfg2, err := LoadCacheTTLConfigFromEnv()
	if err != nil {
		t.Fatalf("LoadCacheTTLConfigFromEnv (overrides) failed: %v", err)
	}
	if cfg2.Configure != 60*time.Minute {
		t.Errorf("Expected Configure TTL 60m, got %v", cfg2.Configure)
	}
	if cfg2.RemoteAccess != 30*time.Minute {
		t.Errorf("Expected RemoteAccess TTL 30m, got %v", cfg2.RemoteAccess)
	}

	// Test Invalid Override
	os.Setenv("OLG_CACHE_TTL_CONFIGURE", "-5m")
	if _, err = LoadCacheTTLConfigFromEnv(); err == nil {
		t.Error("Expected error for negative duration, got nil")
	}

	// Test Sub-Second Rejections
	subSecondTests := []string{"500ms", "1ns", "999ms"}
	for _, val := range subSecondTests {
		os.Setenv("OLG_CACHE_TTL_CONFIGURE", val)
		if _, err = LoadCacheTTLConfigFromEnv(); err == nil {
			t.Errorf("Expected error for sub-second duration %q, got nil", val)
		}
	}

	// Test exactly 1s
	os.Setenv("OLG_CACHE_TTL_CONFIGURE", "1s")
	cfg3, err := LoadCacheTTLConfigFromEnv()
	if err != nil {
		t.Fatalf("LoadCacheTTLConfigFromEnv failed for 1s: %v", err)
	}
	if cfg3.Configure != time.Second {
		t.Errorf("Expected Configure TTL 1s, got %v", cfg3.Configure)
	}
}

func TestTTLForMethod(t *testing.T) {
	cfg := CacheTTLConfig{
		Configure:    10 * time.Second,
		RemoteAccess: 20 * time.Second,
		Default:      30 * time.Second,
	}

	if ttl := cfg.TTLForMethod("configure"); ttl != 10*time.Second {
		t.Errorf("Expected 10s for configure, got %v", ttl)
	}

	if ttl := cfg.TTLForMethod("remote_access"); ttl != 20*time.Second {
		t.Errorf("Expected 20s for remote_access, got %v", ttl)
	}

	if ttl := cfg.TTLForMethod("remoteaccess"); ttl != 20*time.Second {
		t.Errorf("Expected 20s for remoteaccess, got %v", ttl)
	}

	for _, method := range []string{
		"ping",
		"trace",
		"telemetry",
		"capabilities.get",
		"status.get",
		"unknown_method",
	} {
		if ttl := cfg.TTLForMethod(method); ttl != cfg.Default {
			t.Errorf("TTLForMethod(%q) = %v, want %v", method, ttl, cfg.Default)
		}
	}
}

func TestConfig_Validation(t *testing.T) {
	tmpDir := t.TempDir()
	createTempFile := func(name string) string {
		path := tmpDir + "/" + name
		os.WriteFile(path, []byte("test"), 0644)
		return path
	}
	caFile := createTempFile("ca.pem")
	certFile := createTempFile("cert.pem")
	keyFile := createTempFile("key.pem")
	credsFile := createTempFile("creds.creds")

	validTLS := CloudTLSConfig{
		CAFile:         caFile,
		ClientCertFile: certFile,
		ClientKeyFile:  keyFile,
	}
	validCloud := CloudConfig{
		URL:                           "wss://cloud.example.com",
		ConnectTimeoutSeconds:         10,
		WriteTimeoutSeconds:           10,
		PingIntervalSeconds:           10,
		PongTimeoutSeconds:            10,
		StableSessionThresholdSeconds: 10,
		CompressionThresholdBytes:     1024,
		TLS:                           validTLS,
	}
	validNATS := NATSConfig{
		Servers:         []string{"tls://nats.example.com"},
		CredentialsFile: credsFile,
		CAFile:          caFile,
	}
	validQueues := QueueConfig{
		WSWriterCapacity:      100,
		EmergencyCapacity:     100,
		NATSPublishCapacity:   100,
		CommandResultCapacity: 100,
		TelemetryCapacity:     100,
	}

	validConfig := Config{
		Serial: "serial-123",
		Cloud:  validCloud,
		NATS:   validNATS,
		Queues: validQueues,
	}

	if err := validConfig.Validate(); err != nil {
		t.Fatalf("Expected valid config to pass, got: %v", err)
	}

	tests := []struct {
		name string
		mut  func(c *Config)
	}{
		{"Empty serial", func(c *Config) { c.Serial = "" }},
		{"Malformed URL", func(c *Config) { c.Cloud.URL = "wss://" }},
		{"Missing host URL", func(c *Config) { c.Cloud.URL = "wss:// invalid" }},
		{"Invalid URL scheme", func(c *Config) { c.Cloud.URL = "ws://insecure" }},
		{"Negative timeout", func(c *Config) { c.Cloud.ConnectTimeoutSeconds = -1 }},
		{"Zero ping interval", func(c *Config) { c.Cloud.PingIntervalSeconds = 0 }},
		{"Missing TLS CA", func(c *Config) { c.Cloud.TLS.CAFile = "/missing/ca.pem" }},
		{"Directory TLS CA", func(c *Config) { c.Cloud.TLS.CAFile = tmpDir }},
		{"Malformed NATS URL", func(c *Config) { c.NATS.Servers = []string{"tls://"} }},
		{"Invalid NATS scheme", func(c *Config) { c.NATS.Servers = []string{"nats://localhost"} }},
		{"Missing NATS CA", func(c *Config) { c.NATS.CAFile = "/missing/ca.pem" }},
		{"Directory NATS CA", func(c *Config) { c.NATS.CAFile = tmpDir }},
		{"Negative queue capacity", func(c *Config) { c.Queues.WSWriterCapacity = -100 }},
		{"Zero queue capacity", func(c *Config) { c.Queues.TelemetryCapacity = 0 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig
			// Deep copy the struct to avoid modifying validConfig for other tests
			cfg.Cloud = validCloud
			cfg.Cloud.TLS = validTLS
			cfg.NATS = validNATS
			cfg.NATS.Servers = []string{"tls://nats.example.com"}
			cfg.Queues = validQueues

			tt.mut(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Errorf("Expected error for %s", tt.name)
			}
		})
	}
}
