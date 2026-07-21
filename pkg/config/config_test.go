package config

import (
	"os"
	"testing"
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
	if cfg.Configure != 300 { // 5 minutes in seconds
		t.Errorf("Expected Configure TTL 300, got %d", cfg.Configure)
	}
	if cfg.RemoteAccess != 600 { // 10 minutes in seconds
		t.Errorf("Expected RemoteAccess TTL 600, got %d", cfg.RemoteAccess)
	}

	// Test Overrides
	os.Setenv("OLG_CACHE_TTL_CONFIGURE", "1h")
	os.Setenv("OLG_CACHE_TTL_REMOTE_ACCESS", "30m")

	cfg2, err := LoadCacheTTLConfigFromEnv()
	if err != nil {
		t.Fatalf("LoadCacheTTLConfigFromEnv (overrides) failed: %v", err)
	}
	if cfg2.Configure != 3600 {
		t.Errorf("Expected Configure TTL 3600, got %d", cfg2.Configure)
	}
	if cfg2.RemoteAccess != 1800 {
		t.Errorf("Expected RemoteAccess TTL 1800, got %d", cfg2.RemoteAccess)
	}

	// Test Invalid Override
	os.Setenv("OLG_CACHE_TTL_CONFIGURE", "-5m")
	_, err = LoadCacheTTLConfigFromEnv()
	if err == nil {
		t.Error("Expected error for negative duration, got nil")
	}
}

func TestTTLForMethod(t *testing.T) {
	cfg := CacheTTLConfig{
		Configure:    10,
		RemoteAccess: 20,
		Default:      30,
	}

	if ttl := cfg.TTLForMethod("configure"); ttl != 10 {
		t.Errorf("Expected 10 for configure, got %d", ttl)
	}
	if ttl := cfg.TTLForMethod("remote_access"); ttl != 20 {
		t.Errorf("Expected 20 for remote_access, got %d", ttl)
	}
	if ttl := cfg.TTLForMethod("remoteaccess"); ttl != 20 {
		t.Errorf("Expected 20 for remoteaccess, got %d", ttl)
	}
	if ttl := cfg.TTLForMethod("unknown_method"); ttl != 30 {
		t.Errorf("Expected 30 for unknown, got %d", ttl)
	}
}
