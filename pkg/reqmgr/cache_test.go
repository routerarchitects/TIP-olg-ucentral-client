package reqmgr

import (
	"testing"
	"time"
)

func TestCacheTTLConfig(t *testing.T) {
	config := CacheTTLConfig{
		DefaultTTL: 10 * time.Minute,
		MethodTTLs: map[string]time.Duration{
			"upgrade": 30 * time.Minute,
		},
	}

	if config.TTLForMethod("upgrade") != 30*time.Minute {
		t.Fatalf("expected 30m for upgrade")
	}

	if config.TTLForMethod("configure") != 10*time.Minute {
		t.Fatalf("expected default 10m for configure")
	}

	emptyConfig := CacheTTLConfig{}
	if emptyConfig.TTLForMethod("configure") != 5*time.Minute {
		t.Fatalf("expected fallback default 5m")
	}
}

func TestTransactionCache_SetAndGet(t *testing.T) {
	cache := NewTransactionCache()

	cache.Set("key1", []byte("payload1"), 10*time.Minute)

	val, found := cache.Get("key1")
	if !found {
		t.Fatalf("expected to find key1")
	}
	if string(val) != "payload1" {
		t.Fatalf("expected payload1, got %s", string(val))
	}

	_, found = cache.Get("key2")
	if found {
		t.Fatalf("did not expect to find key2")
	}
}

func TestTransactionCache_ExpirationAndSweep(t *testing.T) {
	cache := NewTransactionCache()

	// Set with very short TTL
	cache.Set("key1", []byte("payload1"), 50*time.Millisecond)
	
	// Set with long TTL
	cache.Set("key2", []byte("payload2"), 1*time.Hour)

	// Sleep until key1 expires
	time.Sleep(100 * time.Millisecond)

	// Get should block key1 even if sweeper hasn't run
	_, found := cache.Get("key1")
	if found {
		t.Fatalf("expected key1 to be expired on Get")
	}

	// Verify it is actually still in the map physically
	cache.mu.RLock()
	_, physicallyFound := cache.items["key1"]
	cache.mu.RUnlock()
	if !physicallyFound {
		t.Fatalf("expected key1 to still be in map before sweep")
	}

	// Run manual sweep
	cache.sweepCache()

	// Verify physical removal
	cache.mu.RLock()
	_, physicallyFound = cache.items["key1"]
	cache.mu.RUnlock()
	if physicallyFound {
		t.Fatalf("expected key1 to be physically removed after sweep")
	}

	// Verify key2 is still untouched
	_, found = cache.Get("key2")
	if !found {
		t.Fatalf("expected key2 to still be valid")
	}
}
