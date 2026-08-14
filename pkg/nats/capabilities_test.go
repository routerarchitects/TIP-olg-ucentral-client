package nats

import (
	"sync"
	"testing"
)

func TestCapabilityCache_Parse_Success(t *testing.T) {
	mockJSON := []byte(`{
		"platform": "olg",
		"version": {
			"olg": {
				"major": 3,
				"minor": 2,
				"patch": 0
			}
		}
	}`)

	cache := NewCapabilityCache()
	data, fwStr, err := cache.parseCapabilities(mockJSON)
	if err != nil {
		t.Fatalf("parseCapabilities failed: %v", err)
	}

	if len(data) == 0 {
		t.Error("Expected non-empty data from parseCapabilities")
	}

	if fwStr != "3.2.0" {
		t.Errorf("Expected firmware 3.2.0, got %s", fwStr)
	}
}

func TestCapabilityCache_Parse_InvalidJSON(t *testing.T) {
	cache := NewCapabilityCache()
	_, _, err := cache.parseCapabilities([]byte(`{ "invalid": json `))
	if err == nil {
		t.Error("Expected error for invalid JSON, got nil")
	}
}

func TestCapabilityCache_InvalidFirmwareStructure(t *testing.T) {
	mockJSON := []byte(`{
		"platform": "olg",
		"version": {
			"olg": {
				"major": 3,
				"minor": "two"
			}
		}
	}`)

	cache := NewCapabilityCache()
	_, _, err := cache.parseCapabilities(mockJSON)
	if err == nil {
		t.Error("Expected error for invalid firmware structure, got nil")
	}
}

func TestCapabilityCache_NonIntegerFirmware(t *testing.T) {
	mockJSON := []byte(`{
		"platform": "olg",
		"version": {
			"olg": {
				"major": 3.5,
				"minor": 2,
				"patch": 0
			}
		}
	}`)

	cache := NewCapabilityCache()
	_, _, err := cache.parseCapabilities(mockJSON)
	if err == nil {
		t.Error("Expected error for non-integer firmware fields, got nil")
	}
}

func TestCapabilityCache_DefaultStub(t *testing.T) {
	cache := NewCapabilityCache()
	// Validate that the actual shipped operational capabilities.json parses successfully
	data, fwStr, err := cache.parseCapabilities(DefaultCapabilities)
	if err != nil {
		t.Fatalf("Failed to parse the real shipped capabilities.json: %v", err)
	}
	if len(data) == 0 {
		t.Error("Default capabilities parsed as empty payload")
	}
	if fwStr == "unknown" || fwStr == "" {
		t.Errorf("Failed to extract firmware from default capabilities, got %s", fwStr)
	}
}

func TestCapabilityCache_LazyLoad(t *testing.T) {
	// The lazy load test relies on the real capabilities.json via DefaultCapabilities
	cache := NewCapabilityCache()

	// Lazy load triggered by GetCapabilities
	caps, err := cache.GetCapabilities()
	if err != nil {
		t.Fatalf("GetCapabilities failed: %v", err)
	}

	if len(caps) == 0 {
		t.Error("Expected non-empty capabilities payload")
	}

	fw, err := cache.GetFirmware()
	if err != nil {
		t.Fatalf("GetFirmware failed: %v", err)
	}
	if fw == "" {
		t.Errorf("Expected firmware string, got empty")
	}
}

func TestCapabilityCache_Concurrency(t *testing.T) {
	// Ensure that GetCapabilities can be called concurrently without data races
	cache := NewCapabilityCache()

	var wg sync.WaitGroup
	// Spawn multiple goroutines to lazily load and read at the same time
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			caps, err := cache.GetCapabilities()
			if err != nil {
				t.Errorf("Concurrent GetCapabilities failed: %v", err)
			}
			if len(caps) == 0 {
				t.Error("Concurrent GetCapabilities returned empty payload")
			}
			fw, err := cache.GetFirmware()
			if err != nil {
				t.Errorf("GetFirmware failed: %v", err)
			}
			if fw == "" {
				t.Errorf("Expected firmware string, got empty")
			}
		}()
	}
	wg.Wait()
}

func TestCapabilityCache_DefensiveCopy(t *testing.T) {
	cache := NewCapabilityCache()

	caps1, err := cache.GetCapabilities()
	if err != nil {
		t.Fatalf("GetCapabilities failed: %v", err)
	}

	// Mutate the returned slice
	if len(caps1) > 0 {
		caps1[0] = 'X'
	}

	// Fetch again and ensure the cache was NOT mutated
	caps2, err := cache.GetCapabilities()
	if err != nil {
		t.Fatalf("Second GetCapabilities failed: %v", err)
	}

	if len(caps2) > 0 && caps2[0] == 'X' {
		t.Error("Cache was mutated! GetCapabilities did not return a defensive copy.")
	}
}
