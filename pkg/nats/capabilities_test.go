package nats

import (
	"os"
	"sync"
	"testing"
)

func TestCapabilityCache_LoadFromDisk_Success(t *testing.T) {
	// Setup a temporary JSON file
	mockJSON := `{
		"platform": "olg",
		"version": {
			"olg": {
				"major": 3,
				"minor": 2,
				"patch": 0
			}
		}
	}`

	tmpFile, err := os.CreateTemp("", "capabilities-*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(mockJSON)); err != nil {
		t.Fatalf("Failed to write mock JSON: %v", err)
	}
	tmpFile.Close()

	cache := NewCapabilityCache()
	data, fwStr, err := cache.LoadFromDisk(tmpFile.Name())
	if err != nil {
		t.Fatalf("LoadFromDisk failed: %v", err)
	}

	if len(data) == 0 {
		t.Error("Expected non-empty data from LoadFromDisk")
	}

	if fwStr != "3.2.0" {
		t.Errorf("Expected firmware 3.2.0, got %s", fwStr)
	}
}

func TestCapabilityCache_LoadFromDisk_InvalidJSON(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "capabilities-*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(`{ "invalid": json `)); err != nil {
		t.Fatalf("Failed to write mock JSON: %v", err)
	}
	tmpFile.Close()

	cache := NewCapabilityCache()
	_, _, err = cache.LoadFromDisk(tmpFile.Name())
	if err == nil {
		t.Error("Expected error for invalid JSON, got nil")
	}
}

func TestCapabilityCache_InvalidFirmwareStructure(t *testing.T) {
	mockJSON := `{
		"platform": "olg",
		"version": {
			"olg": {
				"major": 3,
				"minor": "two"
			}
		}
	}`

	tmpFile, err := os.CreateTemp("", "capabilities-*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(mockJSON)); err != nil {
		t.Fatalf("Failed to write mock JSON: %v", err)
	}
	tmpFile.Close()

	cache := NewCapabilityCache()
	_, _, err = cache.LoadFromDisk(tmpFile.Name())
	if err == nil {
		t.Error("Expected error for invalid firmware structure, got nil")
	}
}

func TestCapabilityCache_LazyLoad(t *testing.T) {
	mockJSON := `{
		"version": {
			"olg": {
				"major": 1,
				"minor": 0,
				"patch": 0
			}
		}
	}`

	err := os.WriteFile("capabilities.json", []byte(mockJSON), 0644)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove("capabilities.json")

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
	if fw != "1.0.0" {
		t.Errorf("Expected firmware 1.0.0, got %s", fw)
	}
}

func TestCapabilityCache_Concurrency(t *testing.T) {
	// Ensure that GetCapabilities can be called concurrently without data races
	mockJSON := `{
		"version": {
			"olg": {
				"major": 2,
				"minor": 5,
				"patch": 1
			}
		}
	}`

	err := os.WriteFile("capabilities.json", []byte(mockJSON), 0644)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove("capabilities.json")

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
			if fw != "2.5.1" {
				t.Errorf("Expected firmware 2.5.1, got %s", fw)
			}
		}()
	}
	wg.Wait()
}

func TestCapabilityCache_MissingFile(t *testing.T) {
	cache := NewCapabilityCache()
	os.Remove("capabilities.json")
	// Attempting to get capabilities from a cache mapped to a missing file
	_, err := cache.GetCapabilities()
	if err == nil {
		t.Error("Expected error when lazy loading a missing file, got nil")
	}
}

func TestCapabilityCache_DefensiveCopy(t *testing.T) {
	// Setup a temporary JSON file
	mockJSON := `{
		"platform": "test",
		"version": {
			"olg": {
				"major": 1,
				"minor": 0,
				"patch": 0
			}
		}
	}`

	err := os.WriteFile("capabilities.json", []byte(mockJSON), 0644)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove("capabilities.json")

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
