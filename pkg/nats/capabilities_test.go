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
	if err := cache.LoadFromDisk(tmpFile.Name()); err != nil {
		t.Fatalf("LoadFromDisk failed: %v", err)
	}

	if cache.GetFirmware() != "3.2.0" {
		t.Errorf("Expected firmware 3.2.0, got %s", cache.GetFirmware())
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
	if err := cache.LoadFromDisk(tmpFile.Name()); err == nil {
		t.Error("Expected error for invalid JSON, got nil")
	}
}

func TestCapabilityCache_LazyLoad(t *testing.T) {
	// Because lazy loading hardcodes "capabilities.json" in the current working directory,
	// we will create it locally in the test directory for this execution.
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
		t.Fatalf("Failed to create local capabilities.json: %v", err)
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

	if cache.GetFirmware() != "1.0.0" {
		t.Errorf("Expected firmware 1.0.0, got %s", cache.GetFirmware())
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

	_ = os.WriteFile("capabilities.json", []byte(mockJSON), 0644)
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
			if cache.GetFirmware() != "2.5.1" {
				t.Errorf("Expected firmware 2.5.1, got %s", cache.GetFirmware())
			}
		}()
	}
	wg.Wait()
}
