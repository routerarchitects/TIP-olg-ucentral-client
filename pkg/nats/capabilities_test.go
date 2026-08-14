package nats

import (
	"os"
	"sync"
	"testing"
)

func TestCapabilityCache_LoadFromDisk_Success(t *testing.T) {
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

	cache := NewCapabilityCache(tmpFile.Name())
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

	cache := NewCapabilityCache(tmpFile.Name())
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

	cache := NewCapabilityCache(tmpFile.Name())
	_, _, err = cache.LoadFromDisk(tmpFile.Name())
	if err == nil {
		t.Error("Expected error for invalid firmware structure, got nil")
	}
}

func TestCapabilityCache_NonIntegerFirmware(t *testing.T) {
	mockJSON := `{
		"platform": "olg",
		"version": {
			"olg": {
				"major": 3.5,
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

	cache := NewCapabilityCache(tmpFile.Name())
	_, _, err = cache.LoadFromDisk(tmpFile.Name())
	if err == nil {
		t.Error("Expected error for non-integer firmware fields, got nil")
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

	tmpFile, err := os.CreateTemp("", "capabilities-*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(mockJSON)); err != nil {
		t.Fatalf("Failed to write mock JSON: %v", err)
	}
	tmpFile.Close()

	cache := NewCapabilityCache(tmpFile.Name())

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
	mockJSON := `{
		"version": {
			"olg": {
				"major": 2,
				"minor": 5,
				"patch": 1
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

	cache := NewCapabilityCache(tmpFile.Name())

	var wg sync.WaitGroup
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
	cache := NewCapabilityCache("does_not_exist_12345.json")
	_, err := cache.GetCapabilities()
	if err == nil {
		t.Error("Expected error when lazy loading a missing file, got nil")
	}
}

func TestCapabilityCache_DefensiveCopy(t *testing.T) {
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

	tmpFile, err := os.CreateTemp("", "capabilities-*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(mockJSON)); err != nil {
		t.Fatalf("Failed to write mock JSON: %v", err)
	}
	tmpFile.Close()

	cache := NewCapabilityCache(tmpFile.Name())

	caps1, err := cache.GetCapabilities()
	if err != nil {
		t.Fatalf("GetCapabilities failed: %v", err)
	}

	if len(caps1) > 0 {
		caps1[0] = 'X'
	}

	caps2, err := cache.GetCapabilities()
	if err != nil {
		t.Fatalf("Second GetCapabilities failed: %v", err)
	}

	if len(caps2) > 0 && caps2[0] == 'X' {
		t.Error("Cache was mutated! GetCapabilities did not return a defensive copy.")
	}
}
