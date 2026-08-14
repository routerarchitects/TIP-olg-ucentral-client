package nats

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
)

// CapabilityCache holds the cached capabilities and firmware version.
type CapabilityCache struct {
	mu           sync.RWMutex
	capabilities []byte
	firmware     string
}

// NewCapabilityCache initializes a new cache.
func NewCapabilityCache() *CapabilityCache {
	return &CapabilityCache{}
}

// LoadFromDisk reads and parses the capabilities from the provided file path.
// It returns the raw payload and extracted firmware string without mutating state.
// This stubs out the missing NATS fetch logic.
func (c *CapabilityCache) LoadFromDisk(filePath string) ([]byte, string, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, "", fmt.Errorf("failed to read capabilities file: %w", err)
	}

	var caps map[string]interface{}
	if err := json.Unmarshal(data, &caps); err != nil {
		return nil, "", fmt.Errorf("failed to parse capabilities JSON: %w", err)
	}

	// Try to extract a firmware version string for caching
	firmware := "unknown"
	if version, ok := caps["version"].(map[string]interface{}); ok {
		if olg, ok := version["olg"].(map[string]interface{}); ok {
			firmware = fmt.Sprintf("%v.%v.%v", olg["major"], olg["minor"], olg["patch"])
		}
	}

	return data, firmware, nil
}

// GetCapabilities returns the cached capabilities payload, lazy-loading if necessary.
func (c *CapabilityCache) GetCapabilities() ([]byte, error) {
	c.mu.RLock()
	loaded := len(c.capabilities) > 0
	c.mu.RUnlock()

	// Cache miss: attempt to load from disk (Stub for NATS fetch)
	if !loaded {
		data, firmware, err := c.LoadFromDisk("capabilities.json")
		if err != nil {
			return nil, err
		}
		
		c.mu.Lock()
		if len(c.capabilities) == 0 {
			c.capabilities = data
			c.firmware = firmware
		}
		c.mu.Unlock()
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.capabilities) == 0 {
		return nil, errors.New("capabilities not populated")
	}

	// Return a copy to prevent external mutation
	out := make([]byte, len(c.capabilities))
	copy(out, c.capabilities)
	return out, nil
}

// GetFirmware returns the cached firmware version, lazy-loading if necessary.
func (c *CapabilityCache) GetFirmware() (string, error) {
	if _, err := c.GetCapabilities(); err != nil {
		return "", err
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.firmware == "" {
		return "", errors.New("firmware not populated")
	}
	return c.firmware, nil
}
