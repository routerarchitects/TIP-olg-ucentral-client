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

// LoadFromDisk loads the capabilities from the provided file path (e.g., capabilities.json)
// and updates the cache. This stubs out the missing NATS fetch logic.
func (c *CapabilityCache) LoadFromDisk(filePath string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read capabilities file: %w", err)
	}

	var caps map[string]interface{}
	if err := json.Unmarshal(data, &caps); err != nil {
		return fmt.Errorf("failed to parse capabilities JSON: %w", err)
	}

	// Try to extract a firmware version string for caching
	firmware := "unknown"
	if version, ok := caps["version"].(map[string]interface{}); ok {
		if olg, ok := version["olg"].(map[string]interface{}); ok {
			firmware = fmt.Sprintf("%v.%v.%v", olg["major"], olg["minor"], olg["patch"])
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.capabilities = data
	c.firmware = firmware
	return nil
}

// GetCapabilities returns the cached capabilities payload, lazy-loading if necessary.
func (c *CapabilityCache) GetCapabilities() ([]byte, error) {
	c.mu.RLock()
	loaded := len(c.capabilities) > 0
	c.mu.RUnlock()

	// Cache miss: attempt to load from disk (Stub for NATS fetch)
	if !loaded {
		if err := c.LoadFromDisk("capabilities.json"); err != nil {
			return nil, err
		}
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
func (c *CapabilityCache) GetFirmware() string {
	c.mu.RLock()
	loaded := len(c.capabilities) > 0
	c.mu.RUnlock()

	if !loaded {
		_ = c.LoadFromDisk("capabilities.json")
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.firmware
}
