package nats

import (
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

//go:embed capabilities.json
var DefaultCapabilities []byte

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

// parseCapabilities parses capabilities from a raw payload byte slice.
// It returns the raw payload and extracted firmware string without mutating state.
// This parses the embedded JSON stub for the missing NATS fetch logic.
func (c *CapabilityCache) parseCapabilities(data []byte) ([]byte, string, error) {
	var metadata struct {
		Version struct {
			OLG struct {
				Major *int `json:"major"`
				Minor *int `json:"minor"`
				Patch *int `json:"patch"`
			} `json:"olg"`
		} `json:"version"`
	}

	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, "", fmt.Errorf("failed to parse capabilities JSON: %w", err)
	}

	if metadata.Version.OLG.Major == nil || metadata.Version.OLG.Minor == nil || metadata.Version.OLG.Patch == nil {
		return nil, "", errors.New("capabilities 'version.olg' missing numeric major, minor, or patch fields")
	}

	firmware := fmt.Sprintf("%d.%d.%d", *metadata.Version.OLG.Major, *metadata.Version.OLG.Minor, *metadata.Version.OLG.Patch)

	return data, firmware, nil
}

// GetCapabilities returns the cached capabilities payload, lazy-loading if necessary.
func (c *CapabilityCache) GetCapabilities() ([]byte, error) {
	c.mu.RLock()
	loaded := len(c.capabilities) > 0
	c.mu.RUnlock()

	// Cache miss: attempt to load from disk (Stub for NATS fetch)
	if !loaded {
		c.mu.Lock()
		// Double-check under write lock to avoid stampedes
		if len(c.capabilities) == 0 {
			data, firmware, err := c.parseCapabilities(DefaultCapabilities)
			if err != nil {
				c.mu.Unlock()
				return nil, err
			}
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
