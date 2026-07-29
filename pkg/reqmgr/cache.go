package reqmgr

import (
	"sync"
)

type CacheTTLConfig struct {
	// Add config fields here
}

func (c *CacheTTLConfig) TTLForMethod(method string) int {
	return 300 // default stub
}

type CacheEntry struct {
	Payload   []byte
	ExpiresAt int64
}

type TransactionCache struct {
	mu    sync.RWMutex
	items map[string]CacheEntry
}

func NewTransactionCache() *TransactionCache {
	return &TransactionCache{
		items: make(map[string]CacheEntry),
	}
}

func (c *TransactionCache) Set(canonicalCloudID string, payload []byte, ttlSeconds int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// stub
}

func (c *TransactionCache) Get(canonicalCloudID string) ([]byte, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return nil, false // stub
}
