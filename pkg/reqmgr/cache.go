package reqmgr

import (
	"bytes"
	"context"
	"sync"
	"time"
)

// CachedResponseError is returned when a duplicate request is intercepted
// and a previously cached response payload is available.
type CachedResponseError struct {
	Payload []byte
}

func (e CachedResponseError) Error() string {
	return "request already completed and cached"
}

// CacheTTLConfig configures how long responses for specific methods are cached.
type CacheTTLConfig struct {
	DefaultTTL time.Duration
	MethodTTLs map[string]time.Duration
}

// TTLForMethod returns the configured TTL for a given method, falling back to DefaultTTL.
func (c *CacheTTLConfig) TTLForMethod(method string) time.Duration {
	if c.MethodTTLs != nil {
		if ttl, ok := c.MethodTTLs[method]; ok {
			return ttl
		}
	}
	if c.DefaultTTL > 0 {
		return c.DefaultTTL
	}
	return 5 * time.Minute
}

type CacheEntry struct {
	Payload   []byte
	ExpiresAt int64 // Unix nanoseconds
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

// Set stores a payload in the cache with the given time-to-live.
func (c *TransactionCache) Set(canonicalCloudID string, payload []byte, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[canonicalCloudID] = CacheEntry{
		Payload:   bytes.Clone(payload),
		ExpiresAt: time.Now().Add(ttl).UnixNano(),
	}
}

// Get retrieves a payload from the cache if it exists and has not expired.
func (c *TransactionCache) Get(canonicalCloudID string) ([]byte, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, ok := c.items[canonicalCloudID]
	if !ok {
		return nil, false
	}

	if time.Now().UnixNano() > entry.ExpiresAt {
		return nil, false // Expired, will be cleaned up by sweeper
	}

	return bytes.Clone(entry.Payload), true
}

// StartCacheSweeper launches a background goroutine that periodically scans the cache
// and deletes expired entries to prevent unbounded memory growth.
func (c *TransactionCache) StartCacheSweeper(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 1 * time.Minute
	}
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.sweepCache()
			}
		}
	}()
}

func (c *TransactionCache) sweepCache() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now().UnixNano()
	for key, entry := range c.items {
		if now > entry.ExpiresAt {
			delete(c.items, key)
		}
	}
}
