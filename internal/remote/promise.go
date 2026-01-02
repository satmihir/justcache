package remote

import (
	"sync"
	"time"
)

const (
	// Default TTL for promises (30 seconds)
	defaultPromiseTTL = 30 * time.Second

	// Cleanup interval for expired promises
	promiseCleanupInterval = 15 * time.Second
)

// Promise represents an intent to upload a cache value
type Promise struct {
	Key       string
	Size      int64 // Expected size from x-jc-size header, -1 if not specified
	CreatedAt time.Time
	ExpiresAt time.Time
}

// PromiseMap manages active upload promises with TTL-based expiration
type PromiseMap struct {
	mu       sync.RWMutex
	promises map[string]*Promise
	stopChan chan struct{}
	stopOnce sync.Once
}

// NewPromiseMap creates a new PromiseMap and starts the background cleanup goroutine
func NewPromiseMap() *PromiseMap {
	pm := &PromiseMap{
		promises: make(map[string]*Promise),
		stopChan: make(chan struct{}),
	}
	go pm.cleanupLoop()
	return pm
}

// Create creates a new promise for the given key.
// Returns false if a promise already exists and hasn't expired.
func (pm *PromiseMap) Create(key string, size int64, ttl time.Duration) bool {
	if ttl <= 0 {
		ttl = defaultPromiseTTL
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Check if promise already exists
	if existing, ok := pm.promises[key]; ok {
		if existing.ExpiresAt.After(time.Now()) {
			// Promise still valid, reject new promise
			return false
		}
		// Existing promise expired, remove it
		delete(pm.promises, key)
	}

	// Create new promise
	now := time.Now()
	pm.promises[key] = &Promise{
		Key:       key,
		Size:      size,
		CreatedAt: now,
		ExpiresAt: now.Add(ttl),
	}
	return true
}

// Get retrieves a promise for the given key.
// Returns nil if no promise exists or if the promise has expired.
// Expired promises are removed on access.
func (pm *PromiseMap) Get(key string) *Promise {
	// First try with read lock (fast path for valid promises)
	pm.mu.RLock()
	promise, ok := pm.promises[key]
	if !ok {
		pm.mu.RUnlock()
		return nil
	}

	now := time.Now()
	if promise.ExpiresAt.After(now) {
		// Promise is valid, return it
		pm.mu.RUnlock()
		return promise
	}
	pm.mu.RUnlock()

	// Promise expired - upgrade to write lock to delete
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Recheck after acquiring write lock (another goroutine may have deleted it)
	promise, ok = pm.promises[key]
	if !ok {
		return nil
	}

	// Recheck expiration (another goroutine may have replaced it with a new promise)
	if promise.ExpiresAt.Before(time.Now()) {
		delete(pm.promises, key)
		return nil
	}

	return promise
}

// Exists checks if a valid (non-expired) promise exists for the given key.
// Expired promises are removed on access.
func (pm *PromiseMap) Exists(key string) bool {
	return pm.Get(key) != nil
}

// Fulfill removes a promise after successful upload.
func (pm *PromiseMap) Fulfill(key string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	delete(pm.promises, key)
}

// RemainingTTL returns the remaining TTL for a promise.
// Returns 0 if the promise doesn't exist or has expired.
func (pm *PromiseMap) RemainingTTL(key string) time.Duration {
	promise := pm.Get(key)
	if promise == nil {
		return 0
	}
	remaining := time.Until(promise.ExpiresAt)
	if remaining < 0 {
		return 0
	}
	return remaining
}

// cleanupLoop runs periodically to remove expired promises
func (pm *PromiseMap) cleanupLoop() {
	ticker := time.NewTicker(promiseCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			pm.cleanupExpired()
		case <-pm.stopChan:
			return
		}
	}
}

// cleanupExpired removes all expired promises
func (pm *PromiseMap) cleanupExpired() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	now := time.Now()
	for key, promise := range pm.promises {
		if promise.ExpiresAt.Before(now) {
			delete(pm.promises, key)
		}
	}
}

// Stop stops the background cleanup goroutine.
// Safe to call multiple times.
func (pm *PromiseMap) Stop() {
	pm.stopOnce.Do(func() {
		close(pm.stopChan)
	})
}

// Len returns the number of promises (including potentially expired ones)
// Primarily for testing purposes.
func (pm *PromiseMap) Len() int {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return len(pm.promises)
}
