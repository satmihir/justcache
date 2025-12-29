package storage

import (
	"errors"
	"sync"
	"time"

	"github.com/satmihir/justcache/internal/constants"
)

var (
	ErrKeyNotFound         = errors.New("key not found")
	ErrDeleteKeyNotFound   = errors.New("delete key not found")
	ErrMemoryLimitExceeded = errors.New("memory limit exceeded")
	ErrKeyTooLong          = errors.New("key is too long")
	ErrKeyTooShort         = errors.New("key is too short")
	ErrObjectTooLarge      = errors.New("value exceeds maximum size")
	ErrValueTooShort       = errors.New("value is too short")
	ErrInvalidTTL          = errors.New("TTL must be greater than zero")
)

// Local storage with key-value store with caching semantics
type LocalStorage interface {
	// Get the value for the given key. Returns nil if the key is not found.
	Get(key string) ([]byte, error)
	// Put the given value for the given key.
	Put(key string, value []byte, ttl time.Duration) error
	// Delete the given key.
	Delete(key string) error
}

// InMemoryStorage is a local storage implementation that uses in-memory storage
// and bounded memory usage.
type InMemoryStorage struct {
	// We use a mutex to protect the storage.
	mutex sync.Mutex
	// We count the bytes of all the keys and values in the storage.
	memoryUsedBytes uint64
	// We set a maximum memory limit for the storage.
	maxMemory uint64
	// We use a map to store the keys and values.
	store map[string]*CachedObject
	// LRU tracking list.
	lru lruList
}

func (s *InMemoryStorage) Get(key string) ([]byte, error) {
	// Validate before acquiring lock to reduce lock hold time
	if err := validateKey(key); err != nil {
		return nil, err
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()
	node, ok := s.store[key]
	if !ok {
		return nil, ErrKeyNotFound
	}

	if node.ExpirationTime.Before(time.Now()) {
		s.deleteUnlocked(key)
		return nil, ErrKeyNotFound
	}

	// Move the node to the tail of the list (most recently used).
	s.lru.moveToTail(node)
	return node.Value, nil
}

func (s *InMemoryStorage) Put(key string, value []byte, ttl time.Duration) error {
	// Validate before acquiring lock to reduce lock hold time
	if err := validateKey(key); err != nil {
		return err
	}

	if ttl <= 0 {
		return ErrInvalidTTL
	}

	if len(value) == 0 {
		return ErrValueTooShort
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Calculate the size this new object will use (key + value)
	newObjectSize := uint64(len(key) + len(value))

	// This check needs the lock since maxMemory could theoretically be dynamic
	if newObjectSize > s.maxMemory {
		return ErrObjectTooLarge
	}

	// Calculate net memory needed, accounting for existing key if present
	existingObjectSize := uint64(0)
	if existing, ok := s.store[key]; ok {
		existingObjectSize = existing.GetBytesUsed()
	}

	// Only need additional memory if new object is larger than existing
	var additionalMemoryNeeded uint64
	if newObjectSize > existingObjectSize {
		additionalMemoryNeeded = newObjectSize - existingObjectSize
	}

	if s.memoryUsedBytes+additionalMemoryNeeded > s.maxMemory {
		// Try to free up some memory by deleting ttl'ed keys.
		freedBytes := s.limitedTtlCleanup(additionalMemoryNeeded)
		if freedBytes < additionalMemoryNeeded {
			// Try to free up some memory by evicting LRU items.
			freedBytes += s.limitedEviction(additionalMemoryNeeded - freedBytes)
		}

		if freedBytes < additionalMemoryNeeded {
			return ErrMemoryLimitExceeded
		}

		// Re-check if our key still exists after eviction (it might have been evicted).
		if existing, ok := s.store[key]; ok {
			existingObjectSize = existing.GetBytesUsed()
		} else {
			existingObjectSize = 0 // Key was evicted during cleanup
		}
	}

	// Delete old key if it still exists.
	if existingObjectSize > 0 {
		s.deleteUnlocked(key)
	}

	// Final memory check: ensure we have space for the new object.
	// This catches edge cases where eviction deleted our key but we still don't have room.
	if s.memoryUsedBytes+newObjectSize > s.maxMemory {
		return ErrMemoryLimitExceeded
	}

	cachedObject := &CachedObject{
		Key:            key,
		Value:          value,
		ExpirationTime: time.Now().Add(ttl),
	}

	s.store[key] = cachedObject
	s.memoryUsedBytes += cachedObject.GetBytesUsed()

	// Add to tail of LRU list (most recently used).
	s.lru.append(cachedObject)

	return nil
}

func (s *InMemoryStorage) Delete(key string) error {
	// Validate before acquiring lock to reduce lock hold time
	if err := validateKey(key); err != nil {
		return err
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.deleteUnlocked(key)
}

// deleteUnlocked removes the key from storage. Lock must be held by caller.
func (s *InMemoryStorage) deleteUnlocked(key string) error {
	node, ok := s.store[key]
	if !ok {
		return ErrDeleteKeyNotFound
	}

	s.lru.remove(node)
	delete(s.store, node.Key)
	s.memoryUsedBytes -= node.GetBytesUsed()

	return nil
}

// limitedTtlCleanup attempts to free up only the given amount of memory by deleting ttl'ed keys.
// Returns the amount of memory freed up. Lock must be held by caller.
func (s *InMemoryStorage) limitedTtlCleanup(minimumReclaimBytes uint64) uint64 {
	ptr := s.lru.front()
	freedBytes := uint64(0)
	now := time.Now()

	for ptr != nil {
		next := ptr.next // Save next before potential deletion
		if ptr.ExpirationTime.Before(now) {
			freedBytes += ptr.GetBytesUsed()
			s.deleteUnlocked(ptr.Key)
			if freedBytes >= minimumReclaimBytes {
				break
			}
		}
		ptr = next
	}

	return freedBytes
}

// limitedEviction evicts LRU items from the head just enough to free up the given amount of memory.
// Returns the amount of memory freed up. Lock must be held by caller.
func (s *InMemoryStorage) limitedEviction(minimumReclaimBytes uint64) uint64 {
	ptr := s.lru.front()
	freedBytes := uint64(0)

	for ptr != nil {
		next := ptr.next // Save next before deletion
		freedBytes += ptr.GetBytesUsed()
		s.deleteUnlocked(ptr.Key)
		if freedBytes >= minimumReclaimBytes {
			break
		}
		ptr = next
	}

	return freedBytes
}

// StorageOptions configures the in-memory storage.
type StorageOptions struct {
	// InitialCapacity is a hint for the expected number of items.
	// Pre-allocating reduces map resizing overhead.
	InitialCapacity int
}

func NewInMemoryStorage(maxMemory uint64, opts ...StorageOptions) *InMemoryStorage {
	initialCapacity := 0
	if len(opts) > 0 {
		initialCapacity = opts[0].InitialCapacity
	}

	return &InMemoryStorage{
		store:     make(map[string]*CachedObject, initialCapacity),
		maxMemory: maxMemory,
		// lru is zero-initialized correctly (head: nil, tail: nil)
	}
}

func validateKey(key string) error {
	if len(key) == 0 {
		return ErrKeyTooShort
	}

	if len(key) > constants.MaxKeySizeBytes {
		return ErrKeyTooLong
	}

	return nil
}
