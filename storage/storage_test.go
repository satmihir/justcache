package storage

import (
	"sync"
	"testing"
	"time"

	"github.com/satmihir/justcache/internal/constants"
)

// ============================================================================
// Helper Functions
// ============================================================================

func newStorage(maxMemory uint64) *InMemoryStorage {
	return NewInMemoryStorage(maxMemory)
}

func mustPut(t *testing.T, s *InMemoryStorage, key string, value []byte, ttl time.Duration) {
	t.Helper()
	if err := s.Put(key, value, ttl); err != nil {
		t.Fatalf("Put(%q) failed: %v", key, err)
	}
}

func assertMemoryUsed(t *testing.T, s *InMemoryStorage, expected uint64) {
	t.Helper()
	if s.memoryUsedBytes != expected {
		t.Errorf("memoryUsedBytes = %d, want %d", s.memoryUsedBytes, expected)
	}
}

func assertStoreSize(t *testing.T, s *InMemoryStorage, expected int) {
	t.Helper()
	if len(s.store) != expected {
		t.Errorf("store size = %d, want %d", len(s.store), expected)
	}
}

// ============================================================================
// Basic Get Tests
// ============================================================================

func TestGet_KeyNotFound(t *testing.T) {
	s := newStorage(1000)
	_, err := s.Get("nonexistent")
	if err != ErrKeyNotFound {
		t.Errorf("Get() error = %v, want ErrKeyNotFound", err)
	}
}

func TestGet_KeyFound(t *testing.T) {
	s := newStorage(1000)
	mustPut(t, s, "key", []byte("value"), time.Hour)

	val, err := s.Get("key")
	if err != nil {
		t.Errorf("Get() error = %v", err)
	}
	if string(val) != "value" {
		t.Errorf("Get() = %q, want %q", val, "value")
	}
}

func TestGet_ExpiredKey(t *testing.T) {
	s := newStorage(1000)
	mustPut(t, s, "key", []byte("value"), 50*time.Millisecond)

	// Wait for expiration
	time.Sleep(100 * time.Millisecond)

	_, err := s.Get("key")
	if err != ErrKeyNotFound {
		t.Errorf("Get() error = %v, want ErrKeyNotFound", err)
	}

	// Verify key was deleted
	assertStoreSize(t, s, 0)
	assertMemoryUsed(t, s, 0)
}

func TestGet_MovesToTail(t *testing.T) {
	s := newStorage(1000)
	mustPut(t, s, "a", []byte("1"), time.Hour)
	mustPut(t, s, "b", []byte("2"), time.Hour)
	mustPut(t, s, "c", []byte("3"), time.Hour)
	// LRU order: a -> b -> c

	// Access 'a' - should move to tail
	s.Get("a")
	// LRU order: b -> c -> a

	// Verify by checking LRU front
	if s.lru.front().Key != "b" {
		t.Errorf("LRU front = %q, want %q", s.lru.front().Key, "b")
	}
}

// ============================================================================
// Basic Put Tests
// ============================================================================

func TestPut_NewKey(t *testing.T) {
	s := newStorage(1000)
	err := s.Put("key", []byte("value"), time.Hour)
	if err != nil {
		t.Errorf("Put() error = %v", err)
	}
	assertStoreSize(t, s, 1)
	assertMemoryUsed(t, s, 8) // "key" (3) + "value" (5)
}

func TestPut_UpdateExistingKey(t *testing.T) {
	s := newStorage(1000)
	mustPut(t, s, "key", []byte("old"), time.Hour)
	assertMemoryUsed(t, s, 6) // 3 + 3

	mustPut(t, s, "key", []byte("newvalue"), time.Hour)
	assertStoreSize(t, s, 1)
	assertMemoryUsed(t, s, 11) // 3 + 8

	val, _ := s.Get("key")
	if string(val) != "newvalue" {
		t.Errorf("Get() = %q, want %q", val, "newvalue")
	}
}

func TestPut_UpdateWithSmallerValue(t *testing.T) {
	s := newStorage(1000)
	mustPut(t, s, "key", []byte("largevalue"), time.Hour)
	assertMemoryUsed(t, s, 13) // 3 + 10

	mustPut(t, s, "key", []byte("sm"), time.Hour)
	assertMemoryUsed(t, s, 5) // 3 + 2
}

// ============================================================================
// Validation Tests
// ============================================================================

func TestPut_EmptyKey(t *testing.T) {
	s := newStorage(1000)
	err := s.Put("", []byte("value"), time.Hour)
	if err != ErrKeyTooShort {
		t.Errorf("Put() error = %v, want ErrKeyTooShort", err)
	}
}

func TestPut_KeyTooLong(t *testing.T) {
	s := newStorage(1000)
	longKey := string(make([]byte, constants.MaxKeySizeBytes+1))
	err := s.Put(longKey, []byte("value"), time.Hour)
	if err != ErrKeyTooLong {
		t.Errorf("Put() error = %v, want ErrKeyTooLong", err)
	}
}

func TestPut_EmptyValue(t *testing.T) {
	s := newStorage(1000)
	err := s.Put("key", []byte{}, time.Hour)
	if err != ErrValueTooShort {
		t.Errorf("Put() error = %v, want ErrValueTooShort", err)
	}
}

func TestPut_ZeroTTL(t *testing.T) {
	s := newStorage(1000)
	err := s.Put("key", []byte("value"), 0)
	if err != ErrInvalidTTL {
		t.Errorf("Put() error = %v, want ErrInvalidTTL", err)
	}
}

func TestPut_NegativeTTL(t *testing.T) {
	s := newStorage(1000)
	err := s.Put("key", []byte("value"), -time.Second)
	if err != ErrInvalidTTL {
		t.Errorf("Put() error = %v, want ErrInvalidTTL", err)
	}
}

func TestGet_EmptyKey(t *testing.T) {
	s := newStorage(1000)
	_, err := s.Get("")
	if err != ErrKeyTooShort {
		t.Errorf("Get() error = %v, want ErrKeyTooShort", err)
	}
}

func TestGet_KeyTooLong(t *testing.T) {
	s := newStorage(1000)
	longKey := string(make([]byte, constants.MaxKeySizeBytes+1))
	_, err := s.Get(longKey)
	if err != ErrKeyTooLong {
		t.Errorf("Get() error = %v, want ErrKeyTooLong", err)
	}
}

func TestDelete_EmptyKey(t *testing.T) {
	s := newStorage(1000)
	err := s.Delete("")
	if err != ErrKeyTooShort {
		t.Errorf("Delete() error = %v, want ErrKeyTooShort", err)
	}
}

func TestDelete_KeyTooLong(t *testing.T) {
	s := newStorage(1000)
	longKey := string(make([]byte, constants.MaxKeySizeBytes+1))
	err := s.Delete(longKey)
	if err != ErrKeyTooLong {
		t.Errorf("Delete() error = %v, want ErrKeyTooLong", err)
	}
}

// ============================================================================
// Delete Tests
// ============================================================================

func TestDelete_KeyExists(t *testing.T) {
	s := newStorage(1000)
	mustPut(t, s, "key", []byte("value"), time.Hour)

	err := s.Delete("key")
	if err != nil {
		t.Errorf("Delete() error = %v", err)
	}
	assertStoreSize(t, s, 0)
	assertMemoryUsed(t, s, 0)
}

func TestDelete_KeyNotFound(t *testing.T) {
	s := newStorage(1000)
	err := s.Delete("nonexistent")
	if err != ErrDeleteKeyNotFound {
		t.Errorf("Delete() error = %v, want ErrDeleteKeyNotFound", err)
	}
}

// ============================================================================
// Memory Limit Tests
// ============================================================================

func TestPut_ObjectTooLarge(t *testing.T) {
	s := newStorage(10) // Very small
	err := s.Put("key", []byte("this is way too large"), time.Hour)
	if err != ErrObjectTooLarge {
		t.Errorf("Put() error = %v, want ErrObjectTooLarge", err)
	}
}

func TestPut_FillsExactly(t *testing.T) {
	s := newStorage(10)
	// key (3) + value (7) = 10 bytes exactly
	err := s.Put("key", []byte("1234567"), time.Hour)
	if err != nil {
		t.Errorf("Put() error = %v", err)
	}
	assertMemoryUsed(t, s, 10)
}

func TestPut_TriggersLRUEviction(t *testing.T) {
	s := newStorage(20)
	// Each entry: key (1) + value (5) = 6 bytes
	mustPut(t, s, "a", []byte("11111"), time.Hour)
	mustPut(t, s, "b", []byte("22222"), time.Hour)
	mustPut(t, s, "c", []byte("33333"), time.Hour)
	// Total: 18 bytes, remaining: 2 bytes

	// This needs 6 bytes, will evict "a"
	err := s.Put("d", []byte("44444"), time.Hour)
	if err != nil {
		t.Errorf("Put() error = %v", err)
	}

	// "a" should be evicted
	_, err = s.Get("a")
	if err != ErrKeyNotFound {
		t.Errorf("Get(a) should return ErrKeyNotFound after eviction")
	}

	// "b", "c", "d" should exist
	for _, k := range []string{"b", "c", "d"} {
		if _, err := s.Get(k); err != nil {
			t.Errorf("Get(%q) error = %v", k, err)
		}
	}
}

func TestPut_TriggersMultipleEvictions(t *testing.T) {
	s := newStorage(20)
	// Each entry: 6 bytes
	mustPut(t, s, "a", []byte("11111"), time.Hour)
	mustPut(t, s, "b", []byte("22222"), time.Hour)
	mustPut(t, s, "c", []byte("33333"), time.Hour)
	// Total: 18 bytes

	// This needs 14 bytes, will evict "a" and "b"
	err := s.Put("d", []byte("1234567890123"), time.Hour)
	if err != nil {
		t.Errorf("Put() error = %v", err)
	}

	// "a" and "b" should be evicted
	for _, k := range []string{"a", "b"} {
		if _, err := s.Get(k); err != ErrKeyNotFound {
			t.Errorf("Get(%q) should return ErrKeyNotFound", k)
		}
	}

	// "c" and "d" should exist
	for _, k := range []string{"c", "d"} {
		if _, err := s.Get(k); err != nil {
			t.Errorf("Get(%q) error = %v", k, err)
		}
	}
}

func TestPut_TriggersTTLCleanup(t *testing.T) {
	s := newStorage(20)
	// Put with short TTL
	mustPut(t, s, "a", []byte("11111"), 50*time.Millisecond)
	mustPut(t, s, "b", []byte("22222"), time.Hour)
	mustPut(t, s, "c", []byte("33333"), time.Hour)
	// Total: 18 bytes

	// Wait for "a" to expire
	time.Sleep(100 * time.Millisecond)

	// This needs 6 bytes, TTL cleanup should free "a"
	err := s.Put("d", []byte("44444"), time.Hour)
	if err != nil {
		t.Errorf("Put() error = %v", err)
	}

	// "a" should be cleaned up (expired)
	_, err = s.Get("a")
	if err != ErrKeyNotFound {
		t.Errorf("Get(a) should return ErrKeyNotFound after TTL cleanup")
	}

	// "b" should still exist (not evicted, TTL cleanup was enough)
	if _, err := s.Get("b"); err != nil {
		t.Errorf("Get(b) error = %v", err)
	}
}

func TestPut_NotEnoughMemoryEvenAfterEviction(t *testing.T) {
	s := newStorage(10)
	mustPut(t, s, "a", []byte("12345"), time.Hour) // 6 bytes

	// Try to put something that won't fit even after evicting everything
	// After eviction, we have 10 bytes available
	// New object: key (1) + value (15) = 16 bytes > 10
	err := s.Put("b", []byte("123456789012345"), time.Hour)
	if err != ErrObjectTooLarge {
		t.Errorf("Put() error = %v, want ErrObjectTooLarge", err)
	}
}

// ============================================================================
// LRU Order Tests
// ============================================================================

func TestLRU_AccessAffectsEvictionOrder(t *testing.T) {
	s := newStorage(18) // Fits exactly 3 items of 6 bytes each

	mustPut(t, s, "a", []byte("11111"), time.Hour)
	mustPut(t, s, "b", []byte("22222"), time.Hour)
	mustPut(t, s, "c", []byte("33333"), time.Hour)
	// LRU order: a -> b -> c

	// Access "a" - moves to tail
	s.Get("a")
	// LRU order: b -> c -> a

	// Add new item, should evict "b" (now the LRU)
	mustPut(t, s, "d", []byte("44444"), time.Hour)

	// "b" should be evicted
	_, err := s.Get("b")
	if err != ErrKeyNotFound {
		t.Errorf("Get(b) should return ErrKeyNotFound")
	}

	// "a" should still exist (was accessed recently)
	if _, err := s.Get("a"); err != nil {
		t.Errorf("Get(a) error = %v", err)
	}
}

func TestLRU_MultipleAccessesAffectOrder(t *testing.T) {
	s := newStorage(24) // Fits 4 items of 6 bytes

	mustPut(t, s, "a", []byte("11111"), time.Hour)
	mustPut(t, s, "b", []byte("22222"), time.Hour)
	mustPut(t, s, "c", []byte("33333"), time.Hour)
	mustPut(t, s, "d", []byte("44444"), time.Hour)
	// LRU order: a -> b -> c -> d

	// Access in reverse order
	s.Get("d")
	s.Get("c")
	s.Get("b")
	s.Get("a")
	// LRU order: d -> c -> b -> a

	// Add new item, should evict "d"
	mustPut(t, s, "e", []byte("55555"), time.Hour)

	_, err := s.Get("d")
	if err != ErrKeyNotFound {
		t.Errorf("Get(d) should return ErrKeyNotFound")
	}

	// "a" should still exist (most recently accessed)
	if _, err := s.Get("a"); err != nil {
		t.Errorf("Get(a) error = %v", err)
	}
}

// ============================================================================
// Edge Cases
// ============================================================================

func TestEviction_DeletesKeyBeingUpdated(t *testing.T) {
	// This tests the bug we fixed: when updating a key that's at the LRU head,
	// eviction might delete it, then we try to delete it again.
	s := newStorage(12)

	mustPut(t, s, "a", []byte("11111"), time.Hour) // 6 bytes, at head
	mustPut(t, s, "b", []byte("22222"), time.Hour) // 6 bytes, at tail
	// Total: 12 bytes (full)

	// Update "a" with larger value (needs more memory)
	// Since "a" is at head, eviction will delete it first
	err := s.Put("a", []byte("1234567"), time.Hour) // 8 bytes needed
	if err != nil {
		t.Errorf("Put() error = %v", err)
	}

	// Verify "a" has new value
	val, err := s.Get("a")
	if err != nil {
		t.Errorf("Get(a) error = %v", err)
	}
	if string(val) != "1234567" {
		t.Errorf("Get(a) = %q, want %q", val, "1234567")
	}
}

func TestPut_UpdateNoEvictionNeeded(t *testing.T) {
	s := newStorage(20)
	mustPut(t, s, "a", []byte("1234567890"), time.Hour) // 11 bytes

	// Update with smaller value, no eviction needed
	err := s.Put("a", []byte("12345"), time.Hour) // 6 bytes
	if err != nil {
		t.Errorf("Put() error = %v", err)
	}
	assertMemoryUsed(t, s, 6)
}

func TestMemoryAccounting_AfterManyOperations(t *testing.T) {
	s := newStorage(1000)

	// Series of puts
	mustPut(t, s, "a", []byte("111"), time.Hour) // 4 bytes
	mustPut(t, s, "b", []byte("222"), time.Hour) // 4 bytes
	mustPut(t, s, "c", []byte("333"), time.Hour) // 4 bytes
	assertMemoryUsed(t, s, 12)

	// Delete one
	s.Delete("b")
	assertMemoryUsed(t, s, 8)

	// Update one
	mustPut(t, s, "a", []byte("11111"), time.Hour) // 6 bytes
	assertMemoryUsed(t, s, 10) // 6 + 4

	// Add another
	mustPut(t, s, "d", []byte("444"), time.Hour) // 4 bytes
	assertMemoryUsed(t, s, 14)

	// Delete all
	s.Delete("a")
	s.Delete("c")
	s.Delete("d")
	assertMemoryUsed(t, s, 0)
}

func TestEmptyStorage_Operations(t *testing.T) {
	s := newStorage(1000)

	// Get from empty
	_, err := s.Get("key")
	if err != ErrKeyNotFound {
		t.Errorf("Get() error = %v, want ErrKeyNotFound", err)
	}

	// Delete from empty
	err = s.Delete("key")
	if err != ErrDeleteKeyNotFound {
		t.Errorf("Delete() error = %v, want ErrDeleteKeyNotFound", err)
	}

	assertMemoryUsed(t, s, 0)
	assertStoreSize(t, s, 0)
}

func TestSingleItem_AllOperations(t *testing.T) {
	s := newStorage(1000)

	// Put
	mustPut(t, s, "key", []byte("value"), time.Hour)
	assertStoreSize(t, s, 1)

	// Get
	val, _ := s.Get("key")
	if string(val) != "value" {
		t.Errorf("Get() = %q", val)
	}

	// Update
	mustPut(t, s, "key", []byte("newval"), time.Hour)
	val, _ = s.Get("key")
	if string(val) != "newval" {
		t.Errorf("Get() = %q", val)
	}

	// Delete
	s.Delete("key")
	assertStoreSize(t, s, 0)
	assertMemoryUsed(t, s, 0)
}

// ============================================================================
// Concurrency Tests
// ============================================================================

func TestConcurrent_Reads(t *testing.T) {
	s := newStorage(1000)
	mustPut(t, s, "key", []byte("value"), time.Hour)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				val, err := s.Get("key")
				if err != nil {
					t.Errorf("Get() error = %v", err)
				}
				if string(val) != "value" {
					t.Errorf("Get() = %q", val)
				}
			}
		}()
	}
	wg.Wait()
}

func TestConcurrent_Writes(t *testing.T) {
	s := newStorage(10000)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := string(rune('a' + id%26))
			for j := 0; j < 100; j++ {
				s.Put(key, []byte("value"), time.Hour)
			}
		}(i)
	}
	wg.Wait()

	// Verify storage is consistent
	if s.memoryUsedBytes > 10000 {
		t.Errorf("memoryUsedBytes = %d, exceeds maxMemory", s.memoryUsedBytes)
	}
}

func TestConcurrent_ReadWrite(t *testing.T) {
	s := newStorage(10000)

	// Pre-populate
	for i := 0; i < 10; i++ {
		key := string(rune('a' + i))
		mustPut(t, s, key, []byte("initial"), time.Hour)
	}

	var wg sync.WaitGroup

	// Readers
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				key := string(rune('a' + j%10))
				s.Get(key)
			}
		}()
	}

	// Writers
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				key := string(rune('a' + j%10))
				s.Put(key, []byte("updated"), time.Hour)
			}
		}(i)
	}

	wg.Wait()
}

// ============================================================================
// Constructor Tests
// ============================================================================

func TestNewInMemoryStorage_Default(t *testing.T) {
	s := NewInMemoryStorage(1000)
	if s.maxMemory != 1000 {
		t.Errorf("maxMemory = %d, want 1000", s.maxMemory)
	}
	if s.store == nil {
		t.Error("store should not be nil")
	}
}

func TestNewInMemoryStorage_WithOptions(t *testing.T) {
	s := NewInMemoryStorage(1000, StorageOptions{InitialCapacity: 100})
	if s.maxMemory != 1000 {
		t.Errorf("maxMemory = %d, want 1000", s.maxMemory)
	}
}

// ============================================================================
// TTL Edge Cases
// ============================================================================

func TestTTL_ExpiresDuringEviction(t *testing.T) {
	s := newStorage(18)

	// Put items with varying TTLs
	mustPut(t, s, "a", []byte("11111"), 50*time.Millisecond)  // Will expire
	mustPut(t, s, "b", []byte("22222"), time.Hour)
	mustPut(t, s, "c", []byte("33333"), time.Hour)
	// Total: 18 bytes

	time.Sleep(100 * time.Millisecond)

	// Put new item - TTL cleanup should find "a"
	mustPut(t, s, "d", []byte("44444"), time.Hour)

	// Only "b", "c", "d" should exist
	if _, err := s.Get("a"); err != ErrKeyNotFound {
		t.Errorf("Get(a) should return ErrKeyNotFound")
	}
	for _, k := range []string{"b", "c", "d"} {
		if _, err := s.Get(k); err != nil {
			t.Errorf("Get(%q) error = %v", k, err)
		}
	}
}

func TestTTL_AllExpired(t *testing.T) {
	s := newStorage(18)

	mustPut(t, s, "a", []byte("11111"), 50*time.Millisecond)
	mustPut(t, s, "b", []byte("22222"), 50*time.Millisecond)
	mustPut(t, s, "c", []byte("33333"), 50*time.Millisecond)

	time.Sleep(100 * time.Millisecond)

	// Put new item - TTL cleanup should free everything
	mustPut(t, s, "d", []byte("44444"), time.Hour)

	assertStoreSize(t, s, 1)
	if _, err := s.Get("d"); err != nil {
		t.Errorf("Get(d) error = %v", err)
	}
}

