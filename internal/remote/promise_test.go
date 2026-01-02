package remote

import (
	"testing"
	"time"
)

func TestPromiseMap_Create(t *testing.T) {
	pm := NewPromiseMap()
	defer pm.Stop()

	// First create should succeed
	if !pm.Create("key1", 100, time.Second) {
		t.Error("First Create should succeed")
	}

	// Second create for same key should fail
	if pm.Create("key1", 100, time.Second) {
		t.Error("Second Create for same key should fail")
	}

	// Create for different key should succeed
	if !pm.Create("key2", 200, time.Second) {
		t.Error("Create for different key should succeed")
	}
}

func TestPromiseMap_Get(t *testing.T) {
	pm := NewPromiseMap()
	defer pm.Stop()

	// Get non-existent key
	if pm.Get("nonexistent") != nil {
		t.Error("Get non-existent key should return nil")
	}

	// Create and get
	pm.Create("key1", 100, time.Second)
	promise := pm.Get("key1")
	if promise == nil {
		t.Fatal("Get existing key should return promise")
	}
	if promise.Key != "key1" {
		t.Errorf("Promise.Key = %q, want %q", promise.Key, "key1")
	}
	if promise.Size != 100 {
		t.Errorf("Promise.Size = %d, want %d", promise.Size, 100)
	}
}

func TestPromiseMap_Exists(t *testing.T) {
	pm := NewPromiseMap()
	defer pm.Stop()

	if pm.Exists("nonexistent") {
		t.Error("Exists should return false for non-existent key")
	}

	pm.Create("key1", 100, time.Second)
	if !pm.Exists("key1") {
		t.Error("Exists should return true for existing key")
	}
}

func TestPromiseMap_Fulfill(t *testing.T) {
	pm := NewPromiseMap()
	defer pm.Stop()

	pm.Create("key1", 100, time.Second)
	if !pm.Exists("key1") {
		t.Error("Key should exist after create")
	}

	pm.Fulfill("key1")
	if pm.Exists("key1") {
		t.Error("Key should not exist after fulfill")
	}
}

func TestPromiseMap_Expiration(t *testing.T) {
	pm := NewPromiseMap()
	defer pm.Stop()

	// Create with short TTL
	pm.Create("shortlived", 100, 50*time.Millisecond)

	// Should exist immediately
	if !pm.Exists("shortlived") {
		t.Error("Key should exist immediately after create")
	}

	// Wait for expiration
	time.Sleep(100 * time.Millisecond)

	// Should be gone (lazy cleanup on access)
	if pm.Exists("shortlived") {
		t.Error("Key should be expired")
	}
}

func TestPromiseMap_ExpiredPromiseAllowsNewCreate(t *testing.T) {
	pm := NewPromiseMap()
	defer pm.Stop()

	// Create with short TTL
	pm.Create("key1", 100, 50*time.Millisecond)

	// Wait for expiration
	time.Sleep(100 * time.Millisecond)

	// New create should succeed
	if !pm.Create("key1", 200, time.Second) {
		t.Error("Create should succeed after previous promise expired")
	}

	promise := pm.Get("key1")
	if promise.Size != 200 {
		t.Errorf("New promise size = %d, want %d", promise.Size, 200)
	}
}

func TestPromiseMap_RemainingTTL(t *testing.T) {
	pm := NewPromiseMap()
	defer pm.Stop()

	// Non-existent key
	if pm.RemainingTTL("nonexistent") != 0 {
		t.Error("RemainingTTL for non-existent key should be 0")
	}

	// Create with 1 second TTL
	pm.Create("key1", 100, time.Second)
	remaining := pm.RemainingTTL("key1")

	// Should be close to 1 second
	if remaining < 900*time.Millisecond || remaining > time.Second {
		t.Errorf("RemainingTTL = %v, want ~1s", remaining)
	}
}

func TestPromiseMap_Len(t *testing.T) {
	pm := NewPromiseMap()
	defer pm.Stop()

	if pm.Len() != 0 {
		t.Error("Empty map should have length 0")
	}

	pm.Create("key1", 100, time.Second)
	if pm.Len() != 1 {
		t.Errorf("Len = %d, want 1", pm.Len())
	}

	pm.Create("key2", 100, time.Second)
	if pm.Len() != 2 {
		t.Errorf("Len = %d, want 2", pm.Len())
	}

	pm.Fulfill("key1")
	if pm.Len() != 1 {
		t.Errorf("Len = %d, want 1", pm.Len())
	}
}

func TestPromiseMap_NegativeSize(t *testing.T) {
	pm := NewPromiseMap()
	defer pm.Stop()

	// -1 means size not specified
	pm.Create("key1", -1, time.Second)
	promise := pm.Get("key1")
	if promise.Size != -1 {
		t.Errorf("Promise.Size = %d, want -1", promise.Size)
	}
}

func TestPromiseMap_ZeroTTLUsesDefault(t *testing.T) {
	pm := NewPromiseMap()
	defer pm.Stop()

	// Zero TTL should use default (30 seconds)
	pm.Create("key1", 100, 0)
	remaining := pm.RemainingTTL("key1")

	// Should be close to 30 seconds (default)
	if remaining < 29*time.Second || remaining > 30*time.Second {
		t.Errorf("RemainingTTL = %v, want ~30s", remaining)
	}
}

func TestPromiseMap_NegativeTTLUsesDefault(t *testing.T) {
	pm := NewPromiseMap()
	defer pm.Stop()

	// Negative TTL should use default (30 seconds)
	pm.Create("key1", 100, -5*time.Second)
	remaining := pm.RemainingTTL("key1")

	// Should be close to 30 seconds (default)
	if remaining < 29*time.Second || remaining > 30*time.Second {
		t.Errorf("RemainingTTL = %v, want ~30s", remaining)
	}
}

func TestPromiseMap_StopMultipleTimes(t *testing.T) {
	pm := NewPromiseMap()

	// Should not panic when called multiple times
	pm.Stop()
	pm.Stop()
	pm.Stop()
}

func TestPromiseMap_ConcurrentAccess(t *testing.T) {
	pm := NewPromiseMap()
	defer pm.Stop()

	done := make(chan bool)

	// Multiple goroutines trying to create promises
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				key := "concurrent-key"
				pm.Create(key, int64(id), 10*time.Millisecond)
				pm.Exists(key)
				pm.Get(key)
				pm.RemainingTTL(key)
			}
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

