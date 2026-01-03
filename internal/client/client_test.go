package client

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/satmihir/justcache/internal/remote"
	"github.com/satmihir/justcache/internal/storage"
)

func newTestServerAndClient() (*remote.CacheServer, *httptest.Server, *Client) {
	store := storage.NewInMemoryStorage(100000)
	cs := remote.NewCacheServer(":0", store)
	ts := httptest.NewServer(cs.Handler())
	client := New(ts.URL)
	return cs, ts, client
}

func TestClient_GetNotFound(t *testing.T) {
	cs, ts, client := newTestServerAndClient()
	defer ts.Close()
	defer cs.Stop()

	_, err := client.Get(context.Background(), "nonexistent")
	if err != ErrNotFound {
		t.Errorf("Get error = %v, want ErrNotFound", err)
	}
}

func TestClient_SetAndGet(t *testing.T) {
	cs, ts, client := newTestServerAndClient()
	defer ts.Close()
	defer cs.Stop()

	ctx := context.Background()

	// Set a value
	err := client.Set(ctx, "mykey", []byte("myvalue"), time.Hour)
	if err != nil {
		t.Fatalf("Set error = %v", err)
	}

	// Get it back
	entry, err := client.Get(ctx, "mykey")
	if err != nil {
		t.Fatalf("Get error = %v", err)
	}

	if string(entry.Value) != "myvalue" {
		t.Errorf("Value = %q, want %q", entry.Value, "myvalue")
	}
	if entry.Size != 7 {
		t.Errorf("Size = %d, want 7", entry.Size)
	}
	// TTL should be close to 1 hour
	if entry.RemainingTTL < 59*time.Minute || entry.RemainingTTL > time.Hour {
		t.Errorf("RemainingTTL = %v, want ~1h", entry.RemainingTTL)
	}
}

func TestClient_SetTwice(t *testing.T) {
	cs, ts, client := newTestServerAndClient()
	defer ts.Close()
	defer cs.Stop()

	ctx := context.Background()

	// Set initial value
	err := client.Set(ctx, "mykey", []byte("initial"), time.Hour)
	if err != nil {
		t.Fatalf("First Set error = %v", err)
	}

	// Set again - should succeed (POST returns 200, treated as success)
	err = client.Set(ctx, "mykey", []byte("updated"), time.Hour)
	if err != nil {
		t.Fatalf("Second Set error = %v", err)
	}

	// Value should still be initial (POST returned 200, so PUT was skipped)
	entry, _ := client.Get(ctx, "mykey")
	if string(entry.Value) != "initial" {
		t.Errorf("Value = %q, want %q", entry.Value, "initial")
	}
}

func TestClient_PostAccepted(t *testing.T) {
	cs, ts, client := newTestServerAndClient()
	defer ts.Close()
	defer cs.Stop()

	result, err := client.Post(context.Background(), "newkey", 100, 0, false)
	if err != nil {
		t.Fatalf("Post error = %v", err)
	}

	if result.Status != PostAccepted {
		t.Errorf("Status = %v, want PostAccepted", result.Status)
	}
	if result.PromiseTTL <= 0 {
		t.Error("PromiseTTL should be > 0")
	}
}

func TestClient_PostExists(t *testing.T) {
	cs, ts, client := newTestServerAndClient()
	defer ts.Close()
	defer cs.Stop()

	ctx := context.Background()

	// First set a value
	client.Set(ctx, "existingkey", []byte("value"), time.Hour)

	// POST should return Exists
	result, err := client.Post(ctx, "existingkey", 100, 0, false)
	if err != nil {
		t.Fatalf("Post error = %v", err)
	}

	if result.Status != PostExists {
		t.Errorf("Status = %v, want PostExists", result.Status)
	}
	if result.Entry == nil {
		t.Error("Entry should be populated")
	}
}

func TestClient_PostConflict(t *testing.T) {
	cs, ts, client := newTestServerAndClient()
	defer ts.Close()
	defer cs.Stop()

	ctx := context.Background()

	// First POST creates promise
	result1, _ := client.Post(ctx, "conflictkey", 100, 0, false)
	if result1.Status != PostAccepted {
		t.Fatalf("First Post status = %v, want PostAccepted", result1.Status)
	}

	// Second POST should conflict
	result2, err := client.Post(ctx, "conflictkey", 100, 0, false)
	if err != nil {
		t.Fatalf("Second Post error = %v", err)
	}

	if result2.Status != PostConflict {
		t.Errorf("Status = %v, want PostConflict", result2.Status)
	}
	if result2.RetryAfter <= 0 {
		t.Error("RetryAfter should be > 0")
	}
}

func TestClient_PostDryRun(t *testing.T) {
	cs, ts, client := newTestServerAndClient()
	defer ts.Close()
	defer cs.Stop()

	ctx := context.Background()

	// Dry run should return Accepted but not create promise
	result1, _ := client.Post(ctx, "dryrunkey", 100, 0, true)
	if result1.Status != PostAccepted {
		t.Fatalf("DryRun Post status = %v, want PostAccepted", result1.Status)
	}

	// Second POST should also succeed (no promise was created)
	result2, _ := client.Post(ctx, "dryrunkey", 100, 0, false)
	if result2.Status != PostAccepted {
		t.Errorf("Second Post status = %v, want PostAccepted", result2.Status)
	}
}

func TestClient_PutWithoutPromise(t *testing.T) {
	cs, ts, client := newTestServerAndClient()
	defer ts.Close()
	defer cs.Stop()

	err := client.Put(context.Background(), "nopromise", []byte("value"), time.Hour)
	if err != ErrNoPromise {
		t.Errorf("Put error = %v, want ErrNoPromise", err)
	}
}

func TestClient_PostAndPut(t *testing.T) {
	cs, ts, client := newTestServerAndClient()
	defer ts.Close()
	defer cs.Stop()

	ctx := context.Background()

	// POST to create promise
	result, _ := client.Post(ctx, "postputkey", 5, 0, false)
	if result.Status != PostAccepted {
		t.Fatalf("Post status = %v, want PostAccepted", result.Status)
	}

	// PUT the value
	err := client.Put(ctx, "postputkey", []byte("value"), time.Hour)
	if err != nil {
		t.Fatalf("Put error = %v", err)
	}

	// Verify
	entry, _ := client.Get(ctx, "postputkey")
	if string(entry.Value) != "value" {
		t.Errorf("Value = %q, want %q", entry.Value, "value")
	}
}

func TestClient_CustomTTL(t *testing.T) {
	cs, ts, client := newTestServerAndClient()
	defer ts.Close()
	defer cs.Stop()

	ctx := context.Background()

	// Set with 5 second TTL
	err := client.Set(ctx, "shortttl", []byte("value"), 5*time.Second)
	if err != nil {
		t.Fatalf("Set error = %v", err)
	}

	// Get and check TTL
	entry, _ := client.Get(ctx, "shortttl")
	if entry.RemainingTTL < 4*time.Second || entry.RemainingTTL > 5*time.Second {
		t.Errorf("RemainingTTL = %v, want ~5s", entry.RemainingTTL)
	}
}

func TestClient_LargeValue(t *testing.T) {
	cs, ts, client := newTestServerAndClient()
	defer ts.Close()
	defer cs.Stop()

	ctx := context.Background()

	largeValue := make([]byte, 50000)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	err := client.Set(ctx, "largekey", largeValue, time.Hour)
	if err != nil {
		t.Fatalf("Set error = %v", err)
	}

	entry, err := client.Get(ctx, "largekey")
	if err != nil {
		t.Fatalf("Get error = %v", err)
	}

	if len(entry.Value) != len(largeValue) {
		t.Errorf("Value length = %d, want %d", len(entry.Value), len(largeValue))
	}
}

func TestClient_SetWithRetry_Success(t *testing.T) {
	_, ts, client := newTestServerAndClient()
	defer ts.Close()
	ctx := context.Background()

	err := client.SetWithRetry(ctx, "retrykey", []byte("value"), time.Hour)
	if err != nil {
		t.Fatalf("SetWithRetry error = %v", err)
	}

	entry, err := client.Get(ctx, "retrykey")
	if err != nil {
		t.Fatalf("Get error = %v", err)
	}
	if string(entry.Value) != "value" {
		t.Errorf("Value = %q, want %q", string(entry.Value), "value")
	}
}

func TestClient_SetWithRetry_ExistingKey(t *testing.T) {
	_, ts, client := newTestServerAndClient()
	defer ts.Close()
	ctx := context.Background()

	// Set initial value
	err := client.Set(ctx, "existingkey", []byte("original"), time.Hour)
	if err != nil {
		t.Fatalf("Initial Set error = %v", err)
	}

	// SetWithRetry on existing key should succeed (idempotent)
	err = client.SetWithRetry(ctx, "existingkey", []byte("newvalue"), time.Hour)
	if err != nil {
		t.Fatalf("SetWithRetry on existing key error = %v", err)
	}

	// Value should still be original (POST returned 200 Exists, no PUT)
	entry, err := client.Get(ctx, "existingkey")
	if err != nil {
		t.Fatalf("Get error = %v", err)
	}
	if string(entry.Value) != "original" {
		t.Errorf("Value = %q, want %q (original)", string(entry.Value), "original")
	}
}

func TestClient_GetWithRetry_Success(t *testing.T) {
	_, ts, client := newTestServerAndClient()
	defer ts.Close()
	ctx := context.Background()

	// Set a value first
	err := client.Set(ctx, "getretrykey", []byte("hello"), time.Hour)
	if err != nil {
		t.Fatalf("Set error = %v", err)
	}

	// GetWithRetry should succeed
	entry, err := client.GetWithRetry(ctx, "getretrykey")
	if err != nil {
		t.Fatalf("GetWithRetry error = %v", err)
	}
	if string(entry.Value) != "hello" {
		t.Errorf("Value = %q, want %q", string(entry.Value), "hello")
	}
}

func TestClient_GetWithRetry_NotFound(t *testing.T) {
	_, ts, client := newTestServerAndClient()
	defer ts.Close()
	ctx := context.Background()

	// GetWithRetry on missing key should not retry and return ErrNotFound
	_, err := client.GetWithRetry(ctx, "missingkey")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Error = %v, want ErrNotFound", err)
	}
}
