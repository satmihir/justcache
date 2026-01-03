package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBackoff_ExponentialGrowth(t *testing.T) {
	b := New(Config{
		InitialDelay:   100 * time.Millisecond,
		MaxDelay:       10 * time.Second,
		Multiplier:     2.0,
		MaxAttempts:    10,
		JitterFraction: 0, // No jitter for predictable testing
	})

	expected := []time.Duration{
		100 * time.Millisecond,  // attempt 1
		200 * time.Millisecond,  // attempt 2
		400 * time.Millisecond,  // attempt 3
		800 * time.Millisecond,  // attempt 4
		1600 * time.Millisecond, // attempt 5
	}

	for i, want := range expected {
		got := b.Next(0)
		if got != want {
			t.Errorf("Attempt %d: got %v, want %v", i+1, got, want)
		}
	}
}

func TestBackoff_MaxDelayCap(t *testing.T) {
	b := New(Config{
		InitialDelay:   1 * time.Second,
		MaxDelay:       5 * time.Second,
		Multiplier:     2.0,
		MaxAttempts:    10,
		JitterFraction: 0,
	})

	// 1s, 2s, 4s, 5s (capped), 5s (capped)
	b.Next(0) // 1s
	b.Next(0) // 2s
	b.Next(0) // 4s

	got := b.Next(0) // Should be capped at 5s
	if got != 5*time.Second {
		t.Errorf("Got %v, want 5s (capped)", got)
	}

	got = b.Next(0) // Still capped
	if got != 5*time.Second {
		t.Errorf("Got %v, want 5s (capped)", got)
	}
}

func TestBackoff_MaxAttempts(t *testing.T) {
	b := New(Config{
		InitialDelay:   100 * time.Millisecond,
		MaxDelay:       10 * time.Second,
		Multiplier:     2.0,
		MaxAttempts:    3,
		JitterFraction: 0,
	})

	b.Next(0) // attempt 1
	b.Next(0) // attempt 2
	b.Next(0) // attempt 3

	got := b.Next(0) // attempt 4 - should return 0
	if got != 0 {
		t.Errorf("Got %v, want 0 (exhausted)", got)
	}

	if !b.Exhausted() {
		t.Error("Exhausted() should return true")
	}
}

func TestBackoff_JitterRange(t *testing.T) {
	b := New(Config{
		InitialDelay:   1 * time.Second,
		MaxDelay:       10 * time.Second,
		Multiplier:     2.0,
		MaxAttempts:    100,
		JitterFraction: 0.2, // ±20%
	})

	// Run many iterations and check all are within expected range
	for i := 0; i < 100; i++ {
		b.Reset()
		delay := b.Next(0)

		// Base delay is 1s, with ±20% jitter = 800ms to 1200ms
		minExpected := 800 * time.Millisecond
		maxExpected := 1200 * time.Millisecond

		if delay < minExpected || delay > maxExpected {
			t.Errorf("Iteration %d: delay %v outside range [%v, %v]", i, delay, minExpected, maxExpected)
		}
	}
}

func TestBackoff_ServerHint(t *testing.T) {
	b := New(Config{
		InitialDelay:   100 * time.Millisecond,
		MaxDelay:       10 * time.Second,
		Multiplier:     2.0,
		MaxAttempts:    10,
		JitterFraction: 0,
	})

	// Server hint larger than calculated delay
	got := b.Next(5 * time.Second)
	if got != 5*time.Second {
		t.Errorf("Got %v, want 5s (server hint)", got)
	}

	// Server hint smaller than calculated delay
	b.Reset()
	got = b.Next(50 * time.Millisecond)
	if got != 100*time.Millisecond {
		t.Errorf("Got %v, want 100ms (calculated > hint)", got)
	}
}

func TestBackoff_Reset(t *testing.T) {
	b := New(Config{
		InitialDelay:   100 * time.Millisecond,
		MaxDelay:       10 * time.Second,
		Multiplier:     2.0,
		MaxAttempts:    5,
		JitterFraction: 0,
	})

	b.Next(0) // 100ms
	b.Next(0) // 200ms
	b.Next(0) // 400ms

	b.Reset()

	got := b.Next(0)
	if got != 100*time.Millisecond {
		t.Errorf("After reset: got %v, want 100ms", got)
	}
}

func TestBackoff_Attempt(t *testing.T) {
	b := NewWithDefaults()

	if b.Attempt() != 0 {
		t.Error("Initial attempt should be 0")
	}

	b.Next(0)
	if b.Attempt() != 1 {
		t.Error("After Next(), attempt should be 1")
	}

	b.Next(0)
	if b.Attempt() != 2 {
		t.Error("After second Next(), attempt should be 2")
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.InitialDelay != 100*time.Millisecond {
		t.Errorf("InitialDelay = %v, want 100ms", cfg.InitialDelay)
	}
	if cfg.MaxDelay != 10*time.Second {
		t.Errorf("MaxDelay = %v, want 10s", cfg.MaxDelay)
	}
	if cfg.Multiplier != 2.0 {
		t.Errorf("Multiplier = %v, want 2.0", cfg.Multiplier)
	}
	if cfg.MaxAttempts != 5 {
		t.Errorf("MaxAttempts = %v, want 5", cfg.MaxAttempts)
	}
	if cfg.JitterFraction != 0.2 {
		t.Errorf("JitterFraction = %v, want 0.2", cfg.JitterFraction)
	}
}

func TestDo_Success(t *testing.T) {
	attempts := 0
	result, err := Do(context.Background(), DefaultConfig(), func() (string, error, bool) {
		attempts++
		return "success", nil, false
	})

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result != "success" {
		t.Errorf("Result = %q, want %q", result, "success")
	}
	if attempts != 1 {
		t.Errorf("Attempts = %d, want 1", attempts)
	}
}

func TestDo_RetryThenSuccess(t *testing.T) {
	attempts := 0
	result, err := Do(context.Background(), Config{
		InitialDelay:   1 * time.Millisecond,
		MaxAttempts:    5,
		JitterFraction: 0,
	}, func() (string, error, bool) {
		attempts++
		if attempts < 3 {
			return "", errors.New("fail"), true // retry
		}
		return "success", nil, false
	})

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result != "success" {
		t.Errorf("Result = %q, want %q", result, "success")
	}
	if attempts != 3 {
		t.Errorf("Attempts = %d, want 3", attempts)
	}
}

func TestDo_ExhaustedRetries(t *testing.T) {
	attempts := 0
	_, err := Do(context.Background(), Config{
		InitialDelay:   1 * time.Millisecond,
		MaxAttempts:    3,
		JitterFraction: 0,
	}, func() (string, error, bool) {
		attempts++
		return "", errors.New("always fail"), true
	})

	if err == nil {
		t.Error("Expected error after exhausted retries")
	}
	if attempts != 3 {
		t.Errorf("Attempts = %d, want 3", attempts)
	}
}

func TestDo_NoRetry(t *testing.T) {
	attempts := 0
	_, err := Do(context.Background(), DefaultConfig(), func() (string, error, bool) {
		attempts++
		return "", errors.New("no retry"), false // shouldRetry = false
	})

	if err == nil {
		t.Error("Expected error")
	}
	if attempts != 1 {
		t.Errorf("Attempts = %d, want 1 (no retry)", attempts)
	}
}

func TestDo_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	attempts := 0
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_, err := Do(ctx, Config{
		InitialDelay:   100 * time.Millisecond, // Longer than cancel delay
		MaxAttempts:    10,
		JitterFraction: 0,
	}, func() (string, error, bool) {
		attempts++
		return "", errors.New("fail"), true
	})

	if !errors.Is(err, context.Canceled) {
		t.Errorf("Error = %v, want context.Canceled", err)
	}
}

func TestDoWithHint_RespectsServerHint(t *testing.T) {
	start := time.Now()
	attempts := 0

	_, err := DoWithHint(context.Background(), Config{
		InitialDelay:   1 * time.Millisecond,
		MaxAttempts:    2,
		JitterFraction: 0,
	}, func() (string, error, bool, time.Duration) {
		attempts++
		if attempts < 2 {
			return "", errors.New("fail"), true, 50 * time.Millisecond // server says wait 50ms
		}
		return "success", nil, false, 0
	})

	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if elapsed < 50*time.Millisecond {
		t.Errorf("Elapsed = %v, expected >= 50ms (server hint)", elapsed)
	}
}
