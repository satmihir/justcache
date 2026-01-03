// Package retry provides exponential backoff with jitter for retry logic.
package retry

import (
	"context"
	"math"
	"math/rand"
	"sync"
	"time"
)

// Config configures the retry behavior.
type Config struct {
	// InitialDelay is the delay before the first retry.
	// Default: 100ms
	InitialDelay time.Duration

	// MaxDelay is the maximum delay between retries.
	// Default: 10s
	MaxDelay time.Duration

	// Multiplier is the factor by which delay increases after each retry.
	// Default: 2.0
	Multiplier float64

	// MaxAttempts is the maximum number of attempts (including the first).
	// 0 means infinite retries.
	// Default: 5
	MaxAttempts int

	// JitterFraction is the fraction of the delay to randomize (0.0 to 1.0).
	// E.g., 0.2 means ±20% jitter.
	// Default: 0.2
	JitterFraction float64
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		InitialDelay:   100 * time.Millisecond,
		MaxDelay:       10 * time.Second,
		Multiplier:     2.0,
		MaxAttempts:    5,
		JitterFraction: 0.2,
	}
}

// Backoff tracks retry state and calculates delays.
type Backoff struct {
	config   Config
	attempt  int
	rng      *rand.Rand
	rngMutex sync.Mutex
}

// New creates a new Backoff with the given configuration.
func New(config Config) *Backoff {
	// Apply defaults for zero values
	if config.InitialDelay <= 0 {
		config.InitialDelay = 100 * time.Millisecond
	}
	if config.MaxDelay <= 0 {
		config.MaxDelay = 10 * time.Second
	}
	if config.Multiplier <= 0 {
		config.Multiplier = 2.0
	}
	if config.JitterFraction < 0 {
		config.JitterFraction = 0
	}
	if config.JitterFraction > 1 {
		config.JitterFraction = 1
	}

	return &Backoff{
		config: config,
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// NewWithDefaults creates a new Backoff with default configuration.
func NewWithDefaults() *Backoff {
	return New(DefaultConfig())
}

// Next returns the delay before the next retry attempt.
// Returns 0 if max attempts reached.
// If serverHint > 0, it's used as a minimum delay (e.g., from Retry-After header).
func (b *Backoff) Next(serverHint time.Duration) time.Duration {
	b.attempt++

	// Check max attempts
	if b.config.MaxAttempts > 0 && b.attempt > b.config.MaxAttempts {
		return 0
	}

	// Calculate base delay with exponential backoff
	delay := float64(b.config.InitialDelay) * math.Pow(b.config.Multiplier, float64(b.attempt-1))

	// Cap at max delay
	if delay > float64(b.config.MaxDelay) {
		delay = float64(b.config.MaxDelay)
	}

	// Apply jitter: delay * (1 ± jitterFraction)
	if b.config.JitterFraction > 0 {
		b.rngMutex.Lock()
		jitter := (b.rng.Float64()*2 - 1) * b.config.JitterFraction // -jitter to +jitter
		b.rngMutex.Unlock()
		delay = delay * (1 + jitter)
	}

	result := time.Duration(delay)

	// Respect server hint if larger
	if serverHint > result {
		result = serverHint
	}

	return result
}

// Attempt returns the current attempt number (1-indexed).
func (b *Backoff) Attempt() int {
	return b.attempt
}

// Reset resets the backoff to its initial state.
func (b *Backoff) Reset() {
	b.attempt = 0
}

// Exhausted returns true if max attempts have been reached.
func (b *Backoff) Exhausted() bool {
	return b.config.MaxAttempts > 0 && b.attempt >= b.config.MaxAttempts
}

// RetryableFunc is a function that can be retried.
// It should return (result, error, shouldRetry).
// If shouldRetry is false, Do() returns immediately.
type RetryableFunc[T any] func() (T, error, bool)

// Do executes fn with retries according to the backoff configuration.
// It returns the result of the first successful call, or the last error if all retries fail.
func Do[T any](ctx context.Context, config Config, fn RetryableFunc[T]) (T, error) {
	backoff := New(config)
	var lastErr error
	var zero T
	attempt := 0

	for {
		attempt++

		// Check if we've exceeded max attempts before trying
		if config.MaxAttempts > 0 && attempt > config.MaxAttempts {
			return zero, lastErr
		}

		result, err, shouldRetry := fn()
		if err == nil {
			return result, nil
		}

		lastErr = err
		if !shouldRetry {
			return zero, lastErr
		}

		delay := backoff.Next(0)

		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-time.After(delay):
			// Continue to next attempt
		}
	}
}

// DoWithHint is like Do but allows providing a server hint for each retry.
type RetryableFuncWithHint[T any] func() (T, error, bool, time.Duration)

// DoWithHint executes fn with retries, respecting server-provided delay hints.
func DoWithHint[T any](ctx context.Context, config Config, fn RetryableFuncWithHint[T]) (T, error) {
	backoff := New(config)
	var lastErr error
	var zero T
	attempt := 0

	for {
		attempt++

		// Check if we've exceeded max attempts before trying
		if config.MaxAttempts > 0 && attempt > config.MaxAttempts {
			return zero, lastErr
		}

		result, err, shouldRetry, serverHint := fn()
		if err == nil {
			return result, nil
		}

		lastErr = err
		if !shouldRetry {
			return zero, lastErr
		}

		delay := backoff.Next(serverHint)

		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-time.After(delay):
			// Continue to next attempt
		}
	}
}
