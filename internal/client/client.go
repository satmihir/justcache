// Package client provides a Go client for the JustCache server.
package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/satmihir/justcache/internal/retry"
)

// Header names used by the protocol
const (
	headerSize       = "x-jc-size"
	headerTTL        = "x-jc-ttl"
	headerSuperhot   = "x-jc-superhot"
	headerPromiseTTL = "x-jc-promise-ttl"
	headerDryRun     = "x-jc-dryrun"
	headerRetryAfter = "Retry-After"
)

// Errors returned by the client
var (
	ErrNotFound            = errors.New("key not found")
	ErrConflict            = errors.New("promise conflict: another client is uploading")
	ErrNoPromise           = errors.New("no active promise for key")
	ErrSizeMismatch        = errors.New("content length does not match promised size")
	ErrInsufficientStorage = errors.New("insufficient storage capacity")
	ErrPayloadTooLarge     = errors.New("payload exceeds maximum size")
	ErrLengthRequired      = errors.New("content-length header required")
	ErrBadRequest          = errors.New("bad request")
)

// Entry represents a cached value with metadata
type Entry struct {
	Value        []byte
	Size         int
	RemainingTTL time.Duration
	Superhot     bool
}

// PostResult represents the result of a POST (promise) request
type PostResult struct {
	// Status indicates the outcome
	Status PostStatus
	// PromiseTTL is the TTL of the promise (on Accepted or Conflict)
	PromiseTTL time.Duration
	// RetryAfter is the suggested backoff (on Conflict)
	RetryAfter time.Duration
	// Entry contains metadata if Status is Exists.
	// NOTE: Entry.Value will be empty; use Get() to fetch the actual value.
	Entry *Entry
}

// PostStatus represents the outcome of a POST request
type PostStatus int

const (
	// PostAccepted means the server accepted the promise; client should PUT
	PostAccepted PostStatus = iota
	// PostExists means the key already exists; client should call Get() to fetch the value.
	// The Entry in PostResult contains metadata (size, TTL) but NOT the value itself.
	PostExists
	// PostConflict means another client has a promise; client should wait and retry
	PostConflict
	// PostInsufficientStorage means the server can't accept this value size
	PostInsufficientStorage
)

// Client is a JustCache client for a single server
type Client struct {
	baseURL     string
	httpClient  *http.Client
	retryConfig retry.Config
}

// Option configures the client
type Option func(*Client)

// WithHTTPClient sets a custom HTTP client
func WithHTTPClient(c *http.Client) Option {
	return func(client *Client) {
		client.httpClient = c
	}
}

// WithTimeout sets the HTTP client timeout
func WithTimeout(d time.Duration) Option {
	return func(client *Client) {
		client.httpClient.Timeout = d
	}
}

// WithRetryConfig sets the retry configuration
func WithRetryConfig(config retry.Config) Option {
	return func(client *Client) {
		client.retryConfig = config
	}
}

// New creates a new Client for the given server address
func New(serverAddr string, opts ...Option) *Client {
	c := &Client{
		baseURL: serverAddr,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		retryConfig: retry.DefaultConfig(),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Get retrieves a value from the cache.
// Returns ErrNotFound if the key doesn't exist.
func (c *Client) Get(ctx context.Context, key string) (*Entry, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url(key), nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		value, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("reading response body: %w", err)
		}
		return parseEntry(resp, value), nil
	case http.StatusNotFound:
		return nil, ErrNotFound
	default:
		return nil, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}
}

// Set stores a value in the cache. This handles the full POST+PUT flow.
// Returns ErrConflict if another client is uploading the same key.
// For automatic retry on conflict, use SetWithRetry.
func (c *Client) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	// Step 1: POST to create promise
	result, err := c.Post(ctx, key, int64(len(value)), 0, false)
	if err != nil {
		return err
	}

	switch result.Status {
	case PostAccepted:
		// Step 2: PUT the value
		return c.Put(ctx, key, value, ttl)
	case PostExists:
		// Key already exists - treat as success (idempotent)
		return nil
	case PostConflict:
		return ErrConflict
	case PostInsufficientStorage:
		return ErrInsufficientStorage
	default:
		return fmt.Errorf("unexpected POST status: %d", result.Status)
	}
}

// SetWithRetry stores a value with automatic retry on conflict.
// It uses exponential backoff with jitter, respecting server-provided Retry-After hints.
func (c *Client) SetWithRetry(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	_, err := retry.DoWithHint(ctx, c.retryConfig, func() (struct{}, error, bool, time.Duration) {
		result, err := c.Post(ctx, key, int64(len(value)), 0, false)
		if err != nil {
			// Network/transport errors are retryable
			return struct{}{}, err, true, 0
		}

		switch result.Status {
		case PostAccepted:
			// Got the promise, now PUT
			err := c.Put(ctx, key, value, ttl)
			if err != nil {
				// PUT errors are generally not retryable (promise is consumed)
				return struct{}{}, err, false, 0
			}
			return struct{}{}, nil, false, 0

		case PostExists:
			// Key already exists - success
			return struct{}{}, nil, false, 0

		case PostConflict:
			// Another client has the promise - retry with server hint
			return struct{}{}, ErrConflict, true, result.RetryAfter

		case PostInsufficientStorage:
			// Terminal error - don't retry
			return struct{}{}, ErrInsufficientStorage, false, 0

		default:
			return struct{}{}, fmt.Errorf("unexpected POST status: %d", result.Status), false, 0
		}
	})
	return err
}

// GetWithRetry retrieves a value with automatic retry on transient errors.
func (c *Client) GetWithRetry(ctx context.Context, key string) (*Entry, error) {
	return retry.Do(ctx, c.retryConfig, func() (*Entry, error, bool) {
		entry, err := c.Get(ctx, key)
		if err != nil {
			// NotFound is not retryable
			if errors.Is(err, ErrNotFound) {
				return nil, err, false
			}
			// Other errors (network, etc.) are retryable
			return nil, err, true
		}
		return entry, nil, false
	})
}

// PostOptions configures a POST request
type PostOptions struct {
	// Size is the expected value size (optional but recommended)
	Size int64
	// PromiseTTL is the desired promise TTL (0 for server default)
	PromiseTTL time.Duration
	// DryRun if true, returns decision without creating promise
	DryRun bool
}

// Post creates a promise to upload a value.
// This is the low-level method; most callers should use Set.
//
// If the key already exists (PostExists), Entry contains metadata but NOT the value.
// Call Get() to retrieve the actual value.
func (c *Client) Post(ctx context.Context, key string, size int64, promiseTTL time.Duration, dryRun bool) (*PostResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url(key), nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	if size > 0 {
		req.Header.Set(headerSize, strconv.FormatInt(size, 10))
	}
	if promiseTTL > 0 {
		req.Header.Set(headerPromiseTTL, strconv.FormatInt(promiseTTL.Milliseconds(), 10))
	}
	if dryRun {
		req.Header.Set(headerDryRun, "true")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	result := &PostResult{
		PromiseTTL: parsePromiseTTL(resp),
		RetryAfter: parseRetryAfter(resp),
	}

	switch resp.StatusCode {
	case http.StatusOK:
		// Key exists
		result.Status = PostExists
		value, _ := io.ReadAll(resp.Body)
		result.Entry = parseEntry(resp, value)
	case http.StatusAccepted:
		result.Status = PostAccepted
	case http.StatusConflict:
		result.Status = PostConflict
	case http.StatusInsufficientStorage:
		result.Status = PostInsufficientStorage
	default:
		return nil, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	return result, nil
}

// Put uploads a value after a successful POST.
// This is the low-level method; most callers should use Set.
func (c *Client) Put(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.url(key), bytes.NewReader(value))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	req.ContentLength = int64(len(value))
	if ttl > 0 {
		req.Header.Set(headerTTL, strconv.FormatInt(ttl.Milliseconds(), 10))
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	case http.StatusConflict:
		return ErrNoPromise
	case http.StatusLengthRequired:
		return ErrLengthRequired
	case http.StatusRequestEntityTooLarge:
		return ErrPayloadTooLarge
	case http.StatusInsufficientStorage:
		return ErrInsufficientStorage
	case http.StatusBadRequest:
		return ErrBadRequest
	default:
		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}
}

// url constructs the full URL for a cache key
func (c *Client) url(key string) string {
	return c.baseURL + "/cache/" + url.PathEscape(key)
}

// parseEntry extracts metadata from response headers
func parseEntry(resp *http.Response, value []byte) *Entry {
	entry := &Entry{
		Value: value,
		Size:  len(value),
	}

	if sizeStr := resp.Header.Get(headerSize); sizeStr != "" {
		if size, err := strconv.Atoi(sizeStr); err == nil {
			entry.Size = size
		}
	}

	if ttlStr := resp.Header.Get(headerTTL); ttlStr != "" {
		if ttlMs, err := strconv.ParseInt(ttlStr, 10, 64); err == nil {
			entry.RemainingTTL = time.Duration(ttlMs) * time.Millisecond
		}
	}

	entry.Superhot = resp.Header.Get(headerSuperhot) == "true"

	return entry
}

// parsePromiseTTL extracts promise TTL from response headers
func parsePromiseTTL(resp *http.Response) time.Duration {
	if ttlStr := resp.Header.Get(headerPromiseTTL); ttlStr != "" {
		if ttlMs, err := strconv.ParseInt(ttlStr, 10, 64); err == nil {
			return time.Duration(ttlMs) * time.Millisecond
		}
	}
	return 0
}

// parseRetryAfter extracts Retry-After from response headers
func parseRetryAfter(resp *http.Response) time.Duration {
	if retryStr := resp.Header.Get(headerRetryAfter); retryStr != "" {
		if seconds, err := strconv.Atoi(retryStr); err == nil {
			return time.Duration(seconds) * time.Second
		}
	}
	return 0
}
