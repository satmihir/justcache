package remote

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/satmihir/justcache/internal/constants"
	"github.com/satmihir/justcache/internal/storage"
)

const (
	// Path prefix for cache operations
	cachePathPrefix = "/cache/"

	// Header names
	headerSize       = "x-jc-size"
	headerTTL        = "x-jc-ttl"
	headerSuperhot   = "x-jc-superhot"
	headerDryRun     = "x-jc-dryrun"
	headerPromiseTTL = "x-jc-promise-ttl"
	headerRetryAfter = "Retry-After"

	// Default TTL for PUT operations (30 minutes)
	defaultTTL = 30 * time.Minute
)

// CacheServer represents the HTTP server for the cache
type CacheServer struct {
	addr     string
	mux      *http.ServeMux
	storage  storage.LocalStorage
	promises *PromiseMap
}

// NewCacheServer creates a new CacheServer instance
func NewCacheServer(addr string, store storage.LocalStorage) *CacheServer {
	s := &CacheServer{
		addr:     addr,
		mux:      http.NewServeMux(),
		storage:  store,
		promises: NewPromiseMap(),
	}
	s.registerRoutes()
	return s
}

// Stop stops the CacheServer and cleans up resources
func (s *CacheServer) Stop() {
	s.promises.Stop()
}

// registerRoutes sets up the HTTP routes
func (s *CacheServer) registerRoutes() {
	s.mux.HandleFunc("/", s.handleRequest)
}

// handleRequest routes requests based on HTTP method
func (s *CacheServer) handleRequest(w http.ResponseWriter, r *http.Request) {
	// Parse the key from the path
	key, err := parseKeyFromPath(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.handleGet(w, r, key)
	case http.MethodPost:
		s.handlePost(w, r, key)
	case http.MethodPut:
		s.handlePut(w, r, key)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// parseKeyFromPath extracts the cache key from the URL path
// Expected format: /cache/{key}
func parseKeyFromPath(path string) (string, error) {
	if !strings.HasPrefix(path, cachePathPrefix) {
		return "", errors.New("invalid path: must start with /cache/")
	}

	key := strings.TrimPrefix(path, cachePathPrefix)
	if key == "" {
		return "", errors.New("invalid path: key cannot be empty")
	}

	return key, nil
}

// handleGet handles GET requests
// Returns 200 OK with value on hit, 404 Not Found on miss
func (s *CacheServer) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	entry, err := s.storage.Get(key)
	if err != nil {
		if errors.Is(err, storage.ErrKeyNotFound) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	setResponseHeaders(w, entry)
	w.WriteHeader(http.StatusOK)
	w.Write(entry.Value)
}

// handlePost handles POST requests for intent/promise coordination
// Response codes:
// - 200 OK: key already exists, client should GET it
// - 202 Accepted: server requests an upload, client should PUT
// - 409 Conflict: another client is uploading (promise exists)
// - 507 Insufficient Storage: cannot accept this key/value
func (s *CacheServer) handlePost(w http.ResponseWriter, r *http.Request, key string) {
	// Check if key already exists in cache
	entry, err := s.storage.Get(key)
	if err == nil {
		// Key exists, client should GET it
		setResponseHeaders(w, entry)
		w.WriteHeader(http.StatusOK)
		return
	}

	if !errors.Is(err, storage.ErrKeyNotFound) {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Parse x-jc-size header
	var valueSize int64 = -1
	if sizeHeader := r.Header.Get(headerSize); sizeHeader != "" {
		var parseErr error
		valueSize, parseErr = strconv.ParseInt(sizeHeader, 10, 64)
		if parseErr != nil || valueSize < 0 {
			http.Error(w, "Invalid x-jc-size header: must be non-negative integer", http.StatusBadRequest)
			return
		}

		// Early rejection if value is too large
		if !s.storage.CanFit(len(key), int(valueSize)) {
			http.Error(w, "Value too large for storage capacity", http.StatusInsufficientStorage)
			return
		}
	}

	// Parse x-jc-promise-ttl header for custom promise TTL
	promiseTTL := defaultPromiseTTL
	if ttlHeader := r.Header.Get(headerPromiseTTL); ttlHeader != "" {
		ttlMs, parseErr := strconv.ParseInt(ttlHeader, 10, 64)
		if parseErr != nil || ttlMs <= 0 {
			http.Error(w, "Invalid x-jc-promise-ttl header: must be positive integer (milliseconds)", http.StatusBadRequest)
			return
		}
		promiseTTL = time.Duration(ttlMs) * time.Millisecond
	}

	// Check x-jc-dryrun header
	dryRun := r.Header.Get(headerDryRun) == "true"

	// Check if a promise already exists for this key
	if existingPromise := s.promises.Get(key); existingPromise != nil {
		// Another client is already uploading
		remainingTTL := s.promises.RemainingTTL(key)
		w.Header().Set(headerPromiseTTL, strconv.FormatInt(remainingTTL.Milliseconds(), 10))
		w.Header().Set(headerRetryAfter, strconv.Itoa(int(remainingTTL.Seconds())+1))
		w.WriteHeader(http.StatusConflict)
		return
	}

	// If dry run, don't create the promise
	if dryRun {
		w.Header().Set(headerPromiseTTL, strconv.FormatInt(promiseTTL.Milliseconds(), 10))
		w.WriteHeader(http.StatusAccepted)
		return
	}

	// Try to create the promise
	if !s.promises.Create(key, valueSize, promiseTTL) {
		// Race condition: another client created promise between check and create
		remainingTTL := s.promises.RemainingTTL(key)
		w.Header().Set(headerPromiseTTL, strconv.FormatInt(remainingTTL.Milliseconds(), 10))
		w.Header().Set(headerRetryAfter, strconv.Itoa(int(remainingTTL.Seconds())+1))
		w.WriteHeader(http.StatusConflict)
		return
	}

	// Promise created successfully
	w.Header().Set(headerPromiseTTL, strconv.FormatInt(promiseTTL.Milliseconds(), 10))
	w.WriteHeader(http.StatusAccepted)
}

// handlePut handles PUT requests to upload values
// Response codes:
// - 200 OK: value stored successfully
// - 409 Conflict: upload rejected (no promise, wrong owner, size mismatch)
// - 411 Length Required: missing Content-Length
// - 413 Payload Too Large: exceeds server limits
// - 507 Insufficient Storage: capacity exceeded
func (s *CacheServer) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	// Check Content-Length header
	if r.ContentLength < 0 {
		http.Error(w, "Content-Length required", http.StatusLengthRequired)
		return
	}

	// Reject immediately if Content-Length exceeds hard cap
	if r.ContentLength > constants.MaxValueSizeBytes {
		http.Error(w, "Payload exceeds maximum allowed size", http.StatusRequestEntityTooLarge)
		return
	}

	// Check if a promise exists for this key
	promise := s.promises.Get(key)
	if promise == nil {
		http.Error(w, "No active promise for this key; call POST first", http.StatusConflict)
		return
	}

	// Check size matches if promise specified a size
	if promise.Size >= 0 && r.ContentLength != promise.Size {
		// Terminal error: size mismatch - release promise for other writers
		s.promises.Fulfill(key)
		http.Error(w, "Content-Length does not match promised size", http.StatusConflict)
		return
	}

	// Wrap body with MaxBytesReader to enforce hard cap (defense in depth)
	// This protects against malicious clients that lie about Content-Length
	r.Body = http.MaxBytesReader(w, r.Body, constants.MaxValueSizeBytes)

	// Read the request body
	value, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		// MaxBytesReader returns a specific error when limit is exceeded
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			// Terminal error: payload too large - release promise
			s.promises.Fulfill(key)
			http.Error(w, "Payload exceeds maximum allowed size", http.StatusRequestEntityTooLarge)
			return
		}
		// Transient error: keep promise (client may retry)
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	// Verify we read exactly Content-Length bytes (detect truncated uploads)
	if int64(len(value)) != r.ContentLength {
		// Client disconnected or sent fewer bytes than promised - transient error
		http.Error(w, "Incomplete request body", http.StatusBadRequest)
		return
	}

	// Parse TTL from header, default to 30 minutes
	ttl := defaultTTL
	if ttlHeader := r.Header.Get(headerTTL); ttlHeader != "" {
		ttlMs, parseErr := strconv.ParseInt(ttlHeader, 10, 64)
		if parseErr != nil || ttlMs <= 0 {
			// Transient error: invalid header can be fixed by client
			http.Error(w, "Invalid x-jc-ttl header: must be positive integer (milliseconds)", http.StatusBadRequest)
			return
		}
		ttl = time.Duration(ttlMs) * time.Millisecond
	}

	// Store the value
	err = s.storage.Put(key, value, ttl)
	if err != nil {
		// Determine if error is terminal (won't succeed on retry) or transient
		isTerminal := false
		switch {
		case errors.Is(err, storage.ErrMemoryLimitExceeded):
			// Transient: might succeed after eviction or other keys expire
			http.Error(w, err.Error(), http.StatusInsufficientStorage)
		case errors.Is(err, storage.ErrObjectTooLarge):
			// Terminal: object will never fit
			isTerminal = true
			http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
		case errors.Is(err, storage.ErrKeyTooLong), errors.Is(err, storage.ErrKeyTooShort):
			// Terminal: key is fundamentally invalid
			isTerminal = true
			http.Error(w, err.Error(), http.StatusBadRequest)
		case errors.Is(err, storage.ErrValueTooShort):
			// Terminal: empty value will never be accepted
			isTerminal = true
			http.Error(w, err.Error(), http.StatusBadRequest)
		default:
			// Unknown error: treat as transient
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		if isTerminal {
			s.promises.Fulfill(key)
		}
		return
	}

	// Fulfill the promise (remove it)
	s.promises.Fulfill(key)

	w.WriteHeader(http.StatusOK)
}

// setResponseHeaders sets the x-jc-* response headers
func setResponseHeaders(w http.ResponseWriter, entry *storage.CacheEntry) {
	w.Header().Set(headerSize, strconv.Itoa(entry.Size))
	w.Header().Set(headerTTL, strconv.FormatInt(entry.RemainingTTL.Milliseconds(), 10))
	w.Header().Set(headerSuperhot, "false") // TODO: implement superhot detection
}

// Start starts the CacheServer
func (s *CacheServer) Start() error {
	return http.ListenAndServe(s.addr, s.mux)
}
