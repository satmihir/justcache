package rendezvous

import (
	"github.com/zeebo/xxh3"
)

var (
	DefaultUnsaltedHash64 Hash64 = NewXXH3Hash64(nil)
)

// Hash64 defines the hashing operations needed for rendezvous routing.
type Hash64 interface {
	// Hash64 computes a 64-bit hash of the given bytes.
	Hash64(data []byte) uint64
}

// HashConfig contains configuration for hashing operations.
type HashConfig struct {
	Salt []byte
}

func NewHashConfig(salt []byte) *HashConfig {
	return &HashConfig{Salt: salt}
}

// XXH3Hash64 is a Hash64 implementation using xxhash3.
type XXH3Hash64 struct {
	seed uint64
}

func NewXXH3Hash64(config *HashConfig) *XXH3Hash64 {
	h := &XXH3Hash64{}
	if config != nil && len(config.Salt) > 0 {
		// Hash the salt down to a 64-bit seed
		h.seed = xxh3.Hash(config.Salt)
	}
	return h
}

func (x *XXH3Hash64) Hash64(data []byte) uint64 {
	return xxh3.HashSeed(data, x.seed)
}
