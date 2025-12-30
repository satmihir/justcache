package rendezvous

import (
	"testing"
)

func TestNewHashConfig(t *testing.T) {
	tests := []struct {
		name string
		salt []byte
	}{
		{
			name: "nil salt",
			salt: nil,
		},
		{
			name: "empty salt",
			salt: []byte{},
		},
		{
			name: "short salt",
			salt: []byte("abc"),
		},
		{
			name: "long salt",
			salt: []byte("this-is-a-very-long-salt-value-for-testing"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := NewHashConfig(tt.salt)
			if config == nil {
				t.Fatal("expected non-nil config")
			}
			if len(tt.salt) > 0 && len(config.Salt) != len(tt.salt) {
				t.Errorf("expected salt length %d, got %d", len(tt.salt), len(config.Salt))
			}
		})
	}
}

func TestNewXXH3Hash64(t *testing.T) {
	tests := []struct {
		name   string
		config *HashConfig
	}{
		{
			name:   "nil config",
			config: nil,
		},
		{
			name:   "config with nil salt",
			config: &HashConfig{Salt: nil},
		},
		{
			name:   "config with empty salt",
			config: &HashConfig{Salt: []byte{}},
		},
		{
			name:   "config with salt",
			config: &HashConfig{Salt: []byte("test-salt")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hasher := NewXXH3Hash64(tt.config)
			if hasher == nil {
				t.Fatal("expected non-nil hasher")
			}
		})
	}
}

func TestXXH3Hash64_Determinism(t *testing.T) {
	tests := []struct {
		name  string
		salt  []byte
		input []byte
	}{
		{
			name:  "no salt, short input",
			salt:  nil,
			input: []byte("hello"),
		},
		{
			name:  "no salt, empty input",
			salt:  nil,
			input: []byte{},
		},
		{
			name:  "with salt, short input",
			salt:  []byte("my-salt"),
			input: []byte("hello"),
		},
		{
			name:  "with salt, long input",
			salt:  []byte("my-salt"),
			input: []byte("this is a much longer input string for testing determinism"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hasher := NewXXH3Hash64(NewHashConfig(tt.salt))

			// Hash the same input multiple times
			hash1 := hasher.Hash64(tt.input)
			hash2 := hasher.Hash64(tt.input)
			hash3 := hasher.Hash64(tt.input)

			if hash1 != hash2 || hash2 != hash3 {
				t.Errorf("hash not deterministic: got %d, %d, %d", hash1, hash2, hash3)
			}
		})
	}
}

func TestXXH3Hash64_DifferentInputs(t *testing.T) {
	hasher := NewXXH3Hash64(nil)

	tests := []struct {
		name   string
		input1 []byte
		input2 []byte
	}{
		{
			name:   "single char difference",
			input1: []byte("hello"),
			input2: []byte("hallo"),
		},
		{
			name:   "length difference",
			input1: []byte("hello"),
			input2: []byte("hello!"),
		},
		{
			name:   "completely different",
			input1: []byte("abc"),
			input2: []byte("xyz"),
		},
		{
			name:   "empty vs non-empty",
			input1: []byte{},
			input2: []byte("a"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash1 := hasher.Hash64(tt.input1)
			hash2 := hasher.Hash64(tt.input2)

			if hash1 == hash2 {
				t.Errorf("different inputs produced same hash: %d", hash1)
			}
		})
	}
}

func TestXXH3Hash64_SaltAffectsOutput(t *testing.T) {
	input := []byte("test-input")

	tests := []struct {
		name  string
		salt1 []byte
		salt2 []byte
	}{
		{
			name:  "nil vs non-nil salt",
			salt1: nil,
			salt2: []byte("salt"),
		},
		{
			name:  "different salts",
			salt1: []byte("salt-a"),
			salt2: []byte("salt-b"),
		},
		{
			name:  "similar salts",
			salt1: []byte("salt1"),
			salt2: []byte("salt2"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hasher1 := NewXXH3Hash64(NewHashConfig(tt.salt1))
			hasher2 := NewXXH3Hash64(NewHashConfig(tt.salt2))

			hash1 := hasher1.Hash64(input)
			hash2 := hasher2.Hash64(input)

			if hash1 == hash2 {
				t.Errorf("different salts produced same hash: %d", hash1)
			}
		})
	}
}

func TestXXH3Hash64_SameSaltSameHasher(t *testing.T) {
	salt := []byte("consistent-salt")
	input := []byte("test-input")

	// Create two hashers with the same salt
	hasher1 := NewXXH3Hash64(NewHashConfig(salt))
	hasher2 := NewXXH3Hash64(NewHashConfig(salt))

	hash1 := hasher1.Hash64(input)
	hash2 := hasher2.Hash64(input)

	if hash1 != hash2 {
		t.Errorf("same salt produced different hashes: %d vs %d", hash1, hash2)
	}
}

func TestDefaultUnsaltedHash64(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
	}{
		{
			name:  "simple input",
			input: []byte("hello"),
		},
		{
			name:  "empty input",
			input: []byte{},
		},
		{
			name:  "long input",
			input: []byte("this is a longer test input for the default hasher"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Should be deterministic
			hash1 := DefaultUnsaltedHash64.Hash64(tt.input)
			hash2 := DefaultUnsaltedHash64.Hash64(tt.input)

			if hash1 != hash2 {
				t.Errorf("DefaultUnsaltedHash64 not deterministic: %d vs %d", hash1, hash2)
			}

			// Should match a fresh unsalted hasher
			freshHasher := NewXXH3Hash64(nil)
			freshHash := freshHasher.Hash64(tt.input)

			if hash1 != freshHash {
				t.Errorf("DefaultUnsaltedHash64 differs from fresh unsalted hasher: %d vs %d", hash1, freshHash)
			}
		})
	}
}

func TestHash64Interface(t *testing.T) {
	// Verify XXH3Hash64 implements Hash64 interface
	var _ Hash64 = (*XXH3Hash64)(nil)
	var _ Hash64 = NewXXH3Hash64(nil)
}
