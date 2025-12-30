package rendezvous

import (
	"fmt"
	"sync"
	"testing"
)

func TestNewNode(t *testing.T) {
	tests := []struct {
		name         string
		id           string
		port         int
		wantIdentity string
	}{
		{
			name:         "simple node",
			id:           "node1",
			port:         8080,
			wantIdentity: "node1:8080",
		},
		{
			name:         "ip address id",
			id:           "192.168.1.1",
			port:         6379,
			wantIdentity: "192.168.1.1:6379",
		},
		{
			name:         "hostname with subdomain",
			id:           "cache.example.com",
			port:         11211,
			wantIdentity: "cache.example.com:11211",
		},
		{
			name:         "empty id",
			id:           "",
			port:         8080,
			wantIdentity: ":8080",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := NewNode(tt.id, tt.port)

			if node == nil {
				t.Fatal("expected non-nil node")
			}
			if node.id != tt.id {
				t.Errorf("expected id %q, got %q", tt.id, node.id)
			}
			if node.port != tt.port {
				t.Errorf("expected port %d, got %d", tt.port, node.port)
			}
			if node.identityString != tt.wantIdentity {
				t.Errorf("expected identityString %q, got %q", tt.wantIdentity, node.identityString)
			}
			if node.identityHash == 0 {
				t.Error("expected non-zero identityHash")
			}
		})
	}
}

func TestNode_IdentityHashDeterminism(t *testing.T) {
	// Same id and port should produce same identity hash
	node1 := NewNode("test", 8080)
	node2 := NewNode("test", 8080)

	if node1.identityHash != node2.identityHash {
		t.Errorf("same node params produced different hashes: %d vs %d",
			node1.identityHash, node2.identityHash)
	}
}

func TestNode_IdentityHashUniqueness(t *testing.T) {
	tests := []struct {
		name  string
		id1   string
		port1 int
		id2   string
		port2 int
	}{
		{
			name:  "different ids",
			id1:   "node1",
			port1: 8080,
			id2:   "node2",
			port2: 8080,
		},
		{
			name:  "different ports",
			id1:   "node1",
			port1: 8080,
			id2:   "node1",
			port2: 8081,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node1 := NewNode(tt.id1, tt.port1)
			node2 := NewNode(tt.id2, tt.port2)

			if node1.identityHash == node2.identityHash {
				t.Errorf("different nodes produced same hash: %d", node1.identityHash)
			}
		})
	}
}

func TestNewRendezvousRouter(t *testing.T) {
	tests := []struct {
		name       string
		nodes      []*Node
		hashConfig *HashConfig
	}{
		{
			name:       "nil nodes, nil config",
			nodes:      nil,
			hashConfig: nil,
		},
		{
			name:       "empty nodes, nil config",
			nodes:      []*Node{},
			hashConfig: nil,
		},
		{
			name:       "with nodes, nil config",
			nodes:      []*Node{NewNode("n1", 8080), NewNode("n2", 8081)},
			hashConfig: nil,
		},
		{
			name:       "with nodes and config",
			nodes:      []*Node{NewNode("n1", 8080)},
			hashConfig: NewHashConfig([]byte("salt")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := NewRendezvousRouter(tt.nodes, tt.hashConfig)

			if router == nil {
				t.Fatal("expected non-nil router")
			}
			if router.hasher == nil {
				t.Error("expected non-nil hasher")
			}
		})
	}
}

func TestRendezvousRouter_SetNodes(t *testing.T) {
	router := NewRendezvousRouter(nil, nil)

	// Set some nodes
	nodes := []*Node{NewNode("a", 1), NewNode("b", 2)}
	router.SetNodes(nodes)

	// Verify nodes are copied (modifying original doesn't affect router)
	originalNode := nodes[0]
	nodes[0] = NewNode("c", 3)

	result := router.GetNodes([]byte("key"), 1)
	if result[0] != originalNode {
		t.Error("SetNodes should copy nodes, not store reference")
	}
}

func TestRendezvousRouter_GetNodes_EdgeCases(t *testing.T) {
	nodes := []*Node{
		NewNode("n1", 8080),
		NewNode("n2", 8081),
		NewNode("n3", 8082),
	}

	tests := []struct {
		name    string
		nodes   []*Node
		key     string
		k       int
		wantLen int
		wantNil bool
	}{
		{
			name:    "empty nodes",
			nodes:   []*Node{},
			key:     "test",
			k:       1,
			wantNil: true,
		},
		{
			name:    "nil nodes",
			nodes:   nil,
			key:     "test",
			k:       1,
			wantNil: true,
		},
		{
			name:    "k=0",
			nodes:   nodes,
			key:     "test",
			k:       0,
			wantNil: true,
		},
		{
			name:    "k negative",
			nodes:   nodes,
			key:     "test",
			k:       -1,
			wantNil: true,
		},
		{
			name:    "k=1",
			nodes:   nodes,
			key:     "test",
			k:       1,
			wantLen: 1,
		},
		{
			name:    "k=2",
			nodes:   nodes,
			key:     "test",
			k:       2,
			wantLen: 2,
		},
		{
			name:    "k=3 (all nodes)",
			nodes:   nodes,
			key:     "test",
			k:       3,
			wantLen: 3,
		},
		{
			name:    "k > len(nodes)",
			nodes:   nodes,
			key:     "test",
			k:       10,
			wantLen: 3,
		},
		{
			name:    "single node, k=1",
			nodes:   []*Node{NewNode("only", 1)},
			key:     "test",
			k:       1,
			wantLen: 1,
		},
		{
			name:    "single node, k=2",
			nodes:   []*Node{NewNode("only", 1)},
			key:     "test",
			k:       2,
			wantLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := NewRendezvousRouter(tt.nodes, nil)
			result := router.GetNodes([]byte(tt.key), tt.k)

			if tt.wantNil {
				if result != nil {
					t.Errorf("expected nil, got %v", result)
				}
				return
			}

			if len(result) != tt.wantLen {
				t.Errorf("expected length %d, got %d", tt.wantLen, len(result))
			}
		})
	}
}

func TestRendezvousRouter_Determinism(t *testing.T) {
	nodes := []*Node{
		NewNode("n1", 8080),
		NewNode("n2", 8081),
		NewNode("n3", 8082),
		NewNode("n4", 8083),
		NewNode("n5", 8084),
	}

	router := NewRendezvousRouter(nodes, NewHashConfig([]byte("salt")))

	tests := []struct {
		key string
		k   int
	}{
		{key: "user:123", k: 1},
		{key: "user:123", k: 2},
		{key: "session:abc", k: 3},
		{key: "", k: 1},
		{key: "very-long-key-with-lots-of-characters", k: 2},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("key=%s,k=%d", tt.key, tt.k), func(t *testing.T) {
			// Call multiple times
			result1 := router.GetNodes([]byte(tt.key), tt.k)
			result2 := router.GetNodes([]byte(tt.key), tt.k)
			result3 := router.GetNodes([]byte(tt.key), tt.k)

			if len(result1) != len(result2) || len(result2) != len(result3) {
				t.Fatal("results have different lengths")
			}

			for i := range result1 {
				if result1[i] != result2[i] || result2[i] != result3[i] {
					t.Errorf("non-deterministic result at index %d", i)
				}
			}
		})
	}
}

func TestRendezvousRouter_ConsistentRegardlessOfNodeOrder(t *testing.T) {
	// Same nodes in different order should produce same routing
	nodes1 := []*Node{
		NewNode("a", 1),
		NewNode("b", 2),
		NewNode("c", 3),
	}
	nodes2 := []*Node{
		NewNode("c", 3),
		NewNode("a", 1),
		NewNode("b", 2),
	}
	nodes3 := []*Node{
		NewNode("b", 2),
		NewNode("c", 3),
		NewNode("a", 1),
	}

	salt := []byte("consistent-salt")
	router1 := NewRendezvousRouter(nodes1, NewHashConfig(salt))
	router2 := NewRendezvousRouter(nodes2, NewHashConfig(salt))
	router3 := NewRendezvousRouter(nodes3, NewHashConfig(salt))

	keys := []string{"key1", "key2", "key3", "user:100", "session:xyz"}

	for _, key := range keys {
		for k := 1; k <= 3; k++ {
			t.Run(fmt.Sprintf("key=%s,k=%d", key, k), func(t *testing.T) {
				result1 := router1.GetNodes([]byte(key), k)
				result2 := router2.GetNodes([]byte(key), k)
				result3 := router3.GetNodes([]byte(key), k)

				for i := range result1 {
					if result1[i].identityString != result2[i].identityString {
						t.Errorf("router1 vs router2 mismatch at %d: %s vs %s",
							i, result1[i].identityString, result2[i].identityString)
					}
					if result2[i].identityString != result3[i].identityString {
						t.Errorf("router2 vs router3 mismatch at %d: %s vs %s",
							i, result2[i].identityString, result3[i].identityString)
					}
				}
			})
		}
	}
}

func TestRendezvousRouter_SaltAffectsRouting(t *testing.T) {
	nodes := []*Node{
		NewNode("n1", 8080),
		NewNode("n2", 8081),
		NewNode("n3", 8082),
	}

	router1 := NewRendezvousRouter(nodes, NewHashConfig([]byte("salt-a")))
	router2 := NewRendezvousRouter(nodes, NewHashConfig([]byte("salt-b")))

	// With different salts, at least some keys should route differently
	differentCount := 0
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		result1 := router1.GetNodes([]byte(key), 1)
		result2 := router2.GetNodes([]byte(key), 1)

		if result1[0].identityString != result2[0].identityString {
			differentCount++
		}
	}

	// Expect at least some keys to route differently
	if differentCount == 0 {
		t.Error("different salts should produce different routing for at least some keys")
	}
}

func TestRendezvousRouter_Distribution(t *testing.T) {
	nodes := []*Node{
		NewNode("n1", 8080),
		NewNode("n2", 8081),
		NewNode("n3", 8082),
		NewNode("n4", 8083),
	}

	router := NewRendezvousRouter(nodes, NewHashConfig([]byte("dist-test")))

	// Count how many keys map to each node
	counts := make(map[string]int)
	numKeys := 10000

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		result := router.GetNodes([]byte(key), 1)
		counts[result[0].identityString]++
	}

	// Each node should get roughly 25% of keys (with some tolerance)
	expected := numKeys / len(nodes)
	tolerance := expected / 4 // 25% tolerance

	for nodeID, count := range counts {
		if count < expected-tolerance || count > expected+tolerance {
			t.Errorf("node %s has %d keys, expected ~%d (±%d)",
				nodeID, count, expected, tolerance)
		}
	}
}

func TestRendezvousRouter_ResultsAreOrdered(t *testing.T) {
	nodes := []*Node{
		NewNode("n1", 8080),
		NewNode("n2", 8081),
		NewNode("n3", 8082),
		NewNode("n4", 8083),
		NewNode("n5", 8084),
	}

	router := NewRendezvousRouter(nodes, NewHashConfig([]byte("order-test")))

	// For k=1, k=2, and k=3, verify that results are subsets
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)

		result1 := router.GetNodes([]byte(key), 1)
		result2 := router.GetNodes([]byte(key), 2)
		result3 := router.GetNodes([]byte(key), 3)

		// result1[0] should equal result2[0] and result3[0]
		if result1[0] != result2[0] {
			t.Errorf("key %s: k=1 result differs from k=2 first element", key)
		}
		if result1[0] != result3[0] {
			t.Errorf("key %s: k=1 result differs from k=3 first element", key)
		}

		// result2 should be prefix of result3
		if result2[0] != result3[0] || result2[1] != result3[1] {
			t.Errorf("key %s: k=2 results not prefix of k=3 results", key)
		}
	}
}

func TestRendezvousRouter_NoDuplicatesInResults(t *testing.T) {
	nodes := []*Node{
		NewNode("n1", 8080),
		NewNode("n2", 8081),
		NewNode("n3", 8082),
	}

	router := NewRendezvousRouter(nodes, nil)

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		result := router.GetNodes([]byte(key), 3)

		seen := make(map[*Node]bool)
		for _, node := range result {
			if seen[node] {
				t.Errorf("key %s: duplicate node in result", key)
			}
			seen[node] = true
		}
	}
}

func TestRendezvousRouter_ConcurrentAccess(t *testing.T) {
	nodes := []*Node{
		NewNode("n1", 8080),
		NewNode("n2", 8081),
		NewNode("n3", 8082),
	}

	router := NewRendezvousRouter(nodes, nil)

	var wg sync.WaitGroup
	numGoroutines := 100
	numOperations := 1000

	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				result := router.GetNodes([]byte(key), 2)
				if len(result) != 2 {
					t.Errorf("expected 2 nodes, got %d", len(result))
				}
			}
		}(i)
	}

	// Concurrent writes
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				newNodes := []*Node{
					NewNode(fmt.Sprintf("n%d", id), 8080+id),
					NewNode(fmt.Sprintf("m%d", id), 9080+id),
				}
				router.SetNodes(newNodes)
			}
		}(i)
	}

	wg.Wait()
}

func TestRouterInterface(t *testing.T) {
	// Verify RendezvousRouter implements Router interface
	var _ Router = (*RendezvousRouter)(nil)
	var _ Router = NewRendezvousRouter(nil, nil)
}
