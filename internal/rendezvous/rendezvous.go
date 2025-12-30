package rendezvous

import (
	"encoding/binary"
	"fmt"
	"sort"
	"sync/atomic"
)

// Represents a single node in the cluster.
type Node struct {
	id   string // canonical identity
	port int

	identityString string // pre-computed, immutable string of node identity
	identityHash   uint64 // pre-computed, immutable hash of node identity
}

func NewNode(id string, port int) *Node {
	n := &Node{id: id, port: port}
	n.identityString = n.computeString()
	n.identityHash = DefaultUnsaltedHash64.Hash64([]byte(n.identityString))
	return n
}

func (n *Node) computeString() string {
	return fmt.Sprintf("%s:%d", n.id, n.port)
}

// A router tells the client where a key is or should be stored.
type Router interface {
	// Update the nodes in the router.
	SetNodes(nodes []*Node)
	// Get up to k nodes for a key, ordered by preference (descending score).
	GetNodes(key []byte, k int) []*Node
}

// RendezvousRouter is safe for concurrent use.
type RendezvousRouter struct {
	nodes  atomic.Value // stores []*Node
	hasher Hash64
}

func NewRendezvousRouter(nodes []*Node, hashConfig *HashConfig) *RendezvousRouter {
	r := &RendezvousRouter{}
	r.hasher = NewXXH3Hash64(hashConfig)
	r.nodes.Store(([]*Node)(nil)) // initialize with typed nil
	r.SetNodes(nodes)
	return r
}

func (r *RendezvousRouter) SetNodes(nodes []*Node) {
	copied := make([]*Node, len(nodes))
	copy(copied, nodes)
	r.nodes.Store(copied)
}

type nodeScore struct {
	node  *Node
	score uint64
}

// scoreBetter returns true if a is better than b (higher score, or same score with lower identity).
func scoreBetter(a, b nodeScore) bool {
	if a.score != b.score {
		return a.score > b.score
	}
	return a.node.identityString < b.node.identityString
}

func (r *RendezvousRouter) GetNodes(key []byte, k int) []*Node {
	nodes := r.nodes.Load().([]*Node)

	if len(nodes) == 0 || k <= 0 {
		return nil
	}

	// Allocate combined key buffer once
	combinedKey := make([]byte, len(key)+8)
	copy(combinedKey, key)

	computeScore := func(node *Node) nodeScore {
		binary.LittleEndian.PutUint64(combinedKey[len(key):], node.identityHash)
		return nodeScore{node: node, score: r.hasher.Hash64(combinedKey)}
	}

	// Fast path for k=1: single pass to find max
	if k == 1 {
		best := computeScore(nodes[0])
		for _, node := range nodes[1:] {
			if s := computeScore(node); scoreBetter(s, best) {
				best = s
			}
		}
		return []*Node{best.node}
	}

	// Fast path for k=2: single pass to find top 2
	if k == 2 {
		first := computeScore(nodes[0])
		second := nodeScore{} // zero value, will be replaced

		for _, node := range nodes[1:] {
			s := computeScore(node)
			if scoreBetter(s, first) {
				second = first
				first = s
			} else if second.node == nil || scoreBetter(s, second) {
				second = s
			}
		}

		if second.node == nil {
			return []*Node{first.node}
		}
		return []*Node{first.node, second.node}
	}

	// General case: compute all scores and sort
	scores := make([]nodeScore, len(nodes))
	for i, node := range nodes {
		scores[i] = computeScore(node)
	}

	sort.Slice(scores, func(i, j int) bool {
		return scoreBetter(scores[i], scores[j])
	})

	if k > len(scores) {
		k = len(scores)
	}

	result := make([]*Node, k)
	for i := 0; i < k; i++ {
		result[i] = scores[i].node
	}

	return result
}
