package storage

import "time"

// CachedObject represents a cached key-value pair with expiration.
type CachedObject struct {
	Key            string
	Value          []byte
	ExpirationTime time.Time

	// Linked list pointers for LRU tracking
	prev *CachedObject
	next *CachedObject
}

// GetBytesUsed returns the total bytes used by the key and value.
func (c *CachedObject) GetBytesUsed() uint64 {
	return uint64(len(c.Key) + len(c.Value))
}

// lruList is a doubly-linked list for LRU tracking.
// All pointer manipulation is centralized here for correctness and readability.
type lruList struct {
	head *CachedObject
	tail *CachedObject
}

// append adds a node to the tail of the list (most recently used position).
func (l *lruList) append(node *CachedObject) {
	node.prev = l.tail
	node.next = nil
	if l.tail != nil {
		l.tail.next = node
	} else {
		l.head = node
	}
	l.tail = node
}

// remove removes a node from anywhere in the list.
func (l *lruList) remove(node *CachedObject) {
	if node == l.head && node == l.tail {
		// Single element: clear both
		l.head = nil
		l.tail = nil
	} else if node == l.head {
		l.head = node.next
		if l.head != nil {
			l.head.prev = nil
		}
	} else if node == l.tail {
		l.tail = node.prev
		if l.tail != nil {
			l.tail.next = nil
		}
	} else {
		// Middle node: bridge neighbors
		node.prev.next = node.next
		node.next.prev = node.prev
	}
	// Clear the node's pointers
	node.prev = nil
	node.next = nil
}

// moveToTail moves an existing node to the tail (most recently used).
func (l *lruList) moveToTail(node *CachedObject) {
	if node == l.tail {
		return // Already at tail
	}
	l.remove(node)
	l.append(node)
}

// front returns the head of the list (least recently used).
func (l *lruList) front() *CachedObject {
	return l.head
}

