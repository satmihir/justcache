package storage

import (
	"testing"
	"time"
)

// Helper to create a CachedObject for testing
func newTestObject(key string, value string) *CachedObject {
	return &CachedObject{
		Key:            key,
		Value:          []byte(value),
		ExpirationTime: time.Now().Add(time.Hour),
	}
}

// Helper to collect list keys in order (head to tail)
func listKeys(l *lruList) []string {
	var keys []string
	for node := l.front(); node != nil; node = node.next {
		keys = append(keys, node.Key)
	}
	return keys
}

// Helper to collect list keys in reverse order (tail to head)
func listKeysReverse(l *lruList) []string {
	var keys []string
	for node := l.tail; node != nil; node = node.prev {
		keys = append(keys, node.Key)
	}
	return keys
}

// ============================================================================
// CachedObject.GetBytesUsed Tests
// ============================================================================

func TestGetBytesUsed_Empty(t *testing.T) {
	obj := &CachedObject{Key: "", Value: []byte{}}
	if got := obj.GetBytesUsed(); got != 0 {
		t.Errorf("GetBytesUsed() = %d, want 0", got)
	}
}

func TestGetBytesUsed_KeyOnly(t *testing.T) {
	obj := &CachedObject{Key: "hello", Value: []byte{}}
	if got := obj.GetBytesUsed(); got != 5 {
		t.Errorf("GetBytesUsed() = %d, want 5", got)
	}
}

func TestGetBytesUsed_ValueOnly(t *testing.T) {
	obj := &CachedObject{Key: "", Value: []byte("world")}
	if got := obj.GetBytesUsed(); got != 5 {
		t.Errorf("GetBytesUsed() = %d, want 5", got)
	}
}

func TestGetBytesUsed_KeyAndValue(t *testing.T) {
	obj := &CachedObject{Key: "hello", Value: []byte("world")}
	if got := obj.GetBytesUsed(); got != 10 {
		t.Errorf("GetBytesUsed() = %d, want 10", got)
	}
}

func TestGetBytesUsed_UTF8Key(t *testing.T) {
	// UTF-8: "你好" is 6 bytes, not 2 characters
	obj := &CachedObject{Key: "你好", Value: []byte("hi")}
	if got := obj.GetBytesUsed(); got != 8 { // 6 + 2
		t.Errorf("GetBytesUsed() = %d, want 8", got)
	}
}

// ============================================================================
// lruList.append Tests
// ============================================================================

func TestAppend_EmptyList(t *testing.T) {
	l := &lruList{}
	a := newTestObject("a", "val")

	l.append(a)

	if l.head != a {
		t.Error("head should be a")
	}
	if l.tail != a {
		t.Error("tail should be a")
	}
	if a.prev != nil || a.next != nil {
		t.Error("single element should have nil prev and next")
	}
}

func TestAppend_TwoElements(t *testing.T) {
	l := &lruList{}
	a := newTestObject("a", "val")
	b := newTestObject("b", "val")

	l.append(a)
	l.append(b)

	if l.head != a {
		t.Error("head should be a")
	}
	if l.tail != b {
		t.Error("tail should be b")
	}
	if a.next != b {
		t.Error("a.next should be b")
	}
	if b.prev != a {
		t.Error("b.prev should be a")
	}
	if a.prev != nil {
		t.Error("a.prev should be nil")
	}
	if b.next != nil {
		t.Error("b.next should be nil")
	}
}

func TestAppend_ThreeElements(t *testing.T) {
	l := &lruList{}
	a := newTestObject("a", "val")
	b := newTestObject("b", "val")
	c := newTestObject("c", "val")

	l.append(a)
	l.append(b)
	l.append(c)

	keys := listKeys(l)
	if len(keys) != 3 || keys[0] != "a" || keys[1] != "b" || keys[2] != "c" {
		t.Errorf("list order = %v, want [a, b, c]", keys)
	}

	// Verify reverse order
	reverseKeys := listKeysReverse(l)
	if len(reverseKeys) != 3 || reverseKeys[0] != "c" || reverseKeys[1] != "b" || reverseKeys[2] != "a" {
		t.Errorf("reverse order = %v, want [c, b, a]", reverseKeys)
	}
}

// ============================================================================
// lruList.remove Tests
// ============================================================================

func TestRemove_SingleElement(t *testing.T) {
	l := &lruList{}
	a := newTestObject("a", "val")
	l.append(a)

	l.remove(a)

	if l.head != nil {
		t.Error("head should be nil")
	}
	if l.tail != nil {
		t.Error("tail should be nil")
	}
	if a.prev != nil || a.next != nil {
		t.Error("removed node should have nil pointers")
	}
}

func TestRemove_HeadOfTwo(t *testing.T) {
	l := &lruList{}
	a := newTestObject("a", "val")
	b := newTestObject("b", "val")
	l.append(a)
	l.append(b)

	l.remove(a)

	if l.head != b {
		t.Error("head should be b")
	}
	if l.tail != b {
		t.Error("tail should be b")
	}
	if b.prev != nil || b.next != nil {
		t.Error("b should have nil prev and next")
	}
	if a.prev != nil || a.next != nil {
		t.Error("removed node should have nil pointers")
	}
}

func TestRemove_TailOfTwo(t *testing.T) {
	l := &lruList{}
	a := newTestObject("a", "val")
	b := newTestObject("b", "val")
	l.append(a)
	l.append(b)

	l.remove(b)

	if l.head != a {
		t.Error("head should be a")
	}
	if l.tail != a {
		t.Error("tail should be a")
	}
	if a.prev != nil || a.next != nil {
		t.Error("a should have nil prev and next")
	}
	if b.prev != nil || b.next != nil {
		t.Error("removed node should have nil pointers")
	}
}

func TestRemove_HeadOfThree(t *testing.T) {
	l := &lruList{}
	a := newTestObject("a", "val")
	b := newTestObject("b", "val")
	c := newTestObject("c", "val")
	l.append(a)
	l.append(b)
	l.append(c)

	l.remove(a)

	keys := listKeys(l)
	if len(keys) != 2 || keys[0] != "b" || keys[1] != "c" {
		t.Errorf("list = %v, want [b, c]", keys)
	}
	if l.head != b {
		t.Error("head should be b")
	}
	if b.prev != nil {
		t.Error("new head should have nil prev")
	}
}

func TestRemove_TailOfThree(t *testing.T) {
	l := &lruList{}
	a := newTestObject("a", "val")
	b := newTestObject("b", "val")
	c := newTestObject("c", "val")
	l.append(a)
	l.append(b)
	l.append(c)

	l.remove(c)

	keys := listKeys(l)
	if len(keys) != 2 || keys[0] != "a" || keys[1] != "b" {
		t.Errorf("list = %v, want [a, b]", keys)
	}
	if l.tail != b {
		t.Error("tail should be b")
	}
	if b.next != nil {
		t.Error("new tail should have nil next")
	}
}

func TestRemove_MiddleOfThree(t *testing.T) {
	l := &lruList{}
	a := newTestObject("a", "val")
	b := newTestObject("b", "val")
	c := newTestObject("c", "val")
	l.append(a)
	l.append(b)
	l.append(c)

	l.remove(b)

	keys := listKeys(l)
	if len(keys) != 2 || keys[0] != "a" || keys[1] != "c" {
		t.Errorf("list = %v, want [a, c]", keys)
	}
	if a.next != c {
		t.Error("a.next should be c")
	}
	if c.prev != a {
		t.Error("c.prev should be a")
	}
	if b.prev != nil || b.next != nil {
		t.Error("removed node should have nil pointers")
	}
}

func TestRemove_MiddleOfFive(t *testing.T) {
	l := &lruList{}
	nodes := make([]*CachedObject, 5)
	for i := 0; i < 5; i++ {
		nodes[i] = newTestObject(string(rune('a'+i)), "val")
		l.append(nodes[i])
	}

	// Remove middle element (c)
	l.remove(nodes[2])

	keys := listKeys(l)
	expected := []string{"a", "b", "d", "e"}
	if len(keys) != len(expected) {
		t.Errorf("list length = %d, want %d", len(keys), len(expected))
	}
	for i, k := range expected {
		if keys[i] != k {
			t.Errorf("keys[%d] = %s, want %s", i, keys[i], k)
		}
	}
}

// ============================================================================
// lruList.moveToTail Tests
// ============================================================================

func TestMoveToTail_AlreadyAtTail(t *testing.T) {
	l := &lruList{}
	a := newTestObject("a", "val")
	b := newTestObject("b", "val")
	l.append(a)
	l.append(b)

	l.moveToTail(b)

	keys := listKeys(l)
	if len(keys) != 2 || keys[0] != "a" || keys[1] != "b" {
		t.Errorf("list = %v, want [a, b]", keys)
	}
}

func TestMoveToTail_SingleElement(t *testing.T) {
	l := &lruList{}
	a := newTestObject("a", "val")
	l.append(a)

	l.moveToTail(a)

	if l.head != a || l.tail != a {
		t.Error("single element should remain unchanged")
	}
}

func TestMoveToTail_HeadToTail(t *testing.T) {
	l := &lruList{}
	a := newTestObject("a", "val")
	b := newTestObject("b", "val")
	l.append(a)
	l.append(b)

	l.moveToTail(a)

	keys := listKeys(l)
	if len(keys) != 2 || keys[0] != "b" || keys[1] != "a" {
		t.Errorf("list = %v, want [b, a]", keys)
	}
	if l.head != b {
		t.Error("head should be b")
	}
	if l.tail != a {
		t.Error("tail should be a")
	}
}

func TestMoveToTail_HeadOfThreeToTail(t *testing.T) {
	l := &lruList{}
	a := newTestObject("a", "val")
	b := newTestObject("b", "val")
	c := newTestObject("c", "val")
	l.append(a)
	l.append(b)
	l.append(c)

	l.moveToTail(a)

	keys := listKeys(l)
	if len(keys) != 3 || keys[0] != "b" || keys[1] != "c" || keys[2] != "a" {
		t.Errorf("list = %v, want [b, c, a]", keys)
	}
}

func TestMoveToTail_MiddleToTail(t *testing.T) {
	l := &lruList{}
	a := newTestObject("a", "val")
	b := newTestObject("b", "val")
	c := newTestObject("c", "val")
	l.append(a)
	l.append(b)
	l.append(c)

	l.moveToTail(b)

	keys := listKeys(l)
	if len(keys) != 3 || keys[0] != "a" || keys[1] != "c" || keys[2] != "b" {
		t.Errorf("list = %v, want [a, c, b]", keys)
	}

	// Verify bidirectional links
	reverseKeys := listKeysReverse(l)
	if len(reverseKeys) != 3 || reverseKeys[0] != "b" || reverseKeys[1] != "c" || reverseKeys[2] != "a" {
		t.Errorf("reverse = %v, want [b, c, a]", reverseKeys)
	}
}

// ============================================================================
// lruList.front Tests
// ============================================================================

func TestFront_EmptyList(t *testing.T) {
	l := &lruList{}
	if l.front() != nil {
		t.Error("front of empty list should be nil")
	}
}

func TestFront_NonEmpty(t *testing.T) {
	l := &lruList{}
	a := newTestObject("a", "val")
	b := newTestObject("b", "val")
	l.append(a)
	l.append(b)

	if l.front() != a {
		t.Error("front should be a")
	}
}

// ============================================================================
// Complex Sequence Tests
// ============================================================================

func TestSequence_AppendRemoveAppend(t *testing.T) {
	l := &lruList{}
	a := newTestObject("a", "val")
	b := newTestObject("b", "val")

	l.append(a)
	l.append(b)
	l.remove(a)
	l.remove(b)

	if l.head != nil || l.tail != nil {
		t.Error("list should be empty")
	}

	// Re-append
	l.append(a)
	l.append(b)

	keys := listKeys(l)
	if len(keys) != 2 || keys[0] != "a" || keys[1] != "b" {
		t.Errorf("list = %v, want [a, b]", keys)
	}
}

func TestSequence_LRUSimulation(t *testing.T) {
	l := &lruList{}
	a := newTestObject("a", "val")
	b := newTestObject("b", "val")
	c := newTestObject("c", "val")

	// Simulate: put a, put b, put c
	l.append(a)
	l.append(b)
	l.append(c)
	// Order: a -> b -> c (a is LRU)

	// Simulate: get a (move to tail)
	l.moveToTail(a)
	// Order: b -> c -> a

	keys := listKeys(l)
	if keys[0] != "b" || keys[1] != "c" || keys[2] != "a" {
		t.Errorf("after get(a): list = %v, want [b, c, a]", keys)
	}

	// Simulate: evict LRU (should be b)
	lru := l.front()
	if lru.Key != "b" {
		t.Errorf("LRU = %s, want b", lru.Key)
	}
	l.remove(lru)
	// Order: c -> a

	keys = listKeys(l)
	if len(keys) != 2 || keys[0] != "c" || keys[1] != "a" {
		t.Errorf("after evict: list = %v, want [c, a]", keys)
	}

	// Simulate: get c (move to tail)
	l.moveToTail(c)
	// Order: a -> c

	keys = listKeys(l)
	if keys[0] != "a" || keys[1] != "c" {
		t.Errorf("after get(c): list = %v, want [a, c]", keys)
	}
}

func TestSequence_RemoveAllThenAdd(t *testing.T) {
	l := &lruList{}
	nodes := make([]*CachedObject, 5)
	for i := 0; i < 5; i++ {
		nodes[i] = newTestObject(string(rune('a'+i)), "val")
		l.append(nodes[i])
	}

	// Remove all from head
	for i := 0; i < 5; i++ {
		l.remove(nodes[i])
	}

	if l.head != nil || l.tail != nil {
		t.Error("list should be empty")
	}

	// Add them back
	for i := 0; i < 5; i++ {
		l.append(nodes[i])
	}

	keys := listKeys(l)
	expected := []string{"a", "b", "c", "d", "e"}
	for i, k := range expected {
		if keys[i] != k {
			t.Errorf("keys[%d] = %s, want %s", i, keys[i], k)
		}
	}
}

