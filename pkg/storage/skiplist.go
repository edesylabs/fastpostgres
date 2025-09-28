// Package storage provides SkipList implementation for LSM-Tree MemTable.
// SkipList provides O(log n) insertion, deletion, and search operations.
package storage

import (
	"bytes"
	"math/rand"
	"sync"
	"time"
)

const (
	maxLevel    = 32    // Maximum number of levels
	probability = 0.25  // Probability for level promotion
)

// SkipList implements a probabilistic skip list for fast key-value operations
type SkipList struct {
	header *skipNode
	level  int
	size   int64
	mu     sync.RWMutex
	rng    *rand.Rand
}

// skipNode represents a node in the skip list
type skipNode struct {
	kv      *KeyValue
	forward []*skipNode
}

// NewSkipList creates a new skip list
func NewSkipList() *SkipList {
	header := &skipNode{
		forward: make([]*skipNode, maxLevel),
	}

	return &SkipList{
		header: header,
		level:  1,
		size:   0,
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Put inserts or updates a key-value pair
func (sl *SkipList) Put(kv *KeyValue) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	update := make([]*skipNode, maxLevel)
	current := sl.header

	// Find the position to insert
	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].kv.Key, kv.Key) < 0 {
			current = current.forward[i]
		}
		update[i] = current
	}

	current = current.forward[0]

	// If key already exists, update the value
	if current != nil && bytes.Equal(current.kv.Key, kv.Key) {
		current.kv = kv
		return
	}

	// Generate random level for new node
	newLevel := sl.randomLevel()
	if newLevel > sl.level {
		for i := sl.level; i < newLevel; i++ {
			update[i] = sl.header
		}
		sl.level = newLevel
	}

	// Create new node
	newNode := &skipNode{
		kv:      kv,
		forward: make([]*skipNode, newLevel),
	}

	// Insert the new node
	for i := 0; i < newLevel; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	sl.size++
}

// Get retrieves a value by key
func (sl *SkipList) Get(key []byte) (*KeyValue, bool) {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	current := sl.header

	// Search from top level down
	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].kv.Key, key) < 0 {
			current = current.forward[i]
		}
	}

	current = current.forward[0]

	if current != nil && bytes.Equal(current.kv.Key, key) {
		return current.kv, true
	}

	return nil, false
}

// Delete removes a key-value pair
func (sl *SkipList) Delete(key []byte) bool {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	update := make([]*skipNode, maxLevel)
	current := sl.header

	// Find the node to delete
	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].kv.Key, key) < 0 {
			current = current.forward[i]
		}
		update[i] = current
	}

	current = current.forward[0]

	if current == nil || !bytes.Equal(current.kv.Key, key) {
		return false
	}

	// Remove the node from all levels
	for i := 0; i < sl.level; i++ {
		if update[i].forward[i] != current {
			break
		}
		update[i].forward[i] = current.forward[i]
	}

	// Update level if necessary
	for sl.level > 1 && sl.header.forward[sl.level-1] == nil {
		sl.level--
	}

	sl.size--
	return true
}

// Size returns the number of elements in the skip list
func (sl *SkipList) Size() int64 {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return sl.size
}

// NewIterator creates an iterator for range scanning
func (sl *SkipList) NewIterator(startKey, endKey []byte) Iterator {
	return &skipListIterator{
		skipList: sl,
		startKey: startKey,
		endKey:   endKey,
		current:  nil,
		started:  false,
	}
}

// randomLevel generates a random level for a new node
func (sl *SkipList) randomLevel() int {
	level := 1
	for level < maxLevel && sl.rng.Float32() < probability {
		level++
	}
	return level
}

// skipListIterator implements the Iterator interface for SkipList
type skipListIterator struct {
	skipList *SkipList
	startKey []byte
	endKey   []byte
	current  *skipNode
	started  bool
}

func (iter *skipListIterator) HasNext() bool {
	iter.skipList.mu.RLock()
	defer iter.skipList.mu.RUnlock()

	if !iter.started {
		iter.started = true
		iter.current = iter.findStart()
	}

	if iter.current == nil {
		return false
	}

	// Check if we've reached the end
	if iter.endKey != nil && bytes.Compare(iter.current.kv.Key, iter.endKey) >= 0 {
		return false
	}

	return true
}

func (iter *skipListIterator) Next() *KeyValue {
	if !iter.HasNext() {
		return nil
	}

	kv := iter.current.kv
	iter.current = iter.current.forward[0]
	return kv
}

func (iter *skipListIterator) Close() error {
	iter.current = nil
	return nil
}

// findStart finds the starting node for iteration
func (iter *skipListIterator) findStart() *skipNode {
	if iter.startKey == nil {
		return iter.skipList.header.forward[0]
	}

	current := iter.skipList.header

	// Find the first node >= startKey
	for i := iter.skipList.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].kv.Key, iter.startKey) < 0 {
			current = current.forward[i]
		}
	}

	return current.forward[0]
}

// MemTable implementation using SkipList

// NewMemTable creates a new MemTable
func NewMemTable() *MemTable {
	return &MemTable{
		data:      NewSkipList(),
		size:      0,
		createdAt: time.Now(),
		readOnly:  false,
	}
}

// Put inserts a key-value pair into the MemTable
func (mt *MemTable) Put(kv *KeyValue) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	if mt.readOnly {
		return // Cannot write to read-only memtable
	}

	// Estimate size increase
	keySize := int64(len(kv.Key))
	valueSize := int64(len(kv.Value))
	overhead := int64(64) // Estimate for node overhead

	mt.data.Put(kv)
	mt.size += keySize + valueSize + overhead
}

// Get retrieves a value by key from the MemTable
func (mt *MemTable) Get(key []byte) (*KeyValue, bool) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	return mt.data.Get(key)
}

// Size returns the estimated size of the MemTable in bytes
func (mt *MemTable) Size() int64 {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.size
}

// SetReadOnly marks the MemTable as read-only
func (mt *MemTable) SetReadOnly() {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.readOnly = true
}

// NewIterator creates an iterator for the MemTable
func (mt *MemTable) NewIterator(startKey, endKey []byte) Iterator {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.data.NewIterator(startKey, endKey)
}

// Count returns the number of entries in the MemTable
func (mt *MemTable) Count() int64 {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.data.Size()
}