// Package storage provides LSM-Tree indexing for write-heavy workloads.
// LSM-Trees optimize for fast writes while maintaining good read performance.
package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// LSMIndex implements a Log-Structured Merge Tree for fast writes and reads
type LSMIndex struct {
	name         string
	directory    string
	memTable     *MemTable
	immutables   []*MemTable       // Immutable memtables being flushed
	sstables     []*SSTable        // Sorted String Tables on disk
	bloomFilters map[string]*BloomFilter

	// Configuration
	memTableSize    int64  // Max size before flush (default: 64MB)
	sstableSize     int64  // Target SSTable size (default: 256MB)
	compactionRatio float64 // Trigger compaction when ratio exceeded (default: 4.0)

	// Concurrency control
	mu           sync.RWMutex
	flushChan    chan *MemTable
	compactChan  chan struct{}
	stopChan     chan struct{}
	wg           sync.WaitGroup
}

// MemTable represents an in-memory sorted table using SkipList
type MemTable struct {
	data      *SkipList
	size      int64
	createdAt time.Time
	readOnly  bool
	mu        sync.RWMutex
}

// SSTable represents a Sorted String Table on disk
type SSTable struct {
	id          string
	path        string
	size        int64
	minKey      []byte
	maxKey      []byte
	bloomFilter *BloomFilter
	index       *SSTableIndex  // Sparse index for faster seeks
	createdAt   time.Time
}

// SSTableIndex provides sparse indexing for fast key lookups
type SSTableIndex struct {
	entries []IndexEntry
}

type IndexEntry struct {
	Key    []byte
	Offset int64
}

// BloomFilter provides probabilistic membership testing
type BloomFilter struct {
	bits     []uint64
	size     uint64
	hashes   int
	elements uint64
}

// KeyValue represents a key-value pair in the LSM tree
type KeyValue struct {
	Key       []byte
	Value     []byte
	Timestamp int64
	Deleted   bool
}

// NewLSMIndex creates a new LSM-Tree index
func NewLSMIndex(name, directory string) (*LSMIndex, error) {
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	lsm := &LSMIndex{
		name:            name,
		directory:       directory,
		memTable:        NewMemTable(),
		immutables:      make([]*MemTable, 0),
		sstables:        make([]*SSTable, 0),
		bloomFilters:    make(map[string]*BloomFilter),
		memTableSize:    64 * 1024 * 1024, // 64MB
		sstableSize:     256 * 1024 * 1024, // 256MB
		compactionRatio: 4.0,
		flushChan:       make(chan *MemTable, 10),
		compactChan:     make(chan struct{}, 1),
		stopChan:        make(chan struct{}),
	}

	// Load existing SSTables
	if err := lsm.loadExistingSSTables(); err != nil {
		return nil, fmt.Errorf("failed to load existing SSTables: %w", err)
	}

	// Start background workers
	lsm.wg.Add(2)
	go lsm.flushWorker()
	go lsm.compactionWorker()

	return lsm, nil
}

// Put inserts or updates a key-value pair
func (lsm *LSMIndex) Put(key, value []byte) error {
	lsm.mu.Lock()
	defer lsm.mu.Unlock()

	kv := &KeyValue{
		Key:       append([]byte(nil), key...),
		Value:     append([]byte(nil), value...),
		Timestamp: time.Now().UnixNano(),
		Deleted:   false,
	}

	// Insert into current memtable
	lsm.memTable.Put(kv)

	// Check if memtable needs to be flushed
	if lsm.memTable.Size() >= lsm.memTableSize {
		lsm.rotateMemTable()
	}

	return nil
}

// Get retrieves a value by key
func (lsm *LSMIndex) Get(key []byte) ([]byte, bool, error) {
	lsm.mu.RLock()
	defer lsm.mu.RUnlock()

	// Search in current memtable
	if kv, found := lsm.memTable.Get(key); found {
		if kv.Deleted {
			return nil, false, nil
		}
		return kv.Value, true, nil
	}

	// Search in immutable memtables
	for i := len(lsm.immutables) - 1; i >= 0; i-- {
		if kv, found := lsm.immutables[i].Get(key); found {
			if kv.Deleted {
				return nil, false, nil
			}
			return kv.Value, true, nil
		}
	}

	// Search in SSTables (newest first)
	for i := len(lsm.sstables) - 1; i >= 0; i-- {
		sstable := lsm.sstables[i]

		// Check bloom filter first
		if !sstable.bloomFilter.MayContain(key) {
			continue
		}

		// Search in SSTable
		kv, found, err := lsm.searchSSTable(sstable, key)
		if err != nil {
			return nil, false, err
		}
		if found {
			if kv.Deleted {
				return nil, false, nil
			}
			return kv.Value, true, nil
		}
	}

	return nil, false, nil
}

// Delete marks a key as deleted
func (lsm *LSMIndex) Delete(key []byte) error {
	lsm.mu.Lock()
	defer lsm.mu.Unlock()

	kv := &KeyValue{
		Key:       append([]byte(nil), key...),
		Value:     nil,
		Timestamp: time.Now().UnixNano(),
		Deleted:   true,
	}

	lsm.memTable.Put(kv)

	if lsm.memTable.Size() >= lsm.memTableSize {
		lsm.rotateMemTable()
	}

	return nil
}

// RangeScan performs a range scan from startKey to endKey
func (lsm *LSMIndex) RangeScan(startKey, endKey []byte, limit int) ([]*KeyValue, error) {
	lsm.mu.RLock()
	defer lsm.mu.RUnlock()

	results := make([]*KeyValue, 0, limit)
	seen := make(map[string]bool)

	// Merge results from all sources
	iterators := make([]Iterator, 0)

	// Add memtable iterator
	iterators = append(iterators, lsm.memTable.NewIterator(startKey, endKey))

	// Add immutable memtable iterators
	for _, immutable := range lsm.immutables {
		iterators = append(iterators, immutable.NewIterator(startKey, endKey))
	}

	// Add SSTable iterators
	for _, sstable := range lsm.sstables {
		iter, err := lsm.newSSTableIterator(sstable, startKey, endKey)
		if err != nil {
			continue // Skip problematic SSTables
		}
		iterators = append(iterators, iter)
	}

	// Merge-sort all iterators
	merged := NewMergeIterator(iterators)
	defer merged.Close()

	for merged.HasNext() && len(results) < limit {
		kv := merged.Next()
		keyStr := string(kv.Key)

		// Skip if we've already seen this key (newer version wins)
		if seen[keyStr] {
			continue
		}
		seen[keyStr] = true

		// Skip deleted keys
		if !kv.Deleted {
			results = append(results, kv)
		}
	}

	return results, nil
}

// rotateMemTable creates a new memtable and marks current as immutable
func (lsm *LSMIndex) rotateMemTable() {
	// Mark current memtable as read-only
	lsm.memTable.SetReadOnly()

	// Add to immutables list
	lsm.immutables = append(lsm.immutables, lsm.memTable)

	// Create new memtable
	lsm.memTable = NewMemTable()

	// Trigger flush
	select {
	case lsm.flushChan <- lsm.immutables[len(lsm.immutables)-1]:
	default:
		// Channel full, flush will happen eventually
	}
}

// flushWorker handles flushing memtables to disk
func (lsm *LSMIndex) flushWorker() {
	defer lsm.wg.Done()

	for {
		select {
		case memtable := <-lsm.flushChan:
			if err := lsm.flushMemTable(memtable); err != nil {
				// Log error but continue
				fmt.Printf("Failed to flush memtable: %v\n", err)
			}
		case <-lsm.stopChan:
			return
		}
	}
}

// flushMemTable writes a memtable to an SSTable on disk
func (lsm *LSMIndex) flushMemTable(memtable *MemTable) error {
	// Create SSTable file
	sstableID := fmt.Sprintf("sstable_%d.sst", time.Now().UnixNano())
	sstablePath := filepath.Join(lsm.directory, sstableID)

	sstable, err := lsm.writeSSTable(sstablePath, memtable.NewIterator(nil, nil))
	if err != nil {
		return err
	}

	// Update LSM structure
	lsm.mu.Lock()
	lsm.sstables = append(lsm.sstables, sstable)

	// Remove from immutables
	for i, immutable := range lsm.immutables {
		if immutable == memtable {
			lsm.immutables = append(lsm.immutables[:i], lsm.immutables[i+1:]...)
			break
		}
	}
	lsm.mu.Unlock()

	// Trigger compaction if needed
	if lsm.shouldCompact() {
		select {
		case lsm.compactChan <- struct{}{}:
		default:
		}
	}

	return nil
}

// compactionWorker handles background compaction
func (lsm *LSMIndex) compactionWorker() {
	defer lsm.wg.Done()

	ticker := time.NewTicker(5 * time.Minute) // Check every 5 minutes
	defer ticker.Stop()

	for {
		select {
		case <-lsm.compactChan:
			if err := lsm.performCompaction(); err != nil {
				fmt.Printf("Compaction failed: %v\n", err)
			}
		case <-ticker.C:
			if lsm.shouldCompact() {
				if err := lsm.performCompaction(); err != nil {
					fmt.Printf("Scheduled compaction failed: %v\n", err)
				}
			}
		case <-lsm.stopChan:
			return
		}
	}
}

// shouldCompact determines if compaction is needed
func (lsm *LSMIndex) shouldCompact() bool {
	if len(lsm.sstables) < 2 {
		return false
	}

	// Simple compaction strategy: compact when we have too many SSTables
	return len(lsm.sstables) > 10
}

// performCompaction merges multiple SSTables into fewer, larger ones
func (lsm *LSMIndex) performCompaction() error {
	lsm.mu.Lock()
	defer lsm.mu.Unlock()

	if len(lsm.sstables) < 2 {
		return nil
	}

	// Simple strategy: merge oldest SSTables
	toCompact := lsm.sstables[:min(4, len(lsm.sstables))] // Compact up to 4 SSTables

	// Create iterators for all SSTables to compact
	iterators := make([]Iterator, 0, len(toCompact))
	for _, sstable := range toCompact {
		iter, err := lsm.newSSTableIterator(sstable, nil, nil)
		if err != nil {
			continue
		}
		iterators = append(iterators, iter)
	}

	// Merge them into a new SSTable
	merged := NewMergeIterator(iterators)
	defer merged.Close()

	newSSTablePath := filepath.Join(lsm.directory, fmt.Sprintf("compacted_%d.sst", time.Now().UnixNano()))
	newSSTable, err := lsm.writeSSTable(newSSTablePath, merged)
	if err != nil {
		return err
	}

	// Update SSTable list
	remaining := lsm.sstables[len(toCompact):]
	lsm.sstables = append([]*SSTable{newSSTable}, remaining...)

	// Clean up old SSTables
	for _, sstable := range toCompact {
		os.Remove(sstable.path)
	}

	return nil
}

// GetRowCount returns the approximate number of rows in the LSM-Tree
func (lsm *LSMIndex) GetRowCount() uint64 {
	lsm.mu.RLock()
	defer lsm.mu.RUnlock()

	count := lsm.memTable.Count()

	for _, immutable := range lsm.immutables {
		count += immutable.Count()
	}

	// Note: For SSTables, we'd need to track unique keys across tables
	// This is a simplified implementation
	return uint64(count)
}

// Close shuts down the LSM-Tree
func (lsm *LSMIndex) Close() error {
	close(lsm.stopChan)
	lsm.wg.Wait()
	return nil
}

// Helper functions will be implemented next...

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Placeholder implementations for functions to be implemented
func (lsm *LSMIndex) loadExistingSSTables() error {
	// TODO: Implement loading of existing SSTables from disk
	return nil
}

// Iterator interface for traversing key-value pairs
type Iterator interface {
	HasNext() bool
	Next() *KeyValue
	Close() error
}

// MergeIterator merges multiple sorted iterators
type MergeIterator struct {
	iterators []Iterator
	current   []*KeyValue
}

func NewMergeIterator(iterators []Iterator) *MergeIterator {
	return &MergeIterator{
		iterators: iterators,
		current:   make([]*KeyValue, len(iterators)),
	}
}

func (mi *MergeIterator) HasNext() bool {
	for i, iter := range mi.iterators {
		if mi.current[i] != nil || iter.HasNext() {
			return true
		}
	}
	return false
}

func (mi *MergeIterator) Next() *KeyValue {
	// Load next from each iterator if needed
	for i, iter := range mi.iterators {
		if mi.current[i] == nil && iter.HasNext() {
			mi.current[i] = iter.Next()
		}
	}

	// Find the smallest key
	var minKV *KeyValue
	var minIndex int = -1

	for i, kv := range mi.current {
		if kv != nil {
			if minKV == nil || string(kv.Key) < string(minKV.Key) {
				minKV = kv
				minIndex = i
			}
		}
	}

	// Clear the selected item
	if minIndex >= 0 {
		mi.current[minIndex] = nil
	}

	return minKV
}

func (mi *MergeIterator) Close() error {
	for _, iter := range mi.iterators {
		iter.Close()
	}
	return nil
}