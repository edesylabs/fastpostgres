// Package storage provides high-performance indexing structures.
// It implements B-Tree, Hash, and Bitmap indexes for fast data access.
package storage

import (
	"fmt"
	"sort"
	"sync"

	"fastpostgres/pkg/engine"
)

// IndexManager manages multiple indexes across tables.
type IndexManager struct {
	indexes map[string]*IndexStructure
	mu      sync.RWMutex
}

// IndexStructure represents a single index with its implementation.
type IndexStructure struct {
	Name       string
	Table      string
	Column     string
	Type       engine.IndexType
	Unique     bool
	btree      *BTreeIndexStruct
	hash       *HashIndexStruct
	bitmap     *BitmapIndexStruct
	mu         sync.RWMutex
}

// BTreeIndexStruct implements a B-Tree index for range queries.
type BTreeIndexStruct struct {
	root   *BTreeNode
	height int
	size   int
}

// BTreeNode represents a node in the B-Tree structure.
type BTreeNode struct {
	keys     []interface{}
	values   [][]int  // Row indices for each key
	children []*BTreeNode
	isLeaf   bool
	parent   *BTreeNode
}

// HashIndexStruct implements a hash index for O(1) equality lookups.
type HashIndexStruct struct {
	buckets []HashBucket
	size    int
	count   int
	mask    int
}

// HashBucket stores entries for a hash table bucket.
type HashBucket struct {
	entries []HashEntry
	mu      sync.RWMutex
}

// HashEntry represents a single hash table entry.
type HashEntry struct {
	key    interface{}
	rowIds []int
}

// BitmapIndexStruct implements a bitmap index for low-cardinality data.
type BitmapIndexStruct struct {
	values  map[interface{}]*Bitmap
	rowCount int
}

// Bitmap stores a bit array for the bitmap index.
type Bitmap struct {
	bits []uint64
	size int
}

const (
	BTreeDegree = 256 // High fan-out for better cache performance
	HashBuckets = 65536
)

// Index entry for bulk loading
type indexEntry struct {
	key   interface{}
	rowId int
}

// NewIndexManager creates a new index manager.
func NewIndexManager() *IndexManager {
	return &IndexManager{
		indexes: make(map[string]*IndexStructure),
	}
}

// CreateIndex creates a new index structure.
func (im *IndexManager) CreateIndex(name, table, column string, indexType engine.IndexType, unique bool) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	if _, exists := im.indexes[name]; exists {
		return fmt.Errorf("index %s already exists", name)
	}

	idx := &IndexStructure{
		Name:   name,
		Table:  table,
		Column: column,
		Type:   indexType,
		Unique: unique,
	}

	switch indexType {
	case engine.BTreeIndex:
		idx.btree = NewBTreeIndexStruct()
	case engine.HashIndex:
		idx.hash = NewHashIndexStruct()
	case engine.BitmapIndex:
		idx.bitmap = NewBitmapIndexStruct()
	default:
		return fmt.Errorf("unsupported index type: %v", indexType)
	}

	im.indexes[name] = idx
	return nil
}

// GetIndex retrieves an index by name.
func (im *IndexManager) GetIndex(name string) *IndexStructure {
	im.mu.RLock()
	defer im.mu.RUnlock()
	return im.indexes[name]
}

// BuildIndex constructs an index from table data.
func (im *IndexManager) BuildIndex(table *engine.Table, indexName string) error {
	im.mu.RLock()
	idx, exists := im.indexes[indexName]
	im.mu.RUnlock()

	if !exists {
		return fmt.Errorf("index %s not found", indexName)
	}

	// Get the column to index
	col := table.GetColumn(idx.Column)
	if col == nil {
		return fmt.Errorf("column %s not found in table %s", idx.Column, idx.Table)
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Build the appropriate index type
	switch idx.Type {
	case engine.BTreeIndex:
		return buildBTreeIndexStruct(idx.btree, col)
	case engine.HashIndex:
		return buildHashIndexStruct(idx.hash, col)
	case engine.BitmapIndex:
		return buildBitmapIndexStruct(idx.bitmap, col)
	default:
		return fmt.Errorf("unsupported index type")
	}
}

// NewBTreeIndexStruct creates a new B-Tree index.
func NewBTreeIndexStruct() *BTreeIndexStruct {
	root := &BTreeNode{
		keys:     make([]interface{}, 0, BTreeDegree-1),
		values:   make([][]int, 0, BTreeDegree-1),
		children: make([]*BTreeNode, 0, BTreeDegree),
		isLeaf:   true,
	}

	return &BTreeIndexStruct{
		root:   root,
		height: 1,
		size:   0,
	}
}

func buildBTreeIndexStruct(btree *BTreeIndexStruct, col *engine.Column) error {
	// Create sorted entries for bulk loading
	var entries []indexEntry

	switch col.Type {
	case engine.TypeInt64:
		data := (*[]int64)(col.Data)
		for i, val := range *data {
			if !col.Nulls[i] {
				entries = append(entries, indexEntry{key: val, rowId: i})
			}
		}
	case engine.TypeString:
		data := (*[]string)(col.Data)
		for i, val := range *data {
			if !col.Nulls[i] {
				entries = append(entries, indexEntry{key: val, rowId: i})
			}
		}
	default:
		return fmt.Errorf("unsupported column type for B-tree index")
	}

	// Sort entries by key for bulk loading
	sort.Slice(entries, func(i, j int) bool {
		return compareValues(entries[i].key, entries[j].key) < 0
	})

	// Bulk load into B-tree
	return bulkLoadBTree(btree, entries)
}

func bulkLoadBTree(btree *BTreeIndexStruct, entries []indexEntry) error {
	if len(entries) == 0 {
		return nil
	}

	// Group entries by key
	var keys []interface{}
	var rowIdLists [][]int

	currentKey := entries[0].key
	currentRows := []int{entries[0].rowId}

	for i := 1; i < len(entries); i++ {
		if compareValues(entries[i].key, currentKey) == 0 {
			// Same key, add to current group
			currentRows = append(currentRows, entries[i].rowId)
		} else {
			// New key, save current group
			keys = append(keys, currentKey)
			rowIdLists = append(rowIdLists, currentRows)

			currentKey = entries[i].key
			currentRows = []int{entries[i].rowId}
		}
	}

	// Add the last group
	keys = append(keys, currentKey)
	rowIdLists = append(rowIdLists, currentRows)

	// Build tree bottom-up for optimal performance
	btree.root.keys = keys
	btree.root.values = rowIdLists
	btree.size = len(keys)

	return nil
}

func (btree *BTreeIndexStruct) Search(key interface{}) []int {
	return btree.searchNode(btree.root, key)
}

func (btree *BTreeIndexStruct) searchNode(node *BTreeNode, key interface{}) []int {
	// Binary search within node
	left, right := 0, len(node.keys)

	for left < right {
		mid := (left + right) / 2
		cmp := compareValues(key, node.keys[mid])

		if cmp == 0 {
			return node.values[mid] // Found exact match
		} else if cmp < 0 {
			right = mid
		} else {
			left = mid + 1
		}
	}

	if node.isLeaf {
		return nil // Not found
	}

	// Search child node
	if left < len(node.children) {
		return btree.searchNode(node.children[left], key)
	}

	return nil
}

func (btree *BTreeIndexStruct) RangeSearch(minKey, maxKey interface{}) []int {
	var result []int
	btree.rangeSearchNode(btree.root, minKey, maxKey, &result)
	return result
}

func (btree *BTreeIndexStruct) rangeSearchNode(node *BTreeNode, minKey, maxKey interface{}, result *[]int) {
	for i, key := range node.keys {
		cmpMin := compareValues(key, minKey)
		cmpMax := compareValues(key, maxKey)

		if cmpMin >= 0 && cmpMax <= 0 {
			// Key is in range
			*result = append(*result, node.values[i]...)
		}

		if !node.isLeaf && i < len(node.children) {
			// Search child if it might contain keys in range
			if compareValues(maxKey, key) >= 0 {
				btree.rangeSearchNode(node.children[i], minKey, maxKey, result)
			}
		}
	}

	// Search rightmost child
	if !node.isLeaf && len(node.children) > len(node.keys) {
		lastChild := node.children[len(node.children)-1]
		btree.rangeSearchNode(lastChild, minKey, maxKey, result)
	}
}

// NewHashIndexStruct creates a new hash index.
func NewHashIndexStruct() *HashIndexStruct {
	buckets := make([]HashBucket, HashBuckets)
	for i := range buckets {
		buckets[i].entries = make([]HashEntry, 0)
	}

	return &HashIndexStruct{
		buckets: buckets,
		size:    HashBuckets,
		mask:    HashBuckets - 1,
	}
}

func buildHashIndexStruct(hash *HashIndexStruct, col *engine.Column) error {
	switch col.Type {
	case engine.TypeInt64:
		data := (*[]int64)(col.Data)
		for i, val := range *data {
			if !col.Nulls[i] {
				hash.Insert(val, i)
			}
		}
	case engine.TypeString:
		data := (*[]string)(col.Data)
		for i, val := range *data {
			if !col.Nulls[i] {
				hash.Insert(val, i)
			}
		}
	default:
		return fmt.Errorf("unsupported column type for hash index")
	}

	return nil
}

func (hash *HashIndexStruct) Insert(key interface{}, rowId int) {
	bucketIdx := hash.hashKey(key) & hash.mask
	bucket := &hash.buckets[bucketIdx]

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	// Look for existing key
	for i := range bucket.entries {
		if compareValues(bucket.entries[i].key, key) == 0 {
			bucket.entries[i].rowIds = append(bucket.entries[i].rowIds, rowId)
			return
		}
	}

	// Add new entry
	bucket.entries = append(bucket.entries, HashEntry{
		key:    key,
		rowIds: []int{rowId},
	})
}

func (hash *HashIndexStruct) Search(key interface{}) []int {
	bucketIdx := hash.hashKey(key) & hash.mask
	bucket := &hash.buckets[bucketIdx]

	bucket.mu.RLock()
	defer bucket.mu.RUnlock()

	for _, entry := range bucket.entries {
		if compareValues(entry.key, key) == 0 {
			return entry.rowIds
		}
	}

	return nil
}

func (hash *HashIndexStruct) hashKey(key interface{}) int {
	switch v := key.(type) {
	case int64:
		return int(v*2654435761) % hash.size
	case string:
		h := 0
		for _, c := range v {
			h = h*31 + int(c)
		}
		return h % hash.size
	default:
		return 0
	}
}

// NewBitmapIndexStruct creates a new bitmap index.
func NewBitmapIndexStruct() *BitmapIndexStruct {
	return &BitmapIndexStruct{
		values: make(map[interface{}]*Bitmap),
	}
}

func buildBitmapIndexStruct(bitmap *BitmapIndexStruct, col *engine.Column) error {
	bitmap.rowCount = int(col.Length)

	switch col.Type {
	case engine.TypeInt64:
		data := (*[]int64)(col.Data)
		for i, val := range *data {
			if !col.Nulls[i] {
				if bitmap.values[val] == nil {
					bitmap.values[val] = NewBitmap(bitmap.rowCount)
				}
				bitmap.values[val].Set(i)
			}
		}
	case engine.TypeString:
		data := (*[]string)(col.Data)
		for i, val := range *data {
			if !col.Nulls[i] {
				if bitmap.values[val] == nil {
					bitmap.values[val] = NewBitmap(bitmap.rowCount)
				}
				bitmap.values[val].Set(i)
			}
		}
	default:
		return fmt.Errorf("unsupported column type for bitmap index")
	}

	return nil
}

// NewBitmap creates a new bitmap with the specified size.
func NewBitmap(size int) *Bitmap {
	wordCount := (size + 63) / 64
	return &Bitmap{
		bits: make([]uint64, wordCount),
		size: size,
	}
}

func (b *Bitmap) Set(position int) {
	if position >= b.size {
		return
	}
	wordIdx := position / 64
	bitIdx := position % 64
	b.bits[wordIdx] |= 1 << uint(bitIdx)
}

func (b *Bitmap) Get(position int) bool {
	if position >= b.size {
		return false
	}
	wordIdx := position / 64
	bitIdx := position % 64
	return (b.bits[wordIdx] & (1 << uint(bitIdx))) != 0
}

func (b *Bitmap) GetSetBits() []int {
	var result []int
	for i := 0; i < b.size; i++ {
		if b.Get(i) {
			result = append(result, i)
		}
	}
	return result
}

func (bitmap *BitmapIndexStruct) Search(key interface{}) []int {
	if bm, exists := bitmap.values[key]; exists {
		return bm.GetSetBits()
	}
	return nil
}

// Utility functions
func compareValues(a, b interface{}) int {
	switch va := a.(type) {
	case int64:
		if vb, ok := b.(int64); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case string:
		if vb, ok := b.(string); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	}
	return 0
}

// Note: VectorizedEngine methods for indexed queries have been moved to pkg/query
// to avoid circular import dependencies.

func intersectRowIds(a, b []int) []int {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}

	aMap := make(map[int]bool)
	for _, id := range a {
		aMap[id] = true
	}

	var result []int
	for _, id := range b {
		if aMap[id] {
			result = append(result, id)
		}
	}

	return result
}

func getMinValue(value interface{}) interface{} {
	switch value.(type) {
	case int64:
		return int64(-9223372036854775808) // math.MinInt64
	case string:
		return ""
	}
	return value
}

func getMaxValue(value interface{}) interface{} {
	switch value.(type) {
	case int64:
		return int64(9223372036854775807) // math.MaxInt64
	case string:
		return string(rune(0x10FFFF)) // Max Unicode
	}
	return value
}