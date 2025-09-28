// Package storage provides Bitmap Index implementation for fast analytical queries.
// Bitmap indexes are ideal for low-cardinality columns and provide very fast AND/OR operations.
package storage

import (
	"sync"
)

// BitmapIndex provides fast lookups for categorical data using compressed bitmaps
type BitmapIndex struct {
	name      string
	column    string
	bitmaps   map[interface{}]*RoaringBitmap
	rowCount  uint64
	mu        sync.RWMutex
}

// RoaringBitmap provides compressed bitmap operations
// This is a simplified implementation - in production, use github.com/RoaringBitmap/roaring
type RoaringBitmap struct {
	containers map[uint16]*Container
	size       uint64
}

// Container represents a 64K chunk of bits
type Container struct {
	containerType ContainerType
	data          interface{} // Either []uint16 or []uint64
	cardinality   uint32
}

type ContainerType uint8

const (
	ArrayContainer  ContainerType = iota // Sparse: array of set bits
	BitmapContainer                      // Dense: 64-bit words
	RunContainer                         // Runs of consecutive bits
)

const (
	containerSize     = 65536 // 2^16
	arrayToBitmapSize = 4096   // Threshold to convert array to bitmap
)

// NewBitmapIndex creates a new bitmap index for a column
func NewBitmapIndex(name, column string) *BitmapIndex {
	return &BitmapIndex{
		name:     name,
		column:   column,
		bitmaps:  make(map[interface{}]*RoaringBitmap),
		rowCount: 0,
	}
}

// Add adds a value for a specific row ID
func (bi *BitmapIndex) Add(rowID uint64, value interface{}) {
	bi.mu.Lock()
	defer bi.mu.Unlock()

	// Get or create bitmap for this value
	bitmap, exists := bi.bitmaps[value]
	if !exists {
		bitmap = NewRoaringBitmap()
		bi.bitmaps[value] = bitmap
	}

	// Add row ID to bitmap
	bitmap.Add(rowID)

	// Update row count
	if rowID >= bi.rowCount {
		bi.rowCount = rowID + 1
	}
}

// Query returns row IDs where column equals the given value
func (bi *BitmapIndex) Query(value interface{}) *RoaringBitmap {
	bi.mu.RLock()
	defer bi.mu.RUnlock()

	bitmap, exists := bi.bitmaps[value]
	if !exists {
		return NewRoaringBitmap() // Empty bitmap
	}

	return bitmap.Clone()
}

// QueryRange returns row IDs where column value is in the given range
func (bi *BitmapIndex) QueryRange(minValue, maxValue interface{}) *RoaringBitmap {
	bi.mu.RLock()
	defer bi.mu.RUnlock()

	result := NewRoaringBitmap()

	// For simplicity, only handle numeric ranges
	switch min := minValue.(type) {
	case int64:
		max := maxValue.(int64)
		for value, bitmap := range bi.bitmaps {
			if val, ok := value.(int64); ok && val >= min && val <= max {
				result = result.Or(bitmap)
			}
		}
	case string:
		max := maxValue.(string)
		for value, bitmap := range bi.bitmaps {
			if val, ok := value.(string); ok && val >= min && val <= max {
				result = result.Or(bitmap)
			}
		}
	}

	return result
}

// QueryIn returns row IDs where column value is in the given set
func (bi *BitmapIndex) QueryIn(values []interface{}) *RoaringBitmap {
	bi.mu.RLock()
	defer bi.mu.RUnlock()

	result := NewRoaringBitmap()

	for _, value := range values {
		if bitmap, exists := bi.bitmaps[value]; exists {
			result = result.Or(bitmap)
		}
	}

	return result
}

// GetCardinality returns the number of distinct values
func (bi *BitmapIndex) GetCardinality() int {
	bi.mu.RLock()
	defer bi.mu.RUnlock()
	return len(bi.bitmaps)
}

// GetRowCount returns the total number of rows indexed
func (bi *BitmapIndex) GetRowCount() uint64 {
	bi.mu.RLock()
	defer bi.mu.RUnlock()
	return bi.rowCount
}

// GetValues returns all distinct values in the index
func (bi *BitmapIndex) GetValues() []interface{} {
	bi.mu.RLock()
	defer bi.mu.RUnlock()

	values := make([]interface{}, 0, len(bi.bitmaps))
	for value := range bi.bitmaps {
		values = append(values, value)
	}
	return values
}

// Stats returns statistics about the bitmap index
type BitmapIndexStats struct {
	Name           string
	Column         string
	Cardinality    int
	RowCount       uint64
	MemoryUsage    int64
	CompressionRatio float64
}

func (bi *BitmapIndex) Stats() BitmapIndexStats {
	bi.mu.RLock()
	defer bi.mu.RUnlock()

	var totalMemory int64
	var uncompressedSize int64

	for _, bitmap := range bi.bitmaps {
		totalMemory += bitmap.GetSizeInBytes()
		uncompressedSize += int64(bi.rowCount / 8) // Uncompressed bitmap size
	}

	compressionRatio := 1.0
	if totalMemory > 0 {
		compressionRatio = float64(uncompressedSize) / float64(totalMemory)
	}

	return BitmapIndexStats{
		Name:             bi.name,
		Column:           bi.column,
		Cardinality:      len(bi.bitmaps),
		RowCount:         bi.rowCount,
		MemoryUsage:      totalMemory,
		CompressionRatio: compressionRatio,
	}
}

// RoaringBitmap implementation

// NewRoaringBitmap creates a new roaring bitmap
func NewRoaringBitmap() *RoaringBitmap {
	return &RoaringBitmap{
		containers: make(map[uint16]*Container),
		size:       0,
	}
}

// Add adds a value to the bitmap
func (rb *RoaringBitmap) Add(value uint64) {
	// Split into high and low parts
	high := uint16(value >> 16)
	low := uint16(value & 0xFFFF)

	// Get or create container
	container, exists := rb.containers[high]
	if !exists {
		container = newArrayContainer()
		rb.containers[high] = container
	}

	// Add to container
	if container.add(low) {
		rb.size++
	}
}

// Contains checks if a value is in the bitmap
func (rb *RoaringBitmap) Contains(value uint64) bool {
	high := uint16(value >> 16)
	low := uint16(value & 0xFFFF)

	container, exists := rb.containers[high]
	if !exists {
		return false
	}

	return container.contains(low)
}

// Or performs bitwise OR with another bitmap
func (rb *RoaringBitmap) Or(other *RoaringBitmap) *RoaringBitmap {
	result := NewRoaringBitmap()

	// Add all containers from this bitmap
	for high, container := range rb.containers {
		result.containers[high] = container.clone()
	}

	// OR with containers from other bitmap
	for high, otherContainer := range other.containers {
		if container, exists := result.containers[high]; exists {
			result.containers[high] = container.or(otherContainer)
		} else {
			result.containers[high] = otherContainer.clone()
		}
	}

	// Recalculate size
	result.size = 0
	for _, container := range result.containers {
		result.size += uint64(container.cardinality)
	}

	return result
}

// And performs bitwise AND with another bitmap
func (rb *RoaringBitmap) And(other *RoaringBitmap) *RoaringBitmap {
	result := NewRoaringBitmap()

	// Only process containers that exist in both bitmaps
	for high, container := range rb.containers {
		if otherContainer, exists := other.containers[high]; exists {
			andResult := container.and(otherContainer)
			if andResult.cardinality > 0 {
				result.containers[high] = andResult
				result.size += uint64(andResult.cardinality)
			}
		}
	}

	return result
}

// GetSize returns the number of set bits
func (rb *RoaringBitmap) GetSize() uint64 {
	return rb.size
}

// GetSizeInBytes returns the memory usage in bytes
func (rb *RoaringBitmap) GetSizeInBytes() int64 {
	var size int64

	for _, container := range rb.containers {
		switch container.containerType {
		case ArrayContainer:
			array := container.data.([]uint16)
			size += int64(len(array) * 2) // 2 bytes per uint16
		case BitmapContainer:
			bitmap := container.data.([]uint64)
			size += int64(len(bitmap) * 8) // 8 bytes per uint64
		}
		size += 8 // Container overhead
	}

	return size
}

// Clone creates a copy of the bitmap
func (rb *RoaringBitmap) Clone() *RoaringBitmap {
	result := NewRoaringBitmap()
	result.size = rb.size

	for high, container := range rb.containers {
		result.containers[high] = container.clone()
	}

	return result
}

// ToArray returns all values as a sorted array
func (rb *RoaringBitmap) ToArray() []uint64 {
	result := make([]uint64, 0, rb.size)

	// Process containers in order
	for high := uint16(0); ; high++ {
		container, exists := rb.containers[high]
		if !exists {
			if high == 0xFFFF {
				break
			}
			continue
		}

		// Get values from container
		containerValues := container.toArray()
		for _, low := range containerValues {
			value := (uint64(high) << 16) | uint64(low)
			result = append(result, value)
		}

		if high == 0xFFFF {
			break
		}
	}

	return result
}

// Container implementation

func newArrayContainer() *Container {
	return &Container{
		containerType: ArrayContainer,
		data:          make([]uint16, 0),
		cardinality:   0,
	}
}

func newBitmapContainer() *Container {
	return &Container{
		containerType: BitmapContainer,
		data:          make([]uint64, 1024), // 64K bits = 1024 uint64s
		cardinality:   0,
	}
}

func (c *Container) add(value uint16) bool {
	switch c.containerType {
	case ArrayContainer:
		return c.addToArray(value)
	case BitmapContainer:
		return c.addToBitmap(value)
	default:
		return false
	}
}

func (c *Container) addToArray(value uint16) bool {
	array := c.data.([]uint16)

	// Binary search for insertion point
	left, right := 0, len(array)
	for left < right {
		mid := (left + right) / 2
		if array[mid] < value {
			left = mid + 1
		} else {
			right = mid
		}
	}

	// Check if value already exists
	if left < len(array) && array[left] == value {
		return false
	}

	// Insert value
	array = append(array, 0)
	copy(array[left+1:], array[left:])
	array[left] = value
	c.data = array
	c.cardinality++

	// Convert to bitmap if array gets too large
	if c.cardinality > arrayToBitmapSize {
		c.convertToBitmap()
	}

	return true
}

func (c *Container) addToBitmap(value uint16) bool {
	bitmap := c.data.([]uint64)
	wordIndex := value / 64
	bitIndex := value % 64

	oldWord := bitmap[wordIndex]
	bitmap[wordIndex] |= 1 << bitIndex

	if bitmap[wordIndex] != oldWord {
		c.cardinality++
		return true
	}

	return false
}

func (c *Container) contains(value uint16) bool {
	switch c.containerType {
	case ArrayContainer:
		array := c.data.([]uint16)
		// Binary search
		left, right := 0, len(array)
		for left < right {
			mid := (left + right) / 2
			if array[mid] < value {
				left = mid + 1
			} else {
				right = mid
			}
		}
		return left < len(array) && array[left] == value

	case BitmapContainer:
		bitmap := c.data.([]uint64)
		wordIndex := value / 64
		bitIndex := value % 64
		return (bitmap[wordIndex] & (1 << bitIndex)) != 0

	default:
		return false
	}
}

func (c *Container) convertToBitmap() {
	if c.containerType != ArrayContainer {
		return
	}

	array := c.data.([]uint16)
	bitmap := make([]uint64, 1024)

	for _, value := range array {
		wordIndex := value / 64
		bitIndex := value % 64
		bitmap[wordIndex] |= 1 << bitIndex
	}

	c.containerType = BitmapContainer
	c.data = bitmap
}

func (c *Container) clone() *Container {
	switch c.containerType {
	case ArrayContainer:
		array := c.data.([]uint16)
		newArray := make([]uint16, len(array))
		copy(newArray, array)
		return &Container{
			containerType: ArrayContainer,
			data:          newArray,
			cardinality:   c.cardinality,
		}

	case BitmapContainer:
		bitmap := c.data.([]uint64)
		newBitmap := make([]uint64, len(bitmap))
		copy(newBitmap, bitmap)
		return &Container{
			containerType: BitmapContainer,
			data:          newBitmap,
			cardinality:   c.cardinality,
		}

	default:
		return c
	}
}

func (c *Container) or(other *Container) *Container {
	// Simplified OR implementation
	// In practice, would optimize based on container types
	result := c.clone()

	switch other.containerType {
	case ArrayContainer:
		array := other.data.([]uint16)
		for _, value := range array {
			result.add(value)
		}
	case BitmapContainer:
		if result.containerType == ArrayContainer {
			result.convertToBitmap()
		}
		resultBitmap := result.data.([]uint64)
		otherBitmap := other.data.([]uint64)
		result.cardinality = 0

		for i := range resultBitmap {
			resultBitmap[i] |= otherBitmap[i]
			// Count bits (simplified)
			result.cardinality += uint32(popCount(resultBitmap[i]))
		}
	}

	return result
}

func (c *Container) and(other *Container) *Container {
	// Simplified AND implementation
	result := newArrayContainer()

	// Convert both to arrays for simplicity
	thisValues := c.toArray()
	otherValues := other.toArray()

	// Merge sorted arrays
	i, j := 0, 0
	for i < len(thisValues) && j < len(otherValues) {
		if thisValues[i] == otherValues[j] {
			result.add(thisValues[i])
			i++
			j++
		} else if thisValues[i] < otherValues[j] {
			i++
		} else {
			j++
		}
	}

	return result
}

func (c *Container) toArray() []uint16 {
	switch c.containerType {
	case ArrayContainer:
		array := c.data.([]uint16)
		result := make([]uint16, len(array))
		copy(result, array)
		return result

	case BitmapContainer:
		bitmap := c.data.([]uint64)
		result := make([]uint16, 0, c.cardinality)

		for i, word := range bitmap {
			if word != 0 {
				for bit := 0; bit < 64; bit++ {
					if (word & (1 << bit)) != 0 {
						value := uint16(i*64 + bit)
						result = append(result, value)
					}
				}
			}
		}

		return result

	default:
		return nil
	}
}

// popCount counts the number of set bits in a 64-bit word
func popCount(x uint64) int {
	// Brian Kernighan's algorithm
	count := 0
	for x != 0 {
		count++
		x &= x - 1
	}
	return count
}