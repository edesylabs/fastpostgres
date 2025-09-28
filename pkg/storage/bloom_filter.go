// Package storage provides Bloom Filter implementation for probabilistic membership testing.
// Bloom filters provide fast lookups with no false negatives and configurable false positive rate.
package storage

import (
	"fmt"
	"hash/fnv"
	"math"
)

// NewBloomFilter creates a new bloom filter optimized for the expected number of elements
// and desired false positive rate
func NewBloomFilter(expectedElements int, falsePositiveRate float64) *BloomFilter {
	// Calculate optimal size and number of hash functions
	// m = -n * ln(p) / (ln(2)^2)
	// k = (m / n) * ln(2)

	n := float64(expectedElements)
	p := falsePositiveRate

	// Calculate optimal bit array size
	m := -n * math.Log(p) / (math.Log(2) * math.Log(2))
	bitSize := uint64(math.Ceil(m))

	// Calculate optimal number of hash functions
	k := (m / n) * math.Log(2)
	hashCount := int(math.Ceil(k))

	// Ensure minimum values
	if bitSize < 64 {
		bitSize = 64
	}
	if hashCount < 1 {
		hashCount = 1
	}
	if hashCount > 20 {
		hashCount = 20 // Practical limit
	}

	// Round up to nearest multiple of 64 for efficient storage
	wordCount := (bitSize + 63) / 64
	actualBitSize := wordCount * 64

	return &BloomFilter{
		bits:     make([]uint64, wordCount),
		size:     actualBitSize,
		hashes:   hashCount,
		elements: 0,
	}
}

// Add inserts an element into the bloom filter
func (bf *BloomFilter) Add(data []byte) {
	hash1, hash2 := bf.hash(data)

	for i := 0; i < bf.hashes; i++ {
		// Double hashing: hash_i = hash1 + i * hash2
		hash := (hash1 + uint64(i)*hash2) % bf.size
		wordIndex := hash / 64
		bitIndex := hash % 64
		bf.bits[wordIndex] |= 1 << bitIndex
	}

	bf.elements++
}

// MayContain tests whether an element might be in the set
// Returns false if definitely not in set, true if possibly in set
func (bf *BloomFilter) MayContain(data []byte) bool {
	hash1, hash2 := bf.hash(data)

	for i := 0; i < bf.hashes; i++ {
		hash := (hash1 + uint64(i)*hash2) % bf.size
		wordIndex := hash / 64
		bitIndex := hash % 64

		if (bf.bits[wordIndex] & (1 << bitIndex)) == 0 {
			return false // Definitely not in set
		}
	}

	return true // Possibly in set
}

// EstimatedFalsePositiveRate calculates the current false positive rate
func (bf *BloomFilter) EstimatedFalsePositiveRate() float64 {
	if bf.elements == 0 {
		return 0.0
	}

	// p = (1 - e^(-kn/m))^k
	k := float64(bf.hashes)
	n := float64(bf.elements)
	m := float64(bf.size)

	return math.Pow(1-math.Exp(-k*n/m), k)
}

// Size returns the size of the bit array
func (bf *BloomFilter) Size() uint64 {
	return bf.size
}

// ElementCount returns the number of elements added
func (bf *BloomFilter) ElementCount() uint64 {
	return bf.elements
}

// hash computes two independent hash values using FNV
func (bf *BloomFilter) hash(data []byte) (uint64, uint64) {
	// Use FNV-1a for first hash
	h1 := fnv.New64a()
	h1.Write(data)
	hash1 := h1.Sum64()

	// Create second hash by combining with a different constant
	// This gives us a different hash function
	h2 := fnv.New64a()
	h2.Write(data)
	h2.Write([]byte{0xAA, 0xBB, 0xCC, 0xDD}) // Add salt for different hash
	hash2 := h2.Sum64()

	return hash1, hash2
}

// Clear resets the bloom filter
func (bf *BloomFilter) Clear() {
	for i := range bf.bits {
		bf.bits[i] = 0
	}
	bf.elements = 0
}

// Union combines this bloom filter with another (both must have same parameters)
func (bf *BloomFilter) Union(other *BloomFilter) error {
	if bf.size != other.size || bf.hashes != other.hashes {
		return fmt.Errorf("bloom filters must have same size and hash count for union")
	}

	for i := range bf.bits {
		bf.bits[i] |= other.bits[i]
	}

	// Element count is not accurate after union, but we can estimate
	bf.elements += other.elements

	return nil
}

// Intersection creates a new bloom filter that represents the intersection
func (bf *BloomFilter) Intersection(other *BloomFilter) (*BloomFilter, error) {
	if bf.size != other.size || bf.hashes != other.hashes {
		return nil, fmt.Errorf("bloom filters must have same size and hash count for intersection")
	}

	result := &BloomFilter{
		bits:     make([]uint64, len(bf.bits)),
		size:     bf.size,
		hashes:   bf.hashes,
		elements: 0, // Can't accurately estimate
	}

	for i := range bf.bits {
		result.bits[i] = bf.bits[i] & other.bits[i]
	}

	return result, nil
}

// Serialize returns the bloom filter as bytes for storage
func (bf *BloomFilter) Serialize() []byte {
	// Format: size(8) + hashes(4) + elements(8) + bits
	data := make([]byte, 20+len(bf.bits)*8)

	// Encode metadata
	for i := 0; i < 8; i++ {
		data[i] = byte(bf.size >> (i * 8))
	}
	for i := 0; i < 4; i++ {
		data[8+i] = byte(bf.hashes >> (i * 8))
	}
	for i := 0; i < 8; i++ {
		data[12+i] = byte(bf.elements >> (i * 8))
	}

	// Encode bits
	for i, word := range bf.bits {
		for j := 0; j < 8; j++ {
			data[20+i*8+j] = byte(word >> (j * 8))
		}
	}

	return data
}

// Deserialize creates a bloom filter from serialized bytes
func DeserializeBloomFilter(data []byte) (*BloomFilter, error) {
	if len(data) < 20 {
		return nil, fmt.Errorf("invalid bloom filter data: too short")
	}

	// Decode metadata
	var size, elements uint64
	var hashes int

	for i := 0; i < 8; i++ {
		size |= uint64(data[i]) << (i * 8)
	}
	for i := 0; i < 4; i++ {
		hashes |= int(data[8+i]) << (i * 8)
	}
	for i := 0; i < 8; i++ {
		elements |= uint64(data[12+i]) << (i * 8)
	}

	// Calculate expected bit array size
	wordCount := int((size + 63) / 64)
	expectedDataSize := 20 + wordCount*8

	if len(data) != expectedDataSize {
		return nil, fmt.Errorf("invalid bloom filter data: size mismatch")
	}

	// Decode bits
	bits := make([]uint64, wordCount)
	for i := 0; i < wordCount; i++ {
		for j := 0; j < 8; j++ {
			bits[i] |= uint64(data[20+i*8+j]) << (j * 8)
		}
	}

	return &BloomFilter{
		bits:     bits,
		size:     size,
		hashes:   hashes,
		elements: elements,
	}, nil
}

// Stats returns statistics about the bloom filter
type BloomFilterStats struct {
	Size                    uint64
	HashFunctions          int
	Elements               uint64
	EstimatedFalsePositive float64
	MemoryUsage            int64 // bytes
}

func (bf *BloomFilter) Stats() BloomFilterStats {
	return BloomFilterStats{
		Size:                    bf.size,
		HashFunctions:          bf.hashes,
		Elements:               bf.elements,
		EstimatedFalsePositive: bf.EstimatedFalsePositiveRate(),
		MemoryUsage:            int64(len(bf.bits) * 8),
	}
}