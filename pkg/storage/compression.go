package storage

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sync"
	"unsafe"

	"fastpostgres/pkg/engine"
)

// Compression engine for columnar data
type CompressionEngine struct {
	algorithms map[engine.DataType]CompressionAlgorithm
	stats      *CompressionStats
	mu         sync.RWMutex
}

type CompressionAlgorithm interface {
	Compress(data unsafe.Pointer, length int) (*CompressedBlock, error)
	Decompress(block *CompressedBlock) (unsafe.Pointer, error)
	EstimateRatio(data unsafe.Pointer, length int) float64
	GetType() CompressionType
}

type CompressionType uint8

const (
	CompressionNone CompressionType = iota
	CompressionRLE        // Run-Length Encoding
	CompressionDelta      // Delta Encoding
	CompressionDictionary // Dictionary Encoding
	CompressionBitPacking // Bit Packing
	CompressionGZIP       // GZIP compression
	CompressionLZ4        // LZ4 compression (placeholder)
)

// Compressed data block
type CompressedBlock struct {
	Type         CompressionType
	OriginalSize int
	CompressedSize int
	Data         []byte
	Metadata     map[string]interface{} // Algorithm-specific metadata
}

// Compression statistics
type CompressionStats struct {
	TotalOriginalBytes   int64
	TotalCompressedBytes int64
	CompressionRatio     float64
	AlgorithmStats       map[CompressionType]*AlgorithmStats
	mu                   sync.RWMutex
}

type AlgorithmStats struct {
	UsageCount       int64
	BytesCompressed  int64
	CompressionRatio float64
	AvgCompressionTime int64 // nanoseconds
}

// Run-Length Encoding for repetitive data
type RLECompressor struct{}

type RLEEntry struct {
	Value interface{}
	Count int32
}

// Delta encoding for sequential numeric data
type DeltaCompressor struct{}

// Dictionary encoding for low-cardinality string data
type DictionaryCompressor struct{}

type DictionaryBlock struct {
	Dictionary []string
	Indices    []uint32
}

// Bit packing for small integer ranges
type BitPackingCompressor struct{}

// GZIP compressor for general-purpose compression
type GZIPCompressor struct{}

// NewCompressionEngine creates a new compression engine
func NewCompressionEngine() *CompressionEngine {
	ce := &CompressionEngine{
		algorithms: make(map[engine.DataType]CompressionAlgorithm),
		stats: &CompressionStats{
			AlgorithmStats: make(map[CompressionType]*AlgorithmStats),
		},
	}

	// Register compression algorithms for different data types
	ce.algorithms[engine.TypeInt64] = &DeltaCompressor{}
	ce.algorithms[engine.TypeString] = &DictionaryCompressor{}
	ce.algorithms[engine.TypeInt32] = &BitPackingCompressor{}

	return ce
}

// Compress column data using the best algorithm for the data type
func (ce *CompressionEngine) CompressColumn(col *engine.Column) (*CompressedBlock, error) {
	ce.mu.Lock()
	defer ce.mu.Unlock()

	// Choose compression algorithm based on data characteristics
	algorithm := ce.selectBestAlgorithm(col)

	// Compress the data
	block, err := algorithm.Compress(col.Data, int(col.Length))
	if err != nil {
		return nil, err
	}

	// Update statistics
	ce.updateStats(algorithm.GetType(), block)

	return block, nil
}

// Select the best compression algorithm for the given column
func (ce *CompressionEngine) selectBestAlgorithm(col *engine.Column) CompressionAlgorithm {
	// Analyze data characteristics
	switch col.Type {
	case engine.TypeInt64:
		if ce.hasSequentialPattern(col) {
			return &DeltaCompressor{}
		}
		return &RLECompressor{}
	case engine.TypeString:
		cardinality := ce.calculateCardinality(col)
		if float64(cardinality) / float64(col.Length) < 0.1 {
			return &DictionaryCompressor{}
		}
		return &GZIPCompressor{}
	case engine.TypeInt32:
		return &BitPackingCompressor{}
	default:
		return &GZIPCompressor{}
	}
}

// RLE Compression Implementation
func (rle *RLECompressor) Compress(data unsafe.Pointer, length int) (*CompressedBlock, error) {
	// Try to cast to different types based on the data pointer
	if int64Data := (*[]int64)(data); int64Data != nil {
		return rle.compressInt64(*int64Data)
	}
	if stringData := (*[]string)(data); stringData != nil {
		return rle.compressString(*stringData)
	}
	return nil, fmt.Errorf("unsupported data type for RLE")
}

func (rle *RLECompressor) compressInt64(data []int64) (*CompressedBlock, error) {
	if len(data) == 0 {
		return &CompressedBlock{Type: CompressionRLE}, nil
	}

	var entries []RLEEntry
	currentValue := data[0]
	currentCount := int32(1)

	for i := 1; i < len(data); i++ {
		if data[i] == currentValue {
			currentCount++
		} else {
			entries = append(entries, RLEEntry{Value: currentValue, Count: currentCount})
			currentValue = data[i]
			currentCount = 1
		}
	}
	// Add the last run
	entries = append(entries, RLEEntry{Value: currentValue, Count: currentCount})

	// Serialize RLE entries
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, int32(len(entries)))

	for _, entry := range entries {
		binary.Write(&buf, binary.LittleEndian, entry.Value.(int64))
		binary.Write(&buf, binary.LittleEndian, entry.Count)
	}

	return &CompressedBlock{
		Type:           CompressionRLE,
		OriginalSize:   len(data) * 8, // 8 bytes per int64
		CompressedSize: buf.Len(),
		Data:          buf.Bytes(),
	}, nil
}

func (rle *RLECompressor) compressString(data []string) (*CompressedBlock, error) {
	// String RLE implementation
	if len(data) == 0 {
		return &CompressedBlock{Type: CompressionRLE}, nil
	}

	var buf bytes.Buffer
	currentValue := data[0]
	currentCount := int32(1)

	for i := 1; i < len(data); i++ {
		if data[i] == currentValue {
			currentCount++
		} else {
			// Write run
			binary.Write(&buf, binary.LittleEndian, int32(len(currentValue)))
			buf.WriteString(currentValue)
			binary.Write(&buf, binary.LittleEndian, currentCount)

			currentValue = data[i]
			currentCount = 1
		}
	}

	// Write last run
	binary.Write(&buf, binary.LittleEndian, int32(len(currentValue)))
	buf.WriteString(currentValue)
	binary.Write(&buf, binary.LittleEndian, currentCount)

	originalSize := 0
	for _, s := range data {
		originalSize += len(s)
	}

	return &CompressedBlock{
		Type:           CompressionRLE,
		OriginalSize:   originalSize,
		CompressedSize: buf.Len(),
		Data:          buf.Bytes(),
	}, nil
}

func (rle *RLECompressor) Decompress(block *CompressedBlock) (unsafe.Pointer, error) {
	// RLE decompression implementation
	return nil, fmt.Errorf("RLE decompression not implemented")
}

func (rle *RLECompressor) EstimateRatio(data unsafe.Pointer, length int) float64 {
	// Estimate compression ratio by analyzing run lengths
	return 0.5 // Placeholder
}

func (rle *RLECompressor) GetType() CompressionType {
	return CompressionRLE
}

// Delta Compression Implementation
func (delta *DeltaCompressor) Compress(data unsafe.Pointer, length int) (*CompressedBlock, error) {
	int64Data := (*[]int64)(data)
	if len(*int64Data) == 0 {
		return &CompressedBlock{Type: CompressionDelta}, nil
	}

	var buf bytes.Buffer

	// Write base value
	baseValue := (*int64Data)[0]
	binary.Write(&buf, binary.LittleEndian, baseValue)

	// Write deltas
	for i := 1; i < len(*int64Data); i++ {
		delta := (*int64Data)[i] - (*int64Data)[i-1]

		// Use variable-length encoding for deltas
		if err := writeVarInt(&buf, delta); err != nil {
			return nil, err
		}
	}

	return &CompressedBlock{
		Type:           CompressionDelta,
		OriginalSize:   len(*int64Data) * 8,
		CompressedSize: buf.Len(),
		Data:          buf.Bytes(),
		Metadata:       map[string]interface{}{"base": baseValue},
	}, nil
}

func (delta *DeltaCompressor) Decompress(block *CompressedBlock) (unsafe.Pointer, error) {
	// Delta decompression implementation
	return nil, fmt.Errorf("Delta decompression not implemented")
}

func (delta *DeltaCompressor) EstimateRatio(data unsafe.Pointer, length int) float64 {
	// Analyze delta patterns to estimate compression ratio
	return 0.3 // Placeholder
}

func (delta *DeltaCompressor) GetType() CompressionType {
	return CompressionDelta
}

// Dictionary Compression Implementation
func (dict *DictionaryCompressor) Compress(data unsafe.Pointer, length int) (*CompressedBlock, error) {
	stringData := (*[]string)(data)
	if len(*stringData) == 0 {
		return &CompressedBlock{Type: CompressionDictionary}, nil
	}

	// Build dictionary
	uniqueValues := make(map[string]uint32)
	var dictionary []string
	var indices []uint32

	dictIndex := uint32(0)

	for _, value := range *stringData {
		if index, exists := uniqueValues[value]; exists {
			indices = append(indices, index)
		} else {
			uniqueValues[value] = dictIndex
			dictionary = append(dictionary, value)
			indices = append(indices, dictIndex)
			dictIndex++
		}
	}

	// Serialize dictionary block
	var buf bytes.Buffer

	// Write dictionary size
	binary.Write(&buf, binary.LittleEndian, int32(len(dictionary)))

	// Write dictionary strings
	for _, str := range dictionary {
		binary.Write(&buf, binary.LittleEndian, int32(len(str)))
		buf.WriteString(str)
	}

	// Write indices
	binary.Write(&buf, binary.LittleEndian, int32(len(indices)))
	for _, index := range indices {
		binary.Write(&buf, binary.LittleEndian, index)
	}

	originalSize := 0
	for _, s := range *stringData {
		originalSize += len(s)
	}

	return &CompressedBlock{
		Type:           CompressionDictionary,
		OriginalSize:   originalSize,
		CompressedSize: buf.Len(),
		Data:          buf.Bytes(),
		Metadata:       map[string]interface{}{"dictionary_size": len(dictionary)},
	}, nil
}

func (dict *DictionaryCompressor) Decompress(block *CompressedBlock) (unsafe.Pointer, error) {
	// Dictionary decompression implementation
	return nil, fmt.Errorf("Dictionary decompression not implemented")
}

func (dict *DictionaryCompressor) EstimateRatio(data unsafe.Pointer, length int) float64 {
	// Estimate based on cardinality
	return 0.2 // Placeholder
}

func (dict *DictionaryCompressor) GetType() CompressionType {
	return CompressionDictionary
}

// Bit Packing Implementation
func (bp *BitPackingCompressor) Compress(data unsafe.Pointer, length int) (*CompressedBlock, error) {
	int32Data := (*[]int32)(data)
	if len(*int32Data) == 0 {
		return &CompressedBlock{Type: CompressionBitPacking}, nil
	}

	// Find min/max to determine bit width needed
	minVal, maxVal := (*int32Data)[0], (*int32Data)[0]
	for _, val := range *int32Data {
		if val < minVal {
			minVal = val
		}
		if val > maxVal {
			maxVal = val
		}
	}

	// Calculate bits needed
	valueRange := maxVal - minVal
	bitsNeeded := 32
	if valueRange >= 0 {
		bitsNeeded = int(math.Ceil(math.Log2(float64(valueRange + 1))))
		if bitsNeeded < 1 {
			bitsNeeded = 1
		}
	}

	// Pack bits
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, minVal)       // base value
	binary.Write(&buf, binary.LittleEndian, int32(bitsNeeded)) // bits per value
	binary.Write(&buf, binary.LittleEndian, int32(len(*int32Data))) // count

	// Simple bit packing implementation
	currentByte := uint8(0)
	currentBits := 0

	for _, val := range *int32Data {
		packedVal := uint32(val - minVal)

		// Pack bits (simplified implementation)
		for bitPos := 0; bitPos < bitsNeeded; bitPos++ {
			if (packedVal>>bitPos)&1 == 1 {
				currentByte |= 1 << currentBits
			}
			currentBits++

			if currentBits == 8 {
				buf.WriteByte(currentByte)
				currentByte = 0
				currentBits = 0
			}
		}
	}

	// Write remaining bits
	if currentBits > 0 {
		buf.WriteByte(currentByte)
	}

	return &CompressedBlock{
		Type:           CompressionBitPacking,
		OriginalSize:   len(*int32Data) * 4,
		CompressedSize: buf.Len(),
		Data:          buf.Bytes(),
		Metadata:       map[string]interface{}{"bits_per_value": bitsNeeded, "base_value": minVal},
	}, nil
}

func (bp *BitPackingCompressor) Decompress(block *CompressedBlock) (unsafe.Pointer, error) {
	return nil, fmt.Errorf("Bit packing decompression not implemented")
}

func (bp *BitPackingCompressor) EstimateRatio(data unsafe.Pointer, length int) float64 {
	return 0.4 // Placeholder
}

func (bp *BitPackingCompressor) GetType() CompressionType {
	return CompressionBitPacking
}

// GZIP Compression Implementation
func (gz *GZIPCompressor) Compress(data unsafe.Pointer, length int) (*CompressedBlock, error) {
	// Convert data to bytes for GZIP compression
	var inputBytes []byte
	var buf bytes.Buffer

	// Handle different data types
	// Try int64 first
	if int64Data := (*[]int64)(data); int64Data != nil && len(*int64Data) > 0 {
		for _, val := range *int64Data {
			binary.Write(&buf, binary.LittleEndian, val)
		}
		inputBytes = buf.Bytes()
	} else if stringData := (*[]string)(data); stringData != nil && len(*stringData) > 0 {
		for _, s := range *stringData {
			buf.WriteString(s)
			buf.WriteByte(0) // null separator
		}
		inputBytes = buf.Bytes()
	} else {
		return &CompressedBlock{Type: CompressionGZIP}, nil
	}

	// Compress with GZIP
	var compressedBuf bytes.Buffer
	gzipWriter := gzip.NewWriter(&compressedBuf)

	if _, err := gzipWriter.Write(inputBytes); err != nil {
		return nil, err
	}

	if err := gzipWriter.Close(); err != nil {
		return nil, err
	}

	return &CompressedBlock{
		Type:           CompressionGZIP,
		OriginalSize:   len(inputBytes),
		CompressedSize: compressedBuf.Len(),
		Data:          compressedBuf.Bytes(),
	}, nil
}

func (gz *GZIPCompressor) Decompress(block *CompressedBlock) (unsafe.Pointer, error) {
	reader, err := gzip.NewReader(bytes.NewReader(block.Data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	decompressed, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	// Convert back to original format (simplified)
	var result []string
	parts := bytes.Split(decompressed, []byte{0})
	for _, part := range parts[:len(parts)-1] { // Skip last empty part
		result = append(result, string(part))
	}

	return unsafe.Pointer(&result), nil
}

func (gz *GZIPCompressor) EstimateRatio(data unsafe.Pointer, length int) float64 {
	return 0.6 // Placeholder
}

func (gz *GZIPCompressor) GetType() CompressionType {
	return CompressionGZIP
}

// Helper functions
func (ce *CompressionEngine) hasSequentialPattern(col *engine.Column) bool {
	if col.Type != engine.TypeInt64 || col.Length < 2 {
		return false
	}

	data := (*[]int64)(col.Data)
	if len(*data) < 2 {
		return false
	}

	// Check if values are sequential or have consistent deltas
	delta := (*data)[1] - (*data)[0]
	for i := 2; i < len(*data); i++ {
		if (*data)[i] - (*data)[i-1] != delta {
			return false
		}
	}
	return true
}

func (ce *CompressionEngine) calculateCardinality(col *engine.Column) int {
	if col.Type != engine.TypeString {
		return int(col.Length)
	}

	data := (*[]string)(col.Data)
	unique := make(map[string]bool)
	for _, value := range *data {
		unique[value] = true
	}
	return len(unique)
}

func (ce *CompressionEngine) updateStats(algType CompressionType, block *CompressedBlock) {
	ce.stats.mu.Lock()
	defer ce.stats.mu.Unlock()

	// Update overall stats
	ce.stats.TotalOriginalBytes += int64(block.OriginalSize)
	ce.stats.TotalCompressedBytes += int64(block.CompressedSize)

	if ce.stats.TotalOriginalBytes > 0 {
		ce.stats.CompressionRatio = float64(ce.stats.TotalCompressedBytes) / float64(ce.stats.TotalOriginalBytes)
	}

	// Update algorithm-specific stats
	if _, exists := ce.stats.AlgorithmStats[algType]; !exists {
		ce.stats.AlgorithmStats[algType] = &AlgorithmStats{}
	}

	algStats := ce.stats.AlgorithmStats[algType]
	algStats.UsageCount++
	algStats.BytesCompressed += int64(block.OriginalSize)

	if algStats.BytesCompressed > 0 {
		totalCompressed := float64(0)
		for _, stat := range ce.stats.AlgorithmStats {
			if stat == algStats {
				continue
			}
			// Simplified calculation
		}
		totalCompressed += float64(block.CompressedSize)
		algStats.CompressionRatio = totalCompressed / float64(algStats.BytesCompressed)
	}
}

// Variable-length integer encoding
func writeVarInt(buf *bytes.Buffer, value int64) error {
	uValue := uint64(value)
	if value < 0 {
		uValue = uint64(-value)<<1 | 1
	} else {
		uValue = uint64(value) << 1
	}

	for uValue >= 0x80 {
		buf.WriteByte(byte(uValue) | 0x80)
		uValue >>= 7
	}
	buf.WriteByte(byte(uValue))
	return nil
}

// Get compression statistics
func (ce *CompressionEngine) GetStats() *CompressionStats {
	ce.stats.mu.RLock()
	defer ce.stats.mu.RUnlock()

	// Return a copy of the stats
	statsCopy := &CompressionStats{
		TotalOriginalBytes:   ce.stats.TotalOriginalBytes,
		TotalCompressedBytes: ce.stats.TotalCompressedBytes,
		CompressionRatio:     ce.stats.CompressionRatio,
		AlgorithmStats:       make(map[CompressionType]*AlgorithmStats),
	}

	for algType, stats := range ce.stats.AlgorithmStats {
		statsCopy.AlgorithmStats[algType] = &AlgorithmStats{
			UsageCount:       stats.UsageCount,
			BytesCompressed:  stats.BytesCompressed,
			CompressionRatio: stats.CompressionRatio,
		}
	}

	return statsCopy
}

// Note: Integration with Column moved to avoid circular dependencies.
// Use CompressionEngine.CompressColumn() directly instead.

// Enhanced column with compression support
type CompressedColumn struct {
	*engine.Column
	CompressedBlock *CompressedBlock
	IsCompressed    bool
	CompressionType CompressionType
}

// Create compressed column
func NewCompressedColumn(name string, dataType engine.DataType, capacity uint64) *CompressedColumn {
	baseCol := engine.NewColumn(name, dataType, capacity)
	return &CompressedColumn{
		Column:       baseCol,
		IsCompressed: false,
	}
}

// Compress the column data
func (cc *CompressedColumn) CompressData() error {
	if cc.IsCompressed {
		return nil // Already compressed
	}

	engine := NewCompressionEngine()
	block, err := engine.CompressColumn(cc.Column)
	if err != nil {
		return err
	}

	cc.CompressedBlock = block
	cc.IsCompressed = true
	cc.CompressionType = block.Type

	return nil
}

// Get compression info
func (cc *CompressedColumn) GetCompressionInfo() map[string]interface{} {
	if !cc.IsCompressed || cc.CompressedBlock == nil {
		return map[string]interface{}{
			"compressed": false,
		}
	}

	ratio := float64(cc.CompressedBlock.CompressedSize) / float64(cc.CompressedBlock.OriginalSize)

	return map[string]interface{}{
		"compressed":        true,
		"compression_type":  cc.CompressionType,
		"original_size":     cc.CompressedBlock.OriginalSize,
		"compressed_size":   cc.CompressedBlock.CompressedSize,
		"compression_ratio": ratio,
		"space_saved":       1.0 - ratio,
	}
}