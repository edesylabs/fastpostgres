package main

import (
	"fmt"
	"time"
	"unsafe"
)

func main() {
	fmt.Println("FastPostgres Columnar Compression Demo")
	fmt.Println("=====================================")

	// Test different compression algorithms
	testRLECompression()
	testDeltaCompression()
	testDictionaryCompression()
	testBitPackingCompression()
	testGZIPCompression()

	// Comprehensive compression benchmark
	fmt.Println("\n=== COMPREHENSIVE COMPRESSION BENCHMARK ===")
	benchmarkCompressionAlgorithms()
}

func testRLECompression() {
	fmt.Println("\n=== RUN-LENGTH ENCODING (RLE) TEST ===")
	fmt.Println("Testing RLE on repetitive int64 data...")

	// Create column with repetitive data (perfect for RLE)
	rleCol := NewColumn("rle_test", TypeInt64, 10000)

	// Insert repetitive data pattern
	repetitiveData := []int64{1, 1, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 4, 4, 5, 5, 5, 5}
	for _, val := range repetitiveData {
		rleCol.AppendInt64(val, false)
	}

	fmt.Printf("Original data: %v\n", repetitiveData)
	fmt.Printf("Data length: %d values (%d bytes)\n", len(repetitiveData), len(repetitiveData)*8)

	// Compress using RLE
	rleCompressor := &RLECompressor{}
	start := time.Now()
	compressedBlock, err := rleCompressor.Compress(rleCol.Data, int(rleCol.Length))
	compressionTime := time.Since(start)

	if err != nil {
		fmt.Printf("RLE compression failed: %v\n", err)
		return
	}

	ratio := float64(compressedBlock.CompressedSize) / float64(compressedBlock.OriginalSize)
	spaceSaved := (1.0 - ratio) * 100

	fmt.Printf("Compression completed in: %v\n", compressionTime)
	fmt.Printf("Original size: %d bytes\n", compressedBlock.OriginalSize)
	fmt.Printf("Compressed size: %d bytes\n", compressedBlock.CompressedSize)
	fmt.Printf("Compression ratio: %.3f\n", ratio)
	fmt.Printf("Space saved: %.1f%%\n", spaceSaved)
}

func testDeltaCompression() {
	fmt.Println("\n=== DELTA ENCODING TEST ===")
	fmt.Println("Testing Delta encoding on sequential numeric data...")

	// Create column with sequential data (perfect for Delta)
	deltaCol := NewColumn("delta_test", TypeInt64, 1000)

	// Insert sequential data with consistent increments
	sequentialData := make([]int64, 100)
	baseValue := int64(1000)
	increment := int64(5)

	for i := 0; i < 100; i++ {
		sequentialData[i] = baseValue + int64(i)*increment
		deltaCol.AppendInt64(sequentialData[i], false)
	}

	fmt.Printf("Sequential data pattern: %d, %d, %d, ... %d\n",
		sequentialData[0], sequentialData[1], sequentialData[2], sequentialData[99])
	fmt.Printf("Data length: %d values (%d bytes)\n", len(sequentialData), len(sequentialData)*8)

	// Compress using Delta encoding
	deltaCompressor := &DeltaCompressor{}
	start := time.Now()
	compressedBlock, err := deltaCompressor.Compress(deltaCol.Data, int(deltaCol.Length))
	compressionTime := time.Since(start)

	if err != nil {
		fmt.Printf("Delta compression failed: %v\n", err)
		return
	}

	ratio := float64(compressedBlock.CompressedSize) / float64(compressedBlock.OriginalSize)
	spaceSaved := (1.0 - ratio) * 100

	fmt.Printf("Compression completed in: %v\n", compressionTime)
	fmt.Printf("Original size: %d bytes\n", compressedBlock.OriginalSize)
	fmt.Printf("Compressed size: %d bytes\n", compressedBlock.CompressedSize)
	fmt.Printf("Compression ratio: %.3f\n", ratio)
	fmt.Printf("Space saved: %.1f%%\n", spaceSaved)
}

func testDictionaryCompression() {
	fmt.Println("\n=== DICTIONARY ENCODING TEST ===")
	fmt.Println("Testing Dictionary encoding on low-cardinality string data...")

	// Create column with low-cardinality string data (perfect for Dictionary)
	dictCol := NewColumn("dict_test", TypeString, 1000)

	// Insert repetitive string data
	categories := []string{"Engineering", "Sales", "Marketing", "HR", "Finance"}
	var stringData []string

	for i := 0; i < 200; i++ {
		category := categories[i%len(categories)]
		stringData = append(stringData, category)
		dictCol.AppendString(category, false)
	}

	originalSize := 0
	for _, s := range stringData {
		originalSize += len(s)
	}

	fmt.Printf("Categories: %v\n", categories)
	fmt.Printf("Data length: %d strings (%d unique values)\n", len(stringData), len(categories))
	fmt.Printf("Original size: %d bytes\n", originalSize)

	// Compress using Dictionary encoding
	dictCompressor := &DictionaryCompressor{}
	start := time.Now()
	compressedBlock, err := dictCompressor.Compress(dictCol.Data, int(dictCol.Length))
	compressionTime := time.Since(start)

	if err != nil {
		fmt.Printf("Dictionary compression failed: %v\n", err)
		return
	}

	ratio := float64(compressedBlock.CompressedSize) / float64(compressedBlock.OriginalSize)
	spaceSaved := (1.0 - ratio) * 100

	fmt.Printf("Compression completed in: %v\n", compressionTime)
	fmt.Printf("Original size: %d bytes\n", compressedBlock.OriginalSize)
	fmt.Printf("Compressed size: %d bytes\n", compressedBlock.CompressedSize)
	fmt.Printf("Compression ratio: %.3f\n", ratio)
	fmt.Printf("Space saved: %.1f%%\n", spaceSaved)
	fmt.Printf("Dictionary size: %v\n", compressedBlock.Metadata["dictionary_size"])
}

func testBitPackingCompression() {
	fmt.Println("\n=== BIT PACKING TEST ===")
	fmt.Println("Testing Bit Packing on small integer ranges...")

	// Create column with small integer values (perfect for Bit Packing)
	bitPackCol := NewColumn("bitpack_test", TypeInt32, 1000)

	// Insert data with small range (0-15, needs only 4 bits each)
	var int32Data []int32
	for i := 0; i < 100; i++ {
		value := int32(i % 16) // Values 0-15
		int32Data = append(int32Data, value)
	}

	// Manually set the data for bit packing test
	bitPackCol.Data = unsafe.Pointer(&int32Data)
	bitPackCol.Length = uint64(len(int32Data))

	fmt.Printf("Value range: 0-15 (needs 4 bits per value)\n")
	fmt.Printf("Data length: %d values (%d bytes)\n", len(int32Data), len(int32Data)*4)

	// Compress using Bit Packing
	bitPackCompressor := &BitPackingCompressor{}
	start := time.Now()
	compressedBlock, err := bitPackCompressor.Compress(bitPackCol.Data, int(bitPackCol.Length))
	compressionTime := time.Since(start)

	if err != nil {
		fmt.Printf("Bit packing compression failed: %v\n", err)
		return
	}

	ratio := float64(compressedBlock.CompressedSize) / float64(compressedBlock.OriginalSize)
	spaceSaved := (1.0 - ratio) * 100

	fmt.Printf("Compression completed in: %v\n", compressionTime)
	fmt.Printf("Original size: %d bytes\n", compressedBlock.OriginalSize)
	fmt.Printf("Compressed size: %d bytes\n", compressedBlock.CompressedSize)
	fmt.Printf("Compression ratio: %.3f\n", ratio)
	fmt.Printf("Space saved: %.1f%%\n", spaceSaved)
	fmt.Printf("Bits per value: %v\n", compressedBlock.Metadata["bits_per_value"])
	fmt.Printf("Base value: %v\n", compressedBlock.Metadata["base_value"])
}

func testGZIPCompression() {
	fmt.Println("\n=== GZIP COMPRESSION TEST ===")
	fmt.Println("Testing GZIP on general string data...")

	// Create column with varied string data
	gzipCol := NewColumn("gzip_test", TypeString, 1000)

	// Insert varied text data
	textData := []string{
		"The quick brown fox jumps over the lazy dog",
		"Lorem ipsum dolor sit amet, consectetur adipiscing elit",
		"FastPostgres is a high-performance columnar database",
		"Compression algorithms optimize storage efficiency",
		"The quick brown fox jumps over the lazy dog", // Repeat to show compression
		"Data compression reduces storage costs significantly",
		"FastPostgres is a high-performance columnar database", // Another repeat
	}

	for _, text := range textData {
		gzipCol.AppendString(text, false)
	}

	originalSize := 0
	for _, s := range textData {
		originalSize += len(s)
	}

	fmt.Printf("Text data with %d strings\n", len(textData))
	fmt.Printf("Original size: %d bytes\n", originalSize)

	// Compress using GZIP
	gzipCompressor := &GZIPCompressor{}
	start := time.Now()
	compressedBlock, err := gzipCompressor.Compress(gzipCol.Data, int(gzipCol.Length))
	compressionTime := time.Since(start)

	if err != nil {
		fmt.Printf("GZIP compression failed: %v\n", err)
		return
	}

	ratio := float64(compressedBlock.CompressedSize) / float64(compressedBlock.OriginalSize)
	spaceSaved := (1.0 - ratio) * 100

	fmt.Printf("Compression completed in: %v\n", compressionTime)
	fmt.Printf("Original size: %d bytes\n", compressedBlock.OriginalSize)
	fmt.Printf("Compressed size: %d bytes\n", compressedBlock.CompressedSize)
	fmt.Printf("Compression ratio: %.3f\n", ratio)
	fmt.Printf("Space saved: %.1f%%\n", spaceSaved)
}

func benchmarkCompressionAlgorithms() {
	fmt.Println("Creating large datasets for comprehensive benchmarking...")

	// Create large datasets for different data patterns
	datasets := []struct{
		name string
		dataType DataType
		setupFunc func() *Column
	}{
		{
			name: "Repetitive Integers (RLE-friendly)",
			dataType: TypeInt64,
			setupFunc: func() *Column {
				col := NewColumn("repetitive", TypeInt64, 100000)
				values := []int64{1, 2, 3, 4, 5}
				for i := 0; i < 20000; i++ {
					val := values[i%len(values)]
					// Repeat each value multiple times to make it RLE-friendly
					for j := 0; j < 5; j++ {
						col.AppendInt64(val, false)
					}
				}
				return col
			},
		},
		{
			name: "Sequential Integers (Delta-friendly)",
			dataType: TypeInt64,
			setupFunc: func() *Column {
				col := NewColumn("sequential", TypeInt64, 100000)
				baseValue := int64(1000000)
				for i := 0; i < 100000; i++ {
					col.AppendInt64(baseValue + int64(i)*7, false) // Increment by 7 each time
				}
				return col
			},
		},
		{
			name: "Low-cardinality Strings (Dictionary-friendly)",
			dataType: TypeString,
			setupFunc: func() *Column {
				col := NewColumn("categories", TypeString, 100000)
				categories := []string{
					"Engineering", "Sales", "Marketing", "HR", "Finance",
					"Operations", "Support", "Research", "Legal", "Admin",
				}
				for i := 0; i < 100000; i++ {
					col.AppendString(categories[i%len(categories)], false)
				}
				return col
			},
		},
	}

	// Test each dataset with appropriate compression algorithms
	for _, dataset := range datasets {
		fmt.Printf("\n--- %s ---\n", dataset.name)

		start := time.Now()
		col := dataset.setupFunc()
		setupTime := time.Since(start)

		originalSize := calculateOriginalSize(col)
		fmt.Printf("Dataset created in: %v\n", setupTime)
		fmt.Printf("Rows: %d, Original size: %d bytes\n", col.Length, originalSize)

		// Test appropriate algorithms based on data type
		var algorithms []struct{
			name string
			compressor CompressionAlgorithm
		}

		if dataset.dataType == TypeInt64 {
			algorithms = []struct{
				name string
				compressor CompressionAlgorithm
			}{
				{"RLE", &RLECompressor{}},
				{"Delta", &DeltaCompressor{}},
				{"GZIP", &GZIPCompressor{}},
			}
		} else if dataset.dataType == TypeString {
			algorithms = []struct{
				name string
				compressor CompressionAlgorithm
			}{
				{"RLE", &RLECompressor{}},
				{"Dictionary", &DictionaryCompressor{}},
				{"GZIP", &GZIPCompressor{}},
			}
		} else {
			algorithms = []struct{
				name string
				compressor CompressionAlgorithm
			}{
				{"GZIP", &GZIPCompressor{}},
			}
		}

		results := make([]compressionResult, 0)

		for _, alg := range algorithms {
			start := time.Now()
			block, err := alg.compressor.Compress(col.Data, int(col.Length))
			compressionTime := time.Since(start)

			if err != nil {
				fmt.Printf("  %s: FAILED (%v)\n", alg.name, err)
				continue
			}

			ratio := float64(block.CompressedSize) / float64(block.OriginalSize)
			spaceSaved := (1.0 - ratio) * 100
			throughput := float64(originalSize) / compressionTime.Seconds() / (1024*1024) // MB/s

			result := compressionResult{
				algorithm: alg.name,
				ratio: ratio,
				spaceSaved: spaceSaved,
				time: compressionTime,
				throughput: throughput,
				originalSize: block.OriginalSize,
				compressedSize: block.CompressedSize,
			}
			results = append(results, result)

			fmt.Printf("  %s: %.3f ratio, %.1f%% saved, %v, %.1f MB/s\n",
				alg.name, ratio, spaceSaved, compressionTime, throughput)
		}

		// Find best algorithm for this dataset
		bestAlg := findBestCompression(results)
		if bestAlg.algorithm != "" {
			fmt.Printf("  BEST: %s (%.1f%% space saved)\n", bestAlg.algorithm, bestAlg.spaceSaved)
		}
	}

	fmt.Println("\n=== COMPRESSION SUMMARY ===")
	fmt.Println("RLE: Best for repetitive data (up to 80%+ compression)")
	fmt.Println("Delta: Best for sequential numeric data (up to 90%+ compression)")
	fmt.Println("Dictionary: Best for low-cardinality strings (up to 95%+ compression)")
	fmt.Println("GZIP: General-purpose, works on all data types")
	fmt.Println("Bit Packing: Best for small integer ranges")
}

type compressionResult struct {
	algorithm string
	ratio float64
	spaceSaved float64
	time time.Duration
	throughput float64
	originalSize int
	compressedSize int
}

func findBestCompression(results []compressionResult) compressionResult {
	if len(results) == 0 {
		return compressionResult{}
	}

	best := results[0]
	for _, result := range results[1:] {
		// Prioritize space savings with reasonable performance
		if result.spaceSaved > best.spaceSaved {
			best = result
		}
	}
	return best
}

func calculateOriginalSize(col *Column) int {
	switch col.Type {
	case TypeInt64:
		return int(col.Length * 8)
	case TypeInt32:
		return int(col.Length * 4)
	case TypeString:
		data := (*[]string)(col.Data)
		total := 0
		for _, s := range *data {
			total += len(s)
		}
		return total
	default:
		return int(col.Length * 8) // Default estimate
	}
}