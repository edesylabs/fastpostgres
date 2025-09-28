package main

import (
	"fmt"
	"math/rand"
	"time"

	"fastpostgres/pkg/query/simd"
)

func main() {
	fmt.Println("=== FastPostgres SIMD Benchmark ===\n")

	features := simd.GetCPUFeatures()
	fmt.Printf("CPU Features:\n")
	fmt.Printf("  AVX-512: %v\n", features.HasAVX512)
	fmt.Printf("  AVX2:    %v\n", features.HasAVX2)
	fmt.Printf("  AVX:     %v\n", features.HasAVX)
	fmt.Printf("  SSE4.2:  %v\n", features.HasSSE42)
	fmt.Printf("  SSE4.1:  %v\n\n", features.HasSSE41)

	benchmarkSumOperations()
	benchmarkMinMaxOperations()
	benchmarkFilterOperations()
	benchmarkScanOperations()
	benchmarkStringOperations()
}

func benchmarkSumOperations() {
	fmt.Println("=== SUM Benchmark ===")

	sizes := []int{1000, 10000, 100000, 1000000}

	for _, size := range sizes {
		data := make([]int64, size)
		for i := range data {
			data[i] = rand.Int63n(1000)
		}

		start := time.Now()
		result := simd.SumInt64AVX512(data)
		duration := time.Since(start)

		throughput := float64(size) / duration.Seconds() / 1e6

		fmt.Printf("  Size: %7d | Result: %12d | Time: %8s | Throughput: %.2f M/s\n",
			size, result, duration, throughput)
	}
	fmt.Println()
}

func benchmarkMinMaxOperations() {
	fmt.Println("=== MIN/MAX Benchmark ===")

	sizes := []int{1000, 10000, 100000, 1000000}

	for _, size := range sizes {
		data := make([]int64, size)
		for i := range data {
			data[i] = rand.Int63n(10000)
		}

		start := time.Now()
		min := simd.MinInt64AVX512(data)
		minDuration := time.Since(start)

		start = time.Now()
		max := simd.MaxInt64AVX512(data)
		maxDuration := time.Since(start)

		minThroughput := float64(size) / minDuration.Seconds() / 1e6
		maxThroughput := float64(size) / maxDuration.Seconds() / 1e6

		fmt.Printf("  Size: %7d | Min: %5d (%8s, %.2f M/s) | Max: %5d (%8s, %.2f M/s)\n",
			size, min, minDuration, minThroughput, max, maxDuration, maxThroughput)
	}
	fmt.Println()
}

func benchmarkFilterOperations() {
	fmt.Println("=== FILTER Benchmark ===")

	sizes := []int{1000, 10000, 100000, 1000000}
	target := int64(500)

	for _, size := range sizes {
		data := make([]int64, size)
		selection := make([]bool, size)
		for i := range data {
			data[i] = rand.Int63n(1000)
		}

		start := time.Now()
		matches := simd.FilterInt64AVX512(data, target, simd.OpEqual, selection)
		duration := time.Since(start)

		throughput := float64(size) / duration.Seconds() / 1e6
		selectivity := float64(matches) / float64(size) * 100

		fmt.Printf("  Size: %7d | Matches: %6d (%.1f%%) | Time: %8s | Throughput: %.2f M/s\n",
			size, matches, selectivity, duration, throughput)
	}
	fmt.Println()
}

func benchmarkScanOperations() {
	fmt.Println("=== SCAN Benchmark ===")

	sizes := []int{1000, 10000, 100000, 1000000}
	target := int64(500)

	for _, size := range sizes {
		data := make([]int64, size)
		result := make([]bool, size)
		for i := range data {
			data[i] = rand.Int63n(1000)
		}

		start := time.Now()
		matches := simd.ScanInt64AVX512(data, target, result)
		duration := time.Since(start)

		throughput := float64(size) / duration.Seconds() / 1e6
		selectivity := float64(matches) / float64(size) * 100

		fmt.Printf("  Size: %7d | Matches: %6d (%.1f%%) | Time: %8s | Throughput: %.2f M/s\n",
			size, matches, selectivity, duration, throughput)
	}
	fmt.Println()
}

func benchmarkStringOperations() {
	fmt.Println("=== STRING Comparison Benchmark ===")

	sizes := []int{1000, 10000, 100000}
	target := "test_string_123"

	for _, size := range sizes {
		data := make([]string, size)
		selection := make([]bool, size)
		for i := range data {
			if rand.Float64() < 0.01 {
				data[i] = target
			} else {
				data[i] = fmt.Sprintf("string_%d", rand.Intn(1000))
			}
		}

		start := time.Now()
		matches := simd.CompareStringsAVX2(data, target, selection)
		duration := time.Since(start)

		throughput := float64(size) / duration.Seconds() / 1e6
		selectivity := float64(matches) / float64(size) * 100

		fmt.Printf("  Size: %7d | Matches: %6d (%.1f%%) | Time: %8s | Throughput: %.2f M/s\n",
			size, matches, selectivity, duration, throughput)
	}
	fmt.Println()
}