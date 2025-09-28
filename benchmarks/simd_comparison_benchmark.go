package main

import (
	"fmt"
	"math/rand"
	"time"

	"fastpostgres/pkg/query/simd"
)

func main() {
	fmt.Println("=== SIMD vs Scalar Performance Comparison ===\n")

	benchmarkSumComparison()
	benchmarkFilterComparison()
	benchmarkAggregateComparison()
}

func benchmarkSumComparison() {
	fmt.Println("=== SUM: SIMD vs Scalar ===")
	fmt.Printf("%-10s | %-15s | %-15s | %-10s\n", "Size", "SIMD (AVX-512)", "Scalar", "Speedup")
	fmt.Println("---------------------------------------------------------------")

	sizes := []int{1000, 10000, 100000, 1000000, 10000000}

	for _, size := range sizes {
		data := make([]int64, size)
		for i := range data {
			data[i] = rand.Int63n(1000)
		}

		start := time.Now()
		simdResult := simd.SumInt64AVX512(data)
		simdDuration := time.Since(start)

		start = time.Now()
		scalarResult := sumScalar(data)
		scalarDuration := time.Since(start)

		speedup := float64(scalarDuration) / float64(simdDuration)

		if simdResult != scalarResult {
			fmt.Printf("ERROR: Results don't match! SIMD: %d, Scalar: %d\n", simdResult, scalarResult)
		}

		fmt.Printf("%-10d | %-15s | %-15s | %.2fx\n",
			size, simdDuration, scalarDuration, speedup)
	}
	fmt.Println()
}

func benchmarkFilterComparison() {
	fmt.Println("=== FILTER: SIMD vs Scalar ===")
	fmt.Printf("%-10s | %-15s | %-15s | %-10s\n", "Size", "SIMD (AVX-512)", "Scalar", "Speedup")
	fmt.Println("---------------------------------------------------------------")

	sizes := []int{1000, 10000, 100000, 1000000}
	target := int64(500)

	for _, size := range sizes {
		data := make([]int64, size)
		simdSelection := make([]bool, size)
		scalarSelection := make([]bool, size)

		for i := range data {
			data[i] = rand.Int63n(1000)
		}

		start := time.Now()
		simdMatches := simd.FilterInt64AVX512(data, target, simd.OpEqual, simdSelection)
		simdDuration := time.Since(start)

		start = time.Now()
		scalarMatches := filterScalar(data, target, scalarSelection)
		scalarDuration := time.Since(start)

		speedup := float64(scalarDuration) / float64(simdDuration)

		if simdMatches != scalarMatches {
			fmt.Printf("ERROR: Match counts don't match! SIMD: %d, Scalar: %d\n", simdMatches, scalarMatches)
		}

		fmt.Printf("%-10d | %-15s | %-15s | %.2fx\n",
			size, simdDuration, scalarDuration, speedup)
	}
	fmt.Println()
}

func benchmarkAggregateComparison() {
	fmt.Println("=== MIN/MAX: SIMD vs Scalar ===")
	fmt.Printf("%-10s | %-8s | %-15s | %-15s | %-10s\n", "Size", "Op", "SIMD (AVX-512)", "Scalar", "Speedup")
	fmt.Println("--------------------------------------------------------------------------------")

	sizes := []int{1000, 10000, 100000, 1000000}

	for _, size := range sizes {
		data := make([]int64, size)
		for i := range data {
			data[i] = rand.Int63n(10000)
		}

		start := time.Now()
		simdMin := simd.MinInt64AVX512(data)
		simdMinDuration := time.Since(start)

		start = time.Now()
		scalarMin := minScalar(data)
		scalarMinDuration := time.Since(start)

		minSpeedup := float64(scalarMinDuration) / float64(simdMinDuration)

		if simdMin != scalarMin {
			fmt.Printf("ERROR: Min values don't match! SIMD: %d, Scalar: %d\n", simdMin, scalarMin)
		}

		start = time.Now()
		simdMax := simd.MaxInt64AVX512(data)
		simdMaxDuration := time.Since(start)

		start = time.Now()
		scalarMax := maxScalar(data)
		scalarMaxDuration := time.Since(start)

		maxSpeedup := float64(scalarMaxDuration) / float64(simdMaxDuration)

		if simdMax != scalarMax {
			fmt.Printf("ERROR: Max values don't match! SIMD: %d, Scalar: %d\n", simdMax, scalarMax)
		}

		fmt.Printf("%-10d | %-8s | %-15s | %-15s | %.2fx\n",
			size, "MIN", simdMinDuration, scalarMinDuration, minSpeedup)
		fmt.Printf("%-10d | %-8s | %-15s | %-15s | %.2fx\n",
			size, "MAX", simdMaxDuration, scalarMaxDuration, maxSpeedup)
	}
	fmt.Println()
}

func sumScalar(data []int64) int64 {
	var sum int64
	for _, v := range data {
		sum += v
	}
	return sum
}

func filterScalar(data []int64, target int64, selection []bool) int {
	count := 0
	for i, v := range data {
		match := v == target
		selection[i] = match
		if match {
			count++
		}
	}
	return count
}

func minScalar(data []int64) int64 {
	if len(data) == 0 {
		return 0
	}
	min := data[0]
	for _, v := range data[1:] {
		if v < min {
			min = v
		}
	}
	return min
}

func maxScalar(data []int64) int64 {
	if len(data) == 0 {
		return 0
	}
	max := data[0]
	for _, v := range data[1:] {
		if v > max {
			max = v
		}
	}
	return max
}