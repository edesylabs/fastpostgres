package simd

func ScanInt64AVX512(data []int64, target int64, result []bool) int {
	if !HasAVX512() {
		return scanInt64Scalar(data, target, result)
	}
	return scanInt64AVX512(data, target, result)
}

func ScanInt64RangeAVX512(data []int64, min, max int64, result []bool) int {
	if !HasAVX512() {
		return scanInt64RangeScalar(data, min, max, result)
	}
	return scanInt64RangeAVX512(data, min, max, result)
}

func scanInt64Scalar(data []int64, target int64, result []bool) int {
	count := 0
	for i := range data {
		match := data[i] == target
		result[i] = match
		if match {
			count++
		}
	}
	return count
}

func scanInt64RangeScalar(data []int64, min, max int64, result []bool) int {
	count := 0
	for i := range data {
		match := data[i] >= min && data[i] <= max
		result[i] = match
		if match {
			count++
		}
	}
	return count
}