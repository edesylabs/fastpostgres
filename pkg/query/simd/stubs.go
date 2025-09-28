//go:build !amd64
// +build !amd64

package simd

func ScanInt64AVX512(data []int64, target int64, result []bool) int {
	return scanInt64Scalar(data, target, result)
}

func ScanInt64RangeAVX512(data []int64, min, max int64, result []bool) int {
	return scanInt64RangeScalar(data, min, max, result)
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

func FilterInt64AVX512(data []int64, target int64, op CompareOp, selection []bool) int {
	return filterInt64Scalar(data, target, op, selection)
}

func FilterInt64WithSelectionAVX512(data []int64, target int64, op CompareOp, selectionIn, selectionOut []bool) int {
	return filterInt64WithSelectionScalar(data, target, op, selectionIn, selectionOut)
}

func filterInt64Scalar(data []int64, target int64, op CompareOp, selection []bool) int {
	count := 0
	for i := range data {
		var match bool
		switch op {
		case OpEqual:
			match = data[i] == target
		case OpNotEqual:
			match = data[i] != target
		case OpLess:
			match = data[i] < target
		case OpLessEqual:
			match = data[i] <= target
		case OpGreater:
			match = data[i] > target
		case OpGreaterEqual:
			match = data[i] >= target
		}
		selection[i] = match
		if match {
			count++
		}
	}
	return count
}

func filterInt64WithSelectionScalar(data []int64, target int64, op CompareOp, selectionIn, selectionOut []bool) int {
	count := 0
	for i := range data {
		if !selectionIn[i] {
			selectionOut[i] = false
			continue
		}

		var match bool
		switch op {
		case OpEqual:
			match = data[i] == target
		case OpNotEqual:
			match = data[i] != target
		case OpLess:
			match = data[i] < target
		case OpLessEqual:
			match = data[i] <= target
		case OpGreater:
			match = data[i] > target
		case OpGreaterEqual:
			match = data[i] >= target
		}
		selectionOut[i] = match
		if match {
			count++
		}
	}
	return count
}

func SumInt64AVX512(data []int64) int64 {
	return sumInt64Scalar(data)
}

func MinInt64AVX512(data []int64) int64 {
	if len(data) == 0 {
		return 0
	}
	return minInt64Scalar(data)
}

func MaxInt64AVX512(data []int64) int64 {
	if len(data) == 0 {
		return 0
	}
	return maxInt64Scalar(data)
}

func CountInt64AVX512(data []int64, nulls []bool) int64 {
	return countInt64Scalar(data, nulls)
}

func sumInt64Scalar(data []int64) int64 {
	var sum int64
	for _, v := range data {
		sum += v
	}
	return sum
}

func minInt64Scalar(data []int64) int64 {
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

func maxInt64Scalar(data []int64) int64 {
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

func countInt64Scalar(data []int64, nulls []bool) int64 {
	count := int64(len(data))
	if nulls != nil {
		count = 0
		for _, isNull := range nulls {
			if !isNull {
				count++
			}
		}
	}
	return count
}

func CompareStringsAVX2(data []string, target string, selection []bool) int {
	return compareStringsScalar(data, target, selection)
}

func compareStringsScalar(data []string, target string, selection []bool) int {
	count := 0
	for i, s := range data {
		match := s == target
		selection[i] = match
		if match {
			count++
		}
	}
	return count
}