package simd

func SumInt64AVX512(data []int64) int64 {
	if !HasAVX512() {
		return sumInt64Scalar(data)
	}
	return sumInt64AVX512(data)
}

func MinInt64AVX512(data []int64) int64 {
	if len(data) == 0 {
		return 0
	}
	if !HasAVX512() {
		return minInt64Scalar(data)
	}
	return minInt64AVX512(data)
}

func MaxInt64AVX512(data []int64) int64 {
	if len(data) == 0 {
		return 0
	}
	if !HasAVX512() {
		return maxInt64Scalar(data)
	}
	return maxInt64AVX512(data)
}

func CountInt64AVX512(data []int64, nulls []bool) int64 {
	if !HasAVX512() {
		return countInt64Scalar(data, nulls)
	}
	return countInt64WithNullsAVX512(data, nulls)
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