package simd

func FilterInt64AVX512(data []int64, target int64, op CompareOp, selection []bool) int {
	if !HasAVX512() {
		return filterInt64Scalar(data, target, op, selection)
	}

	switch op {
	case OpEqual:
		return filterInt64EqualAVX512(data, target, selection)
	case OpNotEqual:
		return filterInt64NotEqualAVX512(data, target, selection)
	case OpLess:
		return filterInt64LessAVX512(data, target, selection)
	case OpLessEqual:
		return filterInt64LessEqualAVX512(data, target, selection)
	case OpGreater:
		return filterInt64GreaterAVX512(data, target, selection)
	case OpGreaterEqual:
		return filterInt64GreaterEqualAVX512(data, target, selection)
	default:
		return filterInt64Scalar(data, target, op, selection)
	}
}

func FilterInt64WithSelectionAVX512(data []int64, target int64, op CompareOp, selectionIn, selectionOut []bool) int {
	if !HasAVX512() {
		return filterInt64WithSelectionScalar(data, target, op, selectionIn, selectionOut)
	}
	return filterInt64WithSelectionAVX512(data, target, op, selectionIn, selectionOut)
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