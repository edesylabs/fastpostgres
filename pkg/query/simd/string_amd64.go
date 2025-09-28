package simd

func CompareStringsAVX2(data []string, target string, selection []bool) int {
	if !HasAVX2() {
		return compareStringsScalar(data, target, selection)
	}

	count := 0
	targetBytes := []byte(target)
	targetLen := len(targetBytes)

	if targetLen <= 32 {
		count = compareStringsFixedAVX2(data, targetBytes, selection)
	} else {
		count = compareStringsScalar(data, target, selection)
	}

	return count
}

func ContainsSubstringAVX2(data []string, substring string, selection []bool) int {
	if !HasAVX2() {
		return containsSubstringScalar(data, substring, selection)
	}

	if len(substring) == 0 {
		for i := range data {
			selection[i] = true
		}
		return len(data)
	}

	count := 0
	firstByte := substring[0]

	for i, s := range data {
		match := false
		if len(s) >= len(substring) {
			match = containsScalar(s, substring, firstByte)
		}
		selection[i] = match
		if match {
			count++
		}
	}

	return count
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

func containsSubstringScalar(data []string, substring string, selection []bool) int {
	count := 0
	for i, s := range data {
		match := false
		for j := 0; j <= len(s)-len(substring); j++ {
			if s[j:j+len(substring)] == substring {
				match = true
				break
			}
		}
		selection[i] = match
		if match {
			count++
		}
	}
	return count
}

func containsScalar(haystack, needle string, firstByte byte) bool {
	for i := 0; i <= len(haystack)-len(needle); i++ {
		if haystack[i] == firstByte {
			match := true
			for j := 1; j < len(needle); j++ {
				if haystack[i+j] != needle[j] {
					match = false
					break
				}
			}
			if match {
				return true
			}
		}
	}
	return false
}

func compareStringsFixedAVX2(data []string, targetBytes []byte, selection []bool) int {
	count := 0
	for i, s := range data {
		match := false
		if len(s) == len(targetBytes) {
			match = equalBytesAVX2([]byte(s), targetBytes)
		}
		selection[i] = match
		if match {
			count++
		}
	}
	return count
}

func equalBytesAVX2(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}

	if !HasAVX2() {
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}

	if len(a) <= 32 {
		return compareBytes32AVX2(a, b)
	}

	return compareBytesLongAVX2(a, b)
}