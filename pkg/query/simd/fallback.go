package simd

import "runtime"

func init() {
	printCPUFeatures()
}

func printCPUFeatures() {
	if runtime.GOARCH != "amd64" {
		return
	}

	features := GetCPUFeatures()

	if features.HasAVX512 {
	} else if features.HasAVX2 {
	} else if features.HasAVX {
	} else if features.HasSSE42 {
	} else {
	}
}

func GetOptimalBatchSize() int {
	features := GetCPUFeatures()

	if features.HasAVX512 {
		return 8192
	} else if features.HasAVX2 {
		return 4096
	} else if features.HasAVX {
		return 2048
	}

	return 1024
}

func GetVectorWidth() int {
	features := GetCPUFeatures()

	if features.HasAVX512 {
		return 8
	} else if features.HasAVX2 {
		return 4
	} else if features.HasAVX {
		return 4
	}

	return 2
}

func ShouldUseSIMD(dataSize int) bool {
	if runtime.GOARCH != "amd64" {
		return false
	}

	features := GetCPUFeatures()
	if !features.HasAVX2 && !features.HasAVX512 {
		return false
	}

	minSize := 32
	if features.HasAVX512 {
		minSize = 64
	}

	return dataSize >= minSize
}