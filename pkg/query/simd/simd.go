package simd

import (
	"runtime"
)

type CPUFeatures struct {
	HasAVX512 bool
	HasAVX2   bool
	HasAVX    bool
	HasSSE42  bool
	HasSSE41  bool
}

var cpuFeatures CPUFeatures

func init() {
	cpuFeatures = detectCPUFeatures()
}

func detectCPUFeatures() CPUFeatures {
	if runtime.GOARCH == "amd64" {
		return cpuidDetect()
	}
	return CPUFeatures{}
}

func GetCPUFeatures() CPUFeatures {
	return cpuFeatures
}

func HasAVX512() bool {
	return cpuFeatures.HasAVX512
}

func HasAVX2() bool {
	return cpuFeatures.HasAVX2
}

func HasAVX() bool {
	return cpuFeatures.HasAVX
}