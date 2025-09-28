//go:build !amd64
// +build !amd64

package simd

func cpuidDetect() CPUFeatures {
	return CPUFeatures{}
}