package simd

type CompareOp uint8

const (
	OpEqual CompareOp = iota
	OpNotEqual
	OpLess
	OpLessEqual
	OpGreater
	OpGreaterEqual
)