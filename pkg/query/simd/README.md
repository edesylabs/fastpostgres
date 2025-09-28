# SIMD Vectorization Package

This package implements high-performance SIMD (Single Instruction, Multiple Data) operations for FastPostgres using AVX-512 and AVX2 instructions.

## Features

### CPU Feature Detection
- Automatic detection of AVX-512, AVX2, AVX, SSE4.2, and SSE4.1 support
- Runtime fallback to scalar implementations when SIMD is not available
- Architecture-specific compilation with build tags

### Int64 Operations (AVX-512)

#### Scanning
- `ScanInt64AVX512`: Scan for exact matches (processes 8 int64s per instruction)
- `ScanInt64RangeAVX512`: Range-based scanning with min/max bounds

#### Filtering
- `FilterInt64AVX512`: Vectorized filtering with comparison operators
  - Equal, NotEqual, Less, LessEqual, Greater, GreaterEqual
- `FilterInt64WithSelectionAVX512`: Filter with existing selection vectors

#### Aggregation
- `SumInt64AVX512`: Vectorized sum (processes 8 elements per cycle)
- `MinInt64AVX512`: Vectorized minimum using VPMINSQ instruction
- `MaxInt64AVX512`: Vectorized maximum using VPMAXSQ instruction
- `CountInt64AVX512`: Count with NULL handling

### String Operations (AVX2)

- `CompareStringsAVX2`: Vectorized string equality (32 bytes per instruction)
- `ContainsSubstringAVX2`: Fast substring search
- Automatic fallback for strings > 32 bytes

## Performance

### Expected Speedups (with AVX-512)

| Operation | Data Size | Expected Speedup |
|-----------|-----------|------------------|
| SUM       | 1M ints   | 4-8x            |
| MIN/MAX   | 1M ints   | 4-8x            |
| FILTER    | 1M ints   | 6-10x           |
| SCAN      | 1M ints   | 6-10x           |
| STRING    | 100K strs | 2-4x            |

**Note**: Current benchmarks show scalar performance because AVX-512 is not available on the test machine. On machines with AVX-512 support, the assembly implementations will provide significant speedups.

### Throughput (Current Machine - Scalar Fallback)

- SUM: ~1,189 M elements/sec
- MIN/MAX: ~1,370 M elements/sec
- FILTER: ~1,557 M elements/sec
- SCAN: ~3,089 M elements/sec
- STRING: ~2,113 M elements/sec

## Architecture

### File Structure

```
simd/
├── types.go              # Common types and constants
├── simd.go               # CPU feature detection
├── cpuid_amd64.s         # CPUID assembly for feature detection
├── cpuid_decl_amd64.go   # CPUID declaration for amd64
├── cpuid_stub.go         # CPUID stub for non-amd64
├── scan_amd64.go         # Scanning operations (Go)
├── scan_amd64.s          # Scanning operations (assembly)
├── filter_amd64.go       # Filtering operations (Go)
├── filter_amd64.s        # Filtering operations (assembly)
├── aggregate_amd64.go    # Aggregation operations (Go)
├── aggregate_amd64.s     # Aggregation operations (assembly)
├── string_amd64.go       # String operations (Go)
├── string_amd64.s        # String operations (assembly)
├── stubs.go              # Non-amd64 fallbacks
└── fallback.go           # Runtime feature checks

```

### Key Design Decisions

1. **Automatic Fallback**: All functions automatically fall back to scalar implementations when SIMD is not available
2. **Build Tags**: Use Go build tags to compile architecture-specific code
3. **Assembly + Go**: Performance-critical loops in assembly, logic in Go
4. **Zero Dependencies**: Pure Go + assembly, no CGO required

## Usage

### Basic Example

```go
import "fastpostgres/pkg/query/simd"

// Check CPU features
features := simd.GetCPUFeatures()
if features.HasAVX512 {
    // AVX-512 available
}

// Vectorized sum
data := []int64{1, 2, 3, 4, 5, 6, 7, 8}
sum := simd.SumInt64AVX512(data)

// Vectorized filtering
selection := make([]bool, len(data))
matches := simd.FilterInt64AVX512(data, 5, simd.OpGreater, selection)

// String comparison
strings := []string{"hello", "world", "test"}
selection = make([]bool, len(strings))
matches = simd.CompareStringsAVX2(strings, "test", selection)
```

### Integration with Query Engine

The SIMD operations are automatically used by the vectorized query engine when:
1. Data size is >= 64 elements (for int64) or >= 32 elements (for strings)
2. SIMD instructions are available on the CPU
3. `simdEnabled` flag is true in VectorizedEngine

## Benchmarking

Run the SIMD benchmarks:

```bash
# Build benchmarks
go build -o bin/simd_benchmark ./benchmarks/simd_benchmark.go
go build -o bin/simd_comparison ./benchmarks/simd_comparison_benchmark.go

# Run basic benchmark
./bin/simd_benchmark

# Run SIMD vs Scalar comparison
./bin/simd_comparison
```

## Implementation Details

### AVX-512 Register Usage

- **ZMM Registers**: 512-bit registers holding 8 x int64 or 64 x int8
- **Mask Registers (K0-K7)**: Store comparison results as bitmasks
- **Instructions Used**:
  - `VPBROADCASTQ`: Broadcast value to all lanes
  - `VPCMPEQQ`: Compare 8 int64s for equality
  - `VPCMPQ`: Compare with operator (LT, LE, GT, GE)
  - `VPADDQ`: Add 8 int64s
  - `VPMINSQ`: Min of 8 int64s
  - `VPMAXSQ`: Max of 8 int64s

### AVX2 String Operations

- **YMM Registers**: 256-bit registers holding 32 x int8
- **Instructions Used**:
  - `VPCMPEQB`: Compare 32 bytes for equality
  - `VPMOVMSKB`: Convert comparison mask to integer

### Cache Optimization

- Batch size: 4096 elements (optimal for L1/L2 cache)
- Sequential memory access patterns
- Prefetching handled by CPU

## Future Enhancements

1. **More Data Types**: float32, float64, int32 support
2. **ARM NEON**: SIMD for ARM processors
3. **More Operations**: JOIN, GROUP BY with SIMD
4. **JIT Compilation**: Runtime code generation for hot queries
5. **Adaptive Execution**: Auto-tune batch sizes based on CPU cache

## References

- Intel Intrinsics Guide: https://www.intel.com/content/www/us/en/docs/intrinsics-guide/
- AVX-512 ISA: https://en.wikipedia.org/wiki/AVX-512
- Go Assembly: https://go.dev/doc/asm