# FastPostgres

A high-performance, in-memory columnar database written in Go with PostgreSQL wire protocol compatibility.

## Features

- **Hybrid OLTP/OLAP Architecture** - Handles both transactional and analytical workloads
- **Columnar Storage** - Optimized for analytics with vectorized execution
- **PostgreSQL Compatible** - Works with standard PostgreSQL clients
- **Massive Concurrency** - 150K+ concurrent connections
- **Lock-Free Design** - No contention bottlenecks
- **Advanced Indexing** - Hash, B-Tree, Bitmap, and Bloom filters
- **Query Caching** - Built-in result caching for performance

## Performance Highlights

- **2.11M rows/sec** data ingestion (71x faster than PostgreSQL)
- **23K queries/sec** concurrent query performance
- **150K+ concurrent connections** (vs PostgreSQL's ~500 max)
- **468K requests/sec** throughput
- **0.55KB memory per connection** (18,000x less than PostgreSQL)

See [BENCHMARK_RESULTS.md](docs/BENCHMARK_RESULTS.md) for detailed performance metrics.

## Quick Start

### Installation

```bash
go build -o fastpostgres ./cmd/fastpostgres/
```

### Running the Server

```bash
# Start server on default port 5432
./fastpostgres server

# Start server on custom port
./fastpostgres server 5433
```

### Run Demo

```bash
./fastpostgres demo
```

### Run Benchmarks

```bash
./fastpostgres benchmark
```

## Project Structure

```
fastpostgres/
├── cmd/
│   └── fastpostgres/          # Main application
├── pkg/
│   ├── engine/                # Core database engine
│   │   ├── core.go           # Data types and structures
│   │   ├── client.go         # Client connection handling
│   │   └── connection_pool.go # Connection pooling
│   ├── storage/               # Storage layer
│   │   ├── indexes.go        # Index implementations
│   │   ├── joins.go          # Join algorithms
│   │   ├── aggregates.go     # Aggregate functions
│   │   └── compression.go    # Data compression
│   ├── query/                 # Query processing
│   │   ├── sql_parser.go     # SQL parser
│   │   ├── vectorized_engine.go # Vectorized execution
│   │   ├── optimized_engine.go  # Query optimization
│   │   └── query_cache.go    # Result caching
│   └── server/                # Server implementation
│       ├── pg_server.go      # PostgreSQL protocol server
│       └── pg_protocol.go    # Protocol handlers
├── examples/                  # Example applications
├── benchmarks/                # Performance benchmarks
└── docs/                      # Documentation
```

## Architecture

### Columnar Storage

FastPostgres uses columnar storage for optimal analytical performance:
- Data stored in columns rather than rows
- Better compression ratios
- Vectorized operations for SIMD execution
- Cache-friendly access patterns

### Indexing

Multiple index types for different workloads:
- **Hash Index** - O(1) point lookups
- **B-Tree Index** - Range queries and sorting
- **Bitmap Index** - Low cardinality columns
- **Bloom Filter** - Fast existence checks

### Concurrency

Lock-free architecture with connection multiplexing:
- sync.Map for concurrent access
- Atomic operations for counters
- Connection pooling with 0.55KB per connection
- Scales to 150K+ concurrent connections

## Use Cases

### Analytics Dashboard
- Real-time business intelligence
- 2.11M rows/sec data ingestion
- Sub-millisecond aggregations

### High-Traffic Web Applications
- 23K concurrent queries/sec
- No connection pool bottlenecks
- Massive scalability

### Hybrid Workloads
- OLTP + OLAP in a single database
- No need for separate systems
- Linear concurrency scaling

## Limitations

**Current Limitations (In-Memory Only)**:
- All data stored in RAM
- No disk persistence
- Limited by available memory
- Data lost on restart

For production use with datasets larger than RAM, you would need to add:
- Disk persistence layer
- Buffer cache management
- Memory-mapped files
- Tiered storage (Hot/Warm/Cold)

## Development

### Building

```bash
go build -o fastpostgres ./cmd/fastpostgres/
```

### Running Tests

```bash
go test ./...
```

### Running Benchmarks

```bash
# Run specific benchmarks
go run ./benchmarks/benchmark_suite.go
go run ./benchmarks/comprehensive_benchmark.go
go run ./benchmarks/postgres_comparison.go
```

## Documentation

- [Benchmark Results](docs/BENCHMARK_RESULTS.md) - Detailed performance metrics
- [Optimization Summary](docs/OPTIMIZATION_SUMMARY.md) - Point lookup optimizations

## Contributing

This is a proof-of-concept project demonstrating high-performance database techniques in Go.

## License

MIT License

## Performance Comparison

| Metric | PostgreSQL | FastPostgres | Improvement |
|--------|------------|--------------|-------------|
| Data Ingestion | 29,659 rows/sec | 2,110,977 rows/sec | 71.2x faster |
| Point Lookups | 124µs | 208µs | Competitive |
| Aggregations | 17.3ms | <1ms | 17x+ faster |
| Concurrent Connections | 100-500 max | 150,000+ | 300-1500x more |
| Memory per Connection | 10MB | 0.55KB | 18,000x less |
| Connection Setup | 1-5ms | <50µs | 20-100x faster |

## Roadmap

- [ ] Disk persistence layer
- [ ] Write-ahead logging (WAL)
- [ ] MVCC snapshots
- [ ] Advanced compression (Delta, Dictionary, RLE)
- [ ] Distributed query execution
- [ ] Replication support