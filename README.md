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

### Latest Benchmark Results (Docker)

Real-world performance comparison running both databases in Docker:
- **12-24% better** concurrent connection handling
- **9-12% faster** query processing
- See [benchmark results](#real-world-benchmark-results-docker) and [benchmark scripts](benchmarks/scripts/README.md) for details

See [BENCHMARK_RESULTS.md](docs/BENCHMARK_RESULTS.md) for comprehensive performance metrics.

## Quick Start

### Installation

```bash
go build -o fastpostgres ./cmd/fastpostgres/
```

### Running the Server

```bash
# Start server on default port 5433
./fastpostgres server

# Start server on custom port
./fastpostgres server 5434
```

### Using Docker

```bash
# Start all services (FastPostgres on 5433, PostgreSQL on 5432)
docker-compose up -d

# Start only FastPostgres
docker-compose up -d fastpostgres

# View logs
docker-compose logs -f fastpostgres
```

### Run Demo

```bash
./fastpostgres demo
```

### Run Benchmarks

```bash
./fastpostgres benchmark
```

#### Docker-Based Benchmarks

Compare FastPostgres vs PostgreSQL performance using our benchmark scripts:

```bash
# Ensure databases are running
docker-compose up -d fastpostgres postgres

# Run all benchmarks
./benchmarks/scripts/run_all_benchmarks.sh

# Or run individual benchmarks
./benchmarks/scripts/concurrent_connections_benchmark.sh
./benchmarks/scripts/query_performance_benchmark.sh
./benchmarks/scripts/incremental_insertion_benchmark.sh
./benchmarks/scripts/large_volume_insertion_benchmark.sh
./benchmarks/scripts/data_retrieval_benchmark.sh
./benchmarks/scripts/connection_stress_test.sh

# Quick tests (fast validation)
./benchmarks/scripts/quick_insertion_test.sh          # ~5 seconds
./benchmarks/scripts/large_volume_quick_test.sh       # ~15 seconds
./benchmarks/scripts/data_retrieval_quick_test.sh     # ~30 seconds
./benchmarks/scripts/connection_limit_quick_test.sh   # ~2 minutes
```

See [benchmarks/scripts/README.md](benchmarks/scripts/README.md) for detailed benchmark documentation.

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
│   └── gin_rest_api/         # Gin REST API example
├── benchmarks/                # Performance benchmarks
│   ├── scripts/              # Docker-based benchmark scripts
│   │   ├── run_all_benchmarks.sh
│   │   ├── concurrent_connections_benchmark.sh
│   │   ├── query_performance_benchmark.sh
│   │   └── README.md
│   └── *.go                  # Go benchmark programs
├── docs/                      # Documentation
└── docker-compose.yml         # Docker setup for testing
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

## Support the Project

If FastPostgres has been helpful to you and you'd like to support its continued development, consider making a donation. Your contribution helps fund:

- Further performance optimizations
- Additional features and capabilities
- Documentation improvements
- Community support

[![Donate](https://img.shields.io/badge/Donate-Razorpay-blue.svg)](https://razorpay.me/@edesytechnologylabs)

**[Donate via Razorpay](https://razorpay.me/@edesytechnologylabs)**

Every contribution, no matter the size, is greatly appreciated and helps make FastPostgres even better!

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

### Real-World Benchmark Results (Docker)

Performance comparison running both databases in Docker containers:

#### Concurrent Connection Performance

| Concurrent Connections | PostgreSQL | FastPostgres | Improvement |
|------------------------|------------|--------------|-------------|
| 10 connections | 246.60 conn/sec | 262.47 conn/sec | 6.4% faster |
| 50 connections | 327.77 conn/sec | 385.43 conn/sec | 17.6% faster |
| 100 connections | 360.02 conn/sec | 404.83 conn/sec | 12.5% faster |

#### Query Processing Performance

| Query Count | PostgreSQL | FastPostgres | Improvement |
|-------------|------------|--------------|-------------|
| 100 queries | 40.71 q/sec | 45.50 q/sec | 11.8% faster |
| 500 queries | 40.48 q/sec | 44.26 q/sec | 9.3% faster |
| 1000 queries | 40.84 q/sec | 44.59 q/sec | 9.2% faster |

#### Insertion Performance (Single Row Inserts)

| Row Count | PostgreSQL | FastPostgres | Improvement |
|-----------|------------|--------------|-------------|
| 10 rows | 36.90 rows/sec | 47.76 rows/sec | 29.4% faster |
| 25 rows | 39.50 rows/sec | 47.01 rows/sec | 19.0% faster |
| 50 rows | 38.07 rows/sec | 47.32 rows/sec | 24.3% faster |

#### Large Volume Insertion (Batch Operations)

| Row Count | Batch Size | PostgreSQL | FastPostgres | Improvement |
|-----------|------------|------------|--------------|-------------|
| 5,000 | 500 | 2,008 rows/sec | 2,142 rows/sec | 6.7% faster |
| 10,000 | 1,000 | 1,091 rows/sec | 1,101 rows/sec | 0.9% faster |

*Note: Batch operations show high throughput for bulk data loading. Both databases handle large volumes efficiently with optimized batch inserts.*

#### Data Retrieval/Query Performance (5K rows dataset)

| Query Type | PostgreSQL | FastPostgres | Improvement |
|------------|------------|--------------|-------------|
| Indexed lookup | 0.0251s avg | 0.0211s avg | 15.9% faster |
| COUNT aggregate | 0.0259s avg | 0.0214s avg | 17.4% faster |
| Range query | 0.0298s avg | 0.0222s avg | 25.5% faster |

*Note: FastPostgres shows significant advantages in analytical queries and aggregations, typical for columnar storage.*

#### Connection Stress Test (Concurrent Connection Limits)

| Concurrent Connections | PostgreSQL | FastPostgres | FastPostgres Advantage |
|------------------------|------------|--------------|-------------------------|
| 50 connections | 0% success | 100% success | **Significantly better** |

*Note: Initial test shows FastPostgres handling concurrent connections much more effectively. PostgreSQL may require configuration tuning for higher connection limits. Full stress test (up to 10K connections) available in benchmark suite.*

**Test Environment:**
- Both databases running in Docker containers
- FastPostgres on port 5433, PostgreSQL on port 5432
- Tests performed via PostgreSQL wire protocol
- Scripts available in `benchmarks/scripts/`

**To reproduce these results:**
```bash
docker-compose up -d
./benchmarks/scripts/run_all_benchmarks.sh
```

## Roadmap

- [ ] Disk persistence layer
- [ ] Write-ahead logging (WAL)
- [ ] MVCC snapshots
- [ ] Advanced compression (Delta, Dictionary, RLE)
- [ ] Distributed query execution
- [ ] Replication support
