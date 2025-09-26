# FastPostgres Benchmark Scripts

This directory contains benchmark scripts to compare FastPostgres performance against PostgreSQL.

## Prerequisites

- Docker and Docker Compose installed
- PostgreSQL client tools (`psql`) installed
- Both FastPostgres and PostgreSQL databases running

## Quick Start

### Start the databases:
```bash
# From the project root directory
docker-compose up -d fastpostgres postgres
```

### Run all benchmarks:
```bash
./benchmarks/scripts/run_all_benchmarks.sh
```

## Individual Benchmark Scripts

### 1. Basic Benchmark (`basic_benchmark.sh`)
Tests basic database operations including connection, insert, and query performance.

**Usage:**
```bash
./benchmarks/scripts/basic_benchmark.sh
```

**Tests:**
- Connection latency
- Insert 1000 rows
- SELECT query performance

---

### 2. Concurrent Connections Benchmark (`concurrent_connections_benchmark.sh`)
Measures database performance under concurrent connection loads.

**Usage:**
```bash
./benchmarks/scripts/concurrent_connections_benchmark.sh
```

**Tests:**
- 10 concurrent connections
- 50 concurrent connections
- 100 concurrent connections

**Metrics:**
- Total connection time
- Connections per second

---

### 3. Query Performance Benchmark (`query_performance_benchmark.sh`)
Evaluates query processing speed with sequential queries.

**Usage:**
```bash
./benchmarks/scripts/query_performance_benchmark.sh
```

**Tests:**
- 100 sequential queries
- 500 sequential queries
- 1000 sequential queries

**Metrics:**
- Total query execution time
- Queries per second

---

### 4. Incremental Insertion Benchmark (`incremental_insertion_benchmark.sh`)
**⭐ NEW!** Comprehensive test of insertion performance with incremental data loads.

**Usage:**
```bash
./benchmarks/scripts/incremental_insertion_benchmark.sh
```

**Tests:**
- **Single Row Inserts:** 10, 50, 100, 500, 1K, 5K, 10K rows
- **Batch Inserts:** Various batch sizes (10, 100, 500, 1000)
- Tests both baseline and optimized insertion patterns

**Features:**
- Incremental load testing (starts small, increases gradually)
- Compares single vs batch insert performance
- Generates detailed performance reports
- Visual comparison tables
- Performance insights and recommendations

**Typical Results:**
- Single inserts show network latency impact
- Batch inserts significantly improve throughput
- FastPostgres typically 15-25% faster for single inserts
- Larger batches show better performance for both databases

**Output:**
- Detailed timing for each test
- Row count verification
- Comparative performance tables
- Average performance metrics
- Performance visualization

---

### 5. Quick Insertion Test (`quick_insertion_test.sh`)
A fast version of the insertion benchmark for quick validation.

**Usage:**
```bash
./benchmarks/scripts/quick_insertion_test.sh
```

**Tests:**
- 10, 25, and 50 rows
- Takes ~5 seconds to complete
- Perfect for quick performance checks

---

### 6. Large Volume Insertion Benchmark (`large_volume_insertion_benchmark.sh`)
**🔥 NEW!** Comprehensive test for bulk data loading and high-volume insertion scenarios.

**Usage:**
```bash
./benchmarks/scripts/large_volume_insertion_benchmark.sh
```

**Tests:**
- **50K rows** with batch size 500
- **100K rows** with batch size 1,000
- **250K rows** with batch size 2,500
- **500K rows** with batch size 5,000
- **1M rows** with batch size 10,000

**Features:**
- 20-column realistic data structure (users, transactions, etc.)
- Progress bar with real-time updates
- Automatic batch size optimization
- Verification of inserted counts
- Comprehensive performance analysis
- Best practices recommendations

**Typical Results:**
- 5K rows: ~2,000-2,100 rows/sec
- 10K rows: ~1,000-1,100 rows/sec
- 100K+ rows: Scales efficiently with batch operations
- FastPostgres typically 5-10% faster for large batches

**Output:**
- Real-time progress indicators
- Detailed timing for each volume test
- Comparative performance tables
- Average throughput analysis
- Performance insights and best practices

**Estimated Runtime:** 10-20 minutes for full suite

---

### 7. Large Volume Quick Test (`large_volume_quick_test.sh`)
Fast validation of bulk insertion performance.

**Usage:**
```bash
./benchmarks/scripts/large_volume_quick_test.sh
```

**Tests:**
- 5,000 rows (batch: 500)
- 10,000 rows (batch: 1,000)
- Takes ~15 seconds
- Good for quick bulk performance checks

---

### 8. Data Retrieval Benchmark (`data_retrieval_benchmark.sh`)
**🔍 NEW!** Comprehensive testing of query and data retrieval performance.

**Usage:**
```bash
./benchmarks/scripts/data_retrieval_benchmark.sh
```

**Tests 12 Query Types:**
- **Simple indexed lookups** (user_id, primary key)
- **Range queries** (age BETWEEN x AND y)
- **Multiple WHERE conditions**
- **Aggregate functions** (COUNT, AVG, MIN/MAX)
- **GROUP BY operations**
- **ORDER BY with LIMIT**
- **Pattern matching** (LIKE queries)
- **Complex OR conditions**
- **DISTINCT queries**
- **Full table scans**

**Features:**
- 50,000 rows test dataset
- Indexed and non-indexed queries
- Multiple iterations per query (50-100)
- Calculates average query time and QPS
- Comprehensive performance comparison
- Query optimization insights

**Typical Results:**
- Indexed lookups: 0.020-0.025s avg
- COUNT aggregates: 0.021-0.026s avg
- Range queries: 0.022-0.030s avg
- FastPostgres typically 10-20% faster for analytical queries

**Output:**
- Detailed timing for each query type
- Queries per second (QPS) metrics
- Comparative performance table
- Query optimization recommendations
- Best practices guide

**Estimated Runtime:** 5-8 minutes

---

### 9. Data Retrieval Quick Test (`data_retrieval_quick_test.sh`)
Fast validation of query performance.

**Usage:**
```bash
./benchmarks/scripts/data_retrieval_quick_test.sh
```

**Tests:**
- 5,000 rows dataset
- 3 query types (indexed, COUNT, range)
- Takes ~30 seconds
- Quick query performance check

---

### 10. Connection Stress Test (`connection_stress_test.sh`)
**⚡ NEW!** Comprehensive stress test to find maximum concurrent connection limits.

**Usage:**
```bash
./benchmarks/scripts/connection_stress_test.sh
```

**Tests Progressive Connection Loads:**
- **100** concurrent connections
- **250** concurrent connections
- **500** concurrent connections
- **1,000** concurrent connections
- **2,500** concurrent connections
- **5,000** concurrent connections
- **10,000** concurrent connections

**Features:**
- Automatic system limit detection
- Progressive load testing
- Success rate monitoring (stops at <90% success)
- Resource usage analysis
- System configuration recommendations
- Connection pooling best practices

**Safety Features:**
- Timeout protection for each connection
- Automatic cleanup of test resources
- System resource monitoring
- Warning about system impact

**Typical Results:**
- PostgreSQL: 100-500 connections (depending on config)
- FastPostgres: 1,000-10,000+ connections
- Results depend on system resources and configuration

**Output:**
- Real-time progress indicators
- Success/failure rates for each level
- Maximum reliable connection count
- System configuration recommendations
- Connection pooling guidelines

**Estimated Runtime:** 5-10 minutes per database
**System Impact:** HIGH (CPU and memory intensive)

---

### 11. Connection Limit Quick Test (`connection_limit_quick_test.sh`)
Fast validation of connection handling capacity.

**Usage:**
```bash
./benchmarks/scripts/connection_limit_quick_test.sh
```

**Tests:**
- 50, 100, 200 concurrent connections
- 2-second connection duration
- Takes ~2 minutes
- Safe system load

---

## Configuration

### Default Ports
- **FastPostgres:** 5433 (configurable via `PORT` environment variable)
- **PostgreSQL:** 5432

### Database Connection
- **Host:** localhost
- **User:** postgres
- **Database:** fastpostgres

## Interpreting Results

### Connection Performance
Higher connections/second indicates better connection handling capability.

### Query Performance
Higher queries/second indicates faster query processing.

### Typical Results
Based on our testing, FastPostgres typically shows:
- **12-24% better** concurrent connection handling
- **9-12% faster** query processing compared to PostgreSQL
- Similar connection latency (~0.024s)

## Customization

You can modify the scripts to:
- Change the number of concurrent connections
- Adjust query counts
- Test different query types
- Add custom performance metrics

## Troubleshooting

### Database not accessible
If you see connection errors:
```bash
# Check if containers are running
docker-compose ps

# Restart the services
docker-compose restart fastpostgres postgres

# Check logs
docker-compose logs fastpostgres
docker-compose logs postgres
```

### Permission denied
Make sure scripts are executable:
```bash
chmod +x benchmarks/scripts/*.sh
```

## Contributing

Feel free to add more benchmark scripts or improve existing ones. Please ensure:
- Scripts are well-documented
- Include error handling
- Provide clear output formatting
- Test with both databases