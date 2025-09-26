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