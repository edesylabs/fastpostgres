#!/bin/bash

set -e

echo "📊 FastPostgres vs PostgreSQL Query Performance Benchmark"
echo "=========================================================="
echo

test_query_performance() {
    local db_name=$1
    local host=$2
    local port=$3
    local query_count=$4

    echo "Testing $db_name with $query_count queries..."

    start_time=$(date +%s.%N)

    for i in $(seq 1 $query_count); do
        timeout 5 psql -h $host -p $port -U postgres -d fastpostgres -c "SELECT $i;" > /dev/null 2>&1
    done

    end_time=$(date +%s.%N)
    duration=$(echo "$end_time - $start_time" | bc)

    echo "  ✓ $query_count queries completed in ${duration}s"
    echo "  ✓ Rate: $(echo "scale=2; $query_count / $duration" | bc) queries/second"
    echo
}

echo "🔍 Testing Query Processing Performance"
echo "====================================="
echo

for queries in 100 500 1000; do
    echo "--- Testing with $queries sequential queries ---"

    echo "PostgreSQL (port 5432):"
    test_query_performance "PostgreSQL" "localhost" "5432" $queries

    echo "FastPostgres (port 5433):"
    test_query_performance "FastPostgres" "localhost" "5433" $queries

    echo
done

echo "📈 Summary"
echo "========="
echo "FastPostgres is running on port 5433 (default changed from 5432)"
echo "Both databases are responding to queries via PostgreSQL wire protocol"
echo "Performance comparison shows query processing speed differences"
echo
echo "✅ All benchmarks completed!"