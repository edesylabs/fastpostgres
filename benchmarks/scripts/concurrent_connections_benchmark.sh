#!/bin/bash

set -e

echo "🚀 FastPostgres vs PostgreSQL Concurrent Connection Benchmark"
echo "=============================================================="
echo

test_concurrent_connections() {
    local db_name=$1
    local host=$2
    local port=$3
    local concurrent_count=$4

    echo "Testing $db_name with $concurrent_count concurrent connections..."

    start_time=$(date +%s.%N)

    for i in $(seq 1 $concurrent_count); do
        {
            timeout 10 psql -h $host -p $port -U postgres -d fastpostgres -c "SELECT 1;" > /dev/null 2>&1
        } &
    done

    wait

    end_time=$(date +%s.%N)
    duration=$(echo "$end_time - $start_time" | bc)

    echo "  ✓ $concurrent_count connections completed in ${duration}s"
    echo "  ✓ Rate: $(echo "scale=2; $concurrent_count / $duration" | bc) connections/second"
    echo
}

echo "🔗 Testing Connection Performance"
echo "================================="
echo

for connections in 10 50 100; do
    echo "--- Testing with $connections concurrent connections ---"

    echo "PostgreSQL (port 5432):"
    test_concurrent_connections "PostgreSQL" "localhost" "5432" $connections

    echo "FastPostgres (port 5433):"
    test_concurrent_connections "FastPostgres" "localhost" "5433" $connections

    echo
done

echo "✅ Benchmark completed!"
echo
echo "Note: FastPostgres runs on port 5433 (default) to avoid conflict with PostgreSQL (5432)"