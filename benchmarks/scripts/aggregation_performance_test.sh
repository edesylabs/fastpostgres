#!/bin/bash

set -e

echo "🔢 FastPostgres vs PostgreSQL Aggregation Performance Test"
echo "==========================================================="
echo

# Test configurations
TEST_SIZES=(1000 10000 50000 100000 500000)
POSTGRES_PORT=5432
FASTPG_PORT=5433

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

test_aggregation() {
    local db_name=$1
    local host=$2
    local port=$3
    local table_size=$4
    local iterations=$5

    echo -e "${BLUE}Testing $db_name with $table_size rows, $iterations iterations${NC}"

    # Create test table and data
    psql -h $host -p $port -U postgres -d testdb -c "DROP TABLE IF EXISTS agg_test;" >/dev/null 2>&1 || true

    psql -h $host -p $port -U postgres -d testdb -c "
        CREATE TABLE agg_test (
            id BIGINT,
            value BIGINT,
            category TEXT,
            price DECIMAL(10,2)
        );" >/dev/null 2>&1

    # Insert test data
    echo "  Inserting $table_size rows..."
    for i in $(seq 1 $table_size); do
        category=$((i % 10))
        value=$((RANDOM % 1000))
        price=$(echo "scale=2; $value * 1.5" | bc)
        psql -h $host -p $port -U postgres -d testdb -c "
            INSERT INTO agg_test VALUES ($i, $value, 'cat$category', $price);" >/dev/null 2>&1
    done

    # Test different aggregation queries
    declare -A queries=(
        ["COUNT"]="SELECT COUNT(*) FROM agg_test"
        ["SUM"]="SELECT SUM(value) FROM agg_test"
        ["AVG"]="SELECT AVG(value) FROM agg_test"
        ["MIN_MAX"]="SELECT MIN(value), MAX(value) FROM agg_test"
        ["GROUP_BY"]="SELECT category, COUNT(*), AVG(value) FROM agg_test GROUP BY category"
    )

    for query_name in "${!queries[@]}"; do
        query="${queries[$query_name]}"

        echo "  Testing $query_name query..."
        start_time=$(date +%s.%N)

        for ((j=1; j<=$iterations; j++)); do
            timeout 10 psql -h $host -p $port -U postgres -d testdb -c "$query" >/dev/null 2>&1
        done

        end_time=$(date +%s.%N)
        total_duration=$(echo "$end_time - $start_time" | bc)
        avg_duration=$(echo "scale=6; $total_duration / $iterations" | bc)
        qps=$(echo "scale=2; $iterations / $total_duration" | bc)

        echo "    ✓ $query_name: ${avg_duration}s avg, ${qps} queries/sec"

        # Store results for comparison
        echo "$db_name,$table_size,$query_name,$avg_duration,$qps" >> /tmp/aggregation_results.csv
    done

    echo
}

create_comparison_report() {
    echo "📊 Aggregation Performance Comparison Report"
    echo "============================================="
    echo

    if [[ ! -f /tmp/aggregation_results.csv ]]; then
        echo "No results found!"
        return
    fi

    echo "Results Summary:"
    echo "Database,Table_Size,Query_Type,Avg_Duration(s),QPS"
    cat /tmp/aggregation_results.csv | sort
    echo

    # Create performance comparison
    echo "Performance Comparison (FastPostgres vs PostgreSQL):"
    echo "===================================================="

    for size in "${TEST_SIZES[@]}"; do
        echo
        echo "Dataset Size: $size rows"
        echo "------------------------"

        # Compare each query type
        for query_type in "COUNT" "SUM" "AVG" "MIN_MAX" "GROUP_BY"; do
            fastpg_qps=$(grep "FastPostgres,$size,$query_type" /tmp/aggregation_results.csv | cut -d',' -f5)
            postgres_qps=$(grep "PostgreSQL,$size,$query_type" /tmp/aggregation_results.csv | cut -d',' -f5)

            if [[ -n "$fastpg_qps" && -n "$postgres_qps" ]]; then
                speedup=$(echo "scale=2; $fastpg_qps / $postgres_qps" | bc)
                if (( $(echo "$speedup > 1.1" | bc -l) )); then
                    echo -e "${GREEN}$query_type: FastPostgres is ${speedup}x faster${NC}"
                elif (( $(echo "$speedup < 0.9" | bc -l) )); then
                    speedup=$(echo "scale=2; $postgres_qps / $fastpg_qps" | bc)
                    echo -e "${RED}$query_type: PostgreSQL is ${speedup}x faster${NC}"
                else
                    echo "$query_type: Comparable performance (${speedup}x)"
                fi
            fi
        done
    done
}

# Initialize results file
echo "Database,Table_Size,Query_Type,Avg_Duration,QPS" > /tmp/aggregation_results.csv

echo "🚀 Starting aggregation performance tests..."
echo

# Test each dataset size
for size in "${TEST_SIZES[@]}"; do
    echo "=== Testing with $size rows ==="

    # Test PostgreSQL
    echo -e "${BLUE}PostgreSQL (port $POSTGRES_PORT)${NC}"
    if nc -z localhost $POSTGRES_PORT 2>/dev/null; then
        test_aggregation "PostgreSQL" "localhost" $POSTGRES_PORT $size 5
    else
        echo -e "${RED}PostgreSQL not running on port $POSTGRES_PORT${NC}"
    fi

    # Test FastPostgres
    echo -e "${BLUE}FastPostgres (port $FASTPG_PORT)${NC}"
    if nc -z localhost $FASTPG_PORT 2>/dev/null; then
        test_aggregation "FastPostgres" "localhost" $FASTPG_PORT $size 5
    else
        echo -e "${RED}FastPostgres not running on port $FASTPG_PORT${NC}"
    fi

    echo
done

# Generate comparison report
create_comparison_report

# Cleanup
rm -f /tmp/aggregation_results.csv

echo "✅ Aggregation performance test completed!"