#!/bin/bash

set -e

echo "🔍 FastPostgres Insertion Debug Test"
echo "===================================="
echo

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m'

test_insertion_capacity() {
    local batch_size=$1
    local total_rows=$2

    echo "Testing batch size: $batch_size, total rows: $total_rows"

    # Setup table
    timeout 10 psql -h localhost -p 5433 -U postgres -d fastpostgres -c "
    DROP TABLE IF EXISTS debug_test;
    CREATE TABLE debug_test (
        id INTEGER PRIMARY KEY,
        name VARCHAR(50),
        email VARCHAR(100)
    );" >/dev/null 2>&1

    if [ $? -ne 0 ]; then
        echo -e "${RED}❌ Failed to create table${NC}"
        return 1
    fi

    local start_time=$(date +%s.%N)
    local inserted=0

    for ((i=1; i<=total_rows; i+=batch_size)); do
        local batch_end=$((i + batch_size - 1))
        if [ $batch_end -gt $total_rows ]; then
            batch_end=$total_rows
        fi

        # Generate batch values
        local values=""
        for ((j=i; j<=batch_end; j++)); do
            if [ "$values" != "" ]; then
                values="$values, "
            fi
            values="$values($j, 'user_$j', 'user$j@test.com')"
        done

        # Execute batch insert with timeout
        if timeout 10 psql -h localhost -p 5433 -U postgres -d fastpostgres -c "
            INSERT INTO debug_test (id, name, email) VALUES $values;
        " >/dev/null 2>&1; then
            inserted=$batch_end
            echo "  ✓ Inserted batch ending at row $batch_end"
        else
            echo -e "  ${RED}❌ Failed at batch ending at row $batch_end${NC}"
            break
        fi
    done

    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc)

    # Verify count
    local count=$(timeout 5 psql -h localhost -p 5433 -U postgres -d fastpostgres -t -c "SELECT COUNT(*) FROM debug_test;" 2>/dev/null | tr -d ' ' || echo "0")

    local throughput=0
    if (( $(echo "$duration > 0" | bc -l) )); then
        throughput=$(echo "scale=0; $count / $duration" | bc 2>/dev/null || echo "0")
    fi

    echo -e "${GREEN}  Result: $count rows inserted in ${duration}s ($throughput rows/sec)${NC}"
    echo

    return 0
}

echo "Testing various batch sizes to find optimal performance..."
echo

# Test different batch sizes
echo "=== Testing Batch Size Limits ==="
test_insertion_capacity 10 100      # Very small batch
test_insertion_capacity 50 500      # Small batch
test_insertion_capacity 100 1000    # Medium batch
test_insertion_capacity 200 2000    # Large batch
test_insertion_capacity 500 2500    # Very large batch

echo "=== Testing Row Count Limits ==="
test_insertion_capacity 100 5000    # 5K rows
test_insertion_capacity 100 10000   # 10K rows

echo "🎯 Debug test completed!"
echo
echo "Key Findings:"
echo "• Identify which batch sizes work reliably"
echo "• Find the point where FastPostgres starts to hang"
echo "• Determine optimal batch size for large insertions"