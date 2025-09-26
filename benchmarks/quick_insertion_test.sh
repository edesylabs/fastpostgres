#!/bin/bash

set -e

echo "🚀 Quick Insertion Test: FastPostgres vs PostgreSQL"
echo "==================================================="
echo

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'

# Test configurations
TEST_ROWS=10000
BATCH_SIZE=500

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

test_insertion_performance() {
    local db_name=$1
    local host=$2
    local port=$3
    local database=$4

    echo -e "${BLUE}Testing $db_name insertion performance${NC}"
    echo "Target: $TEST_ROWS rows with batch size $BATCH_SIZE"

    # Setup table
    if [ "$db_name" = "PostgreSQL" ]; then
        psql -h $host -p $port -U postgres -d $database -c "
        DROP TABLE IF EXISTS quick_test;
        CREATE TABLE quick_test (
            id SERIAL PRIMARY KEY,
            user_id INTEGER,
            name VARCHAR(50),
            email VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );" > /dev/null 2>&1
    else
        psql -h $host -p $port -U postgres -d $database -c "
        DROP TABLE IF EXISTS quick_test;
        CREATE TABLE quick_test (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            name VARCHAR(50),
            email VARCHAR(100),
            created_at INTEGER
        );" > /dev/null 2>&1
    fi

    local start_time=$(date +%s.%N)
    local inserted=0

    # Insert data in batches
    for ((i=1; i<=TEST_ROWS; i+=BATCH_SIZE)); do
        local batch_end=$((i + BATCH_SIZE - 1))
        if [ $batch_end -gt $TEST_ROWS ]; then
            batch_end=$TEST_ROWS
        fi

        local values=""
        for ((j=i; j<=batch_end; j++)); do
            if [ "$values" != "" ]; then
                values="$values, "
            fi
            if [ "$db_name" = "PostgreSQL" ]; then
                values="$values($j, 'user_$j', 'user$j@test.com')"
            else
                values="$values($j, $j, 'user_$j', 'user$j@test.com', $((1640995200 + j)))"
            fi
        done

        if [ "$db_name" = "PostgreSQL" ]; then
            psql -h $host -p $port -U postgres -d $database -c "
                INSERT INTO quick_test (user_id, name, email) VALUES $values;
            " > /dev/null 2>&1
        else
            psql -h $host -p $port -U postgres -d $database -c "
                INSERT INTO quick_test (id, user_id, name, email, created_at) VALUES $values;
            " > /dev/null 2>&1
        fi

        inserted=$((batch_end - i + 1 + inserted))
        local percent=$((inserted * 100 / TEST_ROWS))
        printf "\r  Progress: %3d%% (%d / %d rows)" $percent $inserted $TEST_ROWS
    done

    echo ""

    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc)
    local rows_per_sec=$(echo "scale=0; $TEST_ROWS / $duration" | bc)

    # Verify count
    local actual_count=$(psql -h $host -p $port -U postgres -d $database -t -c "SELECT COUNT(*) FROM quick_test;" 2>/dev/null | tr -d ' ')

    print_success "$db_name completed:"
    echo "  • Rows inserted: $actual_count"
    echo "  • Duration: ${duration}s"
    echo "  • Throughput: $rows_per_sec rows/sec"
    echo

    # Store results
    echo "$db_name,$actual_count,$duration,$rows_per_sec" >> /tmp/quick_insertion.csv
}

echo "Database,Rows,Duration,Rows_per_sec" > /tmp/quick_insertion.csv

# Test PostgreSQL
test_insertion_performance "PostgreSQL" "localhost" "5432" "testdb"

# Test FastPostgres
test_insertion_performance "FastPostgres" "localhost" "5433" "fastpostgres"

# Compare results
echo -e "${BLUE}Performance Comparison:${NC}"
echo "======================="

while IFS=',' read -r db rows duration rate; do
    echo "$db: $rows rows in ${duration}s ($rate rows/sec)"
done < /tmp/quick_insertion.csv

# Calculate comparison
pg_rate=$(grep "PostgreSQL" /tmp/quick_insertion.csv | cut -d',' -f4)
fp_rate=$(grep "FastPostgres" /tmp/quick_insertion.csv | cut -d',' -f4)

if [[ -n "$pg_rate" && -n "$fp_rate" ]]; then
    if (( $(echo "$fp_rate > $pg_rate" | bc -l) )); then
        speedup=$(echo "scale=2; $fp_rate / $pg_rate" | bc)
        improvement=$(echo "scale=1; (($fp_rate - $pg_rate) / $pg_rate) * 100" | bc)
        print_success "FastPostgres is ${improvement}% faster (${speedup}x)"
    else
        speedup=$(echo "scale=2; $pg_rate / $fp_rate" | bc)
        improvement=$(echo "scale=1; (($pg_rate - $fp_rate) / $fp_rate) * 100" | bc)
        echo -e "${CYAN}PostgreSQL is ${improvement}% faster (${speedup}x)${NC}"
    fi
fi

rm -f /tmp/quick_insertion.csv
print_success "Quick insertion test completed!"