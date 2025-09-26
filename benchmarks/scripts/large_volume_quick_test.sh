#!/bin/bash

set -e

echo "🚀 Large Volume Insertion - Quick Test"
echo "======================================="
echo

GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

test_bulk_insert() {
    local db_name=$1
    local host=$2
    local port=$3
    local rows=$4
    local batch=$5

    echo -e "${BLUE}Testing $db_name: $rows rows (batch: $batch)${NC}"

    # Setup table
    psql -h $host -p $port -U postgres -d fastpostgres > /dev/null 2>&1 << 'EOF'
DROP TABLE IF EXISTS bulk_test;
CREATE TABLE bulk_test (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    username VARCHAR(100),
    email VARCHAR(100),
    age INTEGER,
    balance DECIMAL(10,2),
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
EOF

    start_time=$(date +%s.%N)

    # Insert in batches
    for ((i=1; i<=rows; i+=batch)); do
        values=""
        for ((j=i; j<i+batch && j<=rows; j++)); do
            if [ "$values" != "" ]; then values="$values, "; fi
            values="$values($j, 'user_$j', 'user$j@example.com', $((20+RANDOM%50)), $((RANDOM%10000)).99, 'active')"
        done

        psql -h $host -p $port -U postgres -d fastpostgres -c "INSERT INTO bulk_test (user_id, username, email, age, balance, status) VALUES $values;" > /dev/null 2>&1
    done

    end_time=$(date +%s.%N)
    duration=$(echo "$end_time - $start_time" | bc)
    rate=$(echo "scale=2; $rows / $duration" | bc)

    count=$(psql -h $host -p $port -U postgres -d fastpostgres -t -c "SELECT COUNT(*) FROM bulk_test;" 2>/dev/null | tr -d ' ')

    echo -e "${GREEN}✓ Inserted $count rows in ${duration}s${NC}"
    echo "  Throughput: ${rate} rows/sec"
    echo

    # Cleanup
    psql -h $host -p $port -U postgres -d fastpostgres -c "DROP TABLE bulk_test;" > /dev/null 2>&1
}

echo "Quick validation of large volume insertion..."
echo

# Test with 5000 rows (batch 500)
echo "═══ Test: 5,000 rows ═══"
test_bulk_insert "PostgreSQL" "localhost" "5432" 5000 500
test_bulk_insert "FastPostgres" "localhost" "5433" 5000 500

echo "═══ Test: 10,000 rows ═══"
test_bulk_insert "PostgreSQL" "localhost" "5432" 10000 1000
test_bulk_insert "FastPostgres" "localhost" "5433" 10000 1000

echo -e "${GREEN}✅ Quick test completed!${NC}"
echo
echo "For comprehensive large volume testing, run:"
echo "  ./benchmarks/scripts/large_volume_insertion_benchmark.sh"