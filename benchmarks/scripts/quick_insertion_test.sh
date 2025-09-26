#!/bin/bash

set -e

echo "🚀 Quick Insertion Performance Test"
echo "===================================="
echo

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

test_insertion() {
    local db_name=$1
    local host=$2
    local port=$3
    local row_count=$4

    echo -e "${BLUE}Testing $db_name with $row_count rows...${NC}"

    psql -h $host -p $port -U postgres -d fastpostgres > /dev/null 2>&1 << EOF
DROP TABLE IF EXISTS quick_test;
CREATE TABLE quick_test (id SERIAL PRIMARY KEY, name VARCHAR(100), value INTEGER);
EOF

    start_time=$(date +%s.%N)

    for i in $(seq 1 $row_count); do
        psql -h $host -p $port -U postgres -d fastpostgres -c "INSERT INTO quick_test (name, value) VALUES ('Test_$i', $i);" > /dev/null 2>&1
    done

    end_time=$(date +%s.%N)
    duration=$(echo "$end_time - $start_time" | bc)
    rows_per_sec=$(echo "scale=2; $row_count / $duration" | bc)

    count=$(psql -h $host -p $port -U postgres -d fastpostgres -t -c "SELECT COUNT(*) FROM quick_test;" 2>/dev/null | tr -d ' ')

    echo -e "${GREEN}✓ Inserted $count rows${NC}"
    echo "  Duration: ${duration}s"
    echo "  Rate: ${rows_per_sec} rows/sec"
    echo

    psql -h $host -p $port -U postgres -d fastpostgres -c "DROP TABLE quick_test;" > /dev/null 2>&1
}

echo "Testing incremental insertion performance..."
echo

for size in 10 25 50; do
    echo "═══ Test with $size rows ═══"
    test_insertion "PostgreSQL" "localhost" "5432" $size
    test_insertion "FastPostgres" "localhost" "5433" $size
    echo
done

echo -e "${GREEN}✅ Quick test completed!${NC}"
echo
echo "For comprehensive testing, run:"
echo "  ./benchmarks/scripts/incremental_insertion_benchmark.sh"