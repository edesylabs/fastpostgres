#!/bin/bash

set -e

echo "🔍 Data Retrieval - Quick Test"
echo "==============================="
echo

GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

setup_and_test() {
    local db_name=$1
    local host=$2
    local port=$3

    echo -e "${BLUE}Testing $db_name${NC}"

    # Setup table with 5000 rows
    psql -h $host -p $port -U postgres -d fastpostgres > /dev/null 2>&1 << 'EOF'
DROP TABLE IF EXISTS quick_query_test;
CREATE TABLE quick_query_test (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    age INTEGER,
    salary INTEGER,
    department VARCHAR(50),
    status VARCHAR(20)
);

CREATE INDEX idx_user_id ON quick_query_test(user_id);
CREATE INDEX idx_age ON quick_query_test(age);
EOF

    echo -n "  Setting up 5,000 rows... "
    for ((i=1; i<=5000; i+=500)); do
        local values=""
        for ((j=i; j<i+500 && j<=5000; j++)); do
            if [ "$values" != "" ]; then values="$values, "; fi
            local dept=$(echo "Dept_$((RANDOM % 5))")
            values="$values($j, $((20 + RANDOM % 50)), $((30000 + RANDOM % 100000)), '$dept', 'active')"
        done
        psql -h $host -p $port -U postgres -d fastpostgres -c "INSERT INTO quick_query_test (user_id, age, salary, department, status) VALUES $values;" > /dev/null 2>&1
    done
    echo -e "${GREEN}Done${NC}"

    # Test queries
    echo "  Running queries..."

    # Simple indexed query
    echo -n "    Indexed lookup... "
    start=$(date +%s.%N)
    for i in {1..50}; do
        psql -h $host -p $port -U postgres -d fastpostgres -c "SELECT * FROM quick_query_test WHERE user_id = 2500;" > /dev/null 2>&1
    done
    end=$(date +%s.%N)
    time1=$(echo "scale=4; ($end - $start) / 50" | bc)
    echo "${time1}s avg"

    # COUNT aggregate
    echo -n "    COUNT aggregate... "
    start=$(date +%s.%N)
    for i in {1..50}; do
        psql -h $host -p $port -U postgres -d fastpostgres -c "SELECT COUNT(*) FROM quick_query_test WHERE status = 'active';" > /dev/null 2>&1
    done
    end=$(date +%s.%N)
    time2=$(echo "scale=4; ($end - $start) / 50" | bc)
    echo "${time2}s avg"

    # Range query
    echo -n "    Range query... "
    start=$(date +%s.%N)
    for i in {1..30}; do
        psql -h $host -p $port -U postgres -d fastpostgres -c "SELECT * FROM quick_query_test WHERE age BETWEEN 30 AND 40;" > /dev/null 2>&1
    done
    end=$(date +%s.%N)
    time3=$(echo "scale=4; ($end - $start) / 30" | bc)
    echo "${time3}s avg"

    # Cleanup
    psql -h $host -p $port -U postgres -d fastpostgres -c "DROP TABLE quick_query_test;" > /dev/null 2>&1

    echo -e "${GREEN}  ✓ Completed${NC}"
    echo
}

echo "Quick validation of query performance..."
echo

setup_and_test "PostgreSQL" "localhost" "5432"
setup_and_test "FastPostgres" "localhost" "5433"

echo -e "${GREEN}✅ Quick test completed!${NC}"
echo
echo "For comprehensive query testing, run:"
echo "  ./benchmarks/scripts/data_retrieval_benchmark.sh"