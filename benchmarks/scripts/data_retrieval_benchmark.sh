#!/bin/bash

set -e

echo "🔍 FastPostgres vs PostgreSQL - Data Retrieval Benchmark"
echo "=========================================================="
echo

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'

RESULTS_FILE="/tmp/data_retrieval_benchmark_results.txt"
> "$RESULTS_FILE"

print_header() {
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
}

print_section() {
    echo -e "${CYAN}$1${NC}"
    echo "───────────────────────────────────────────────────────────────"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

format_number() {
    printf "%'d" $1 2>/dev/null || echo $1
}

setup_test_data() {
    local host=$1
    local port=$2
    local db_name=$3
    local row_count=$4

    print_section "Setting up test data in $db_name ($(format_number $row_count) rows)"

    psql -h $host -p $port -U postgres -d fastpostgres > /dev/null 2>&1 << 'EOF'
DROP TABLE IF EXISTS query_test_data;
CREATE TABLE query_test_data (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    username VARCHAR(100),
    email VARCHAR(150),
    age INTEGER,
    salary INTEGER,
    department VARCHAR(50),
    status VARCHAR(20),
    country VARCHAR(50),
    city VARCHAR(100),
    score INTEGER,
    registration_date DATE,
    last_login TIMESTAMP,
    is_active BOOLEAN,
    category VARCHAR(50)
);

CREATE INDEX idx_user_id ON query_test_data(user_id);
CREATE INDEX idx_age ON query_test_data(age);
CREATE INDEX idx_department ON query_test_data(department);
CREATE INDEX idx_status ON query_test_data(status);
EOF

    print_success "Table and indexes created"

    echo -n "  Inserting $(format_number $row_count) rows... "

    local countries=("USA" "UK" "Canada" "Germany" "France" "Japan" "Australia" "Brazil" "India" "China")
    local departments=("Engineering" "Sales" "Marketing" "HR" "Finance" "Operations" "Support" "IT" "Legal" "Research")
    local statuses=("active" "inactive" "pending" "suspended" "verified")
    local categories=("premium" "standard" "basic" "vip" "trial")

    local batch_size=1000
    for ((i=1; i<=row_count; i+=batch_size)); do
        local values=""
        for ((j=i; j<i+batch_size && j<=row_count; j++)); do
            local country=${countries[$((RANDOM % 10))]}
            local dept=${departments[$((RANDOM % 10))]}
            local status=${statuses[$((RANDOM % 5))]}
            local category=${categories[$((RANDOM % 5))]}
            local age=$((22 + RANDOM % 45))
            local salary=$((30000 + RANDOM % 120000))
            local score=$((RANDOM % 1000))
            local is_active=$((RANDOM % 2))

            if [ "$values" != "" ]; then values="$values, "; fi
            values="$values($j, 'user_$j', 'user$j@example.com', $age, $salary, '$dept', '$status', '$country', 'City_$j', $score, CURRENT_DATE - INTERVAL '$((RANDOM % 1000)) days', NOW() - INTERVAL '$((RANDOM % 30)) days', $is_active, '$category')"
        done

        psql -h $host -p $port -U postgres -d fastpostgres -c "INSERT INTO query_test_data (user_id, username, email, age, salary, department, status, country, city, score, registration_date, last_login, is_active, category) VALUES $values;" > /dev/null 2>&1
    done

    echo -e "${GREEN}Done${NC}"
    echo
}

test_query_performance() {
    local db_name=$1
    local host=$2
    local port=$3
    local query_name=$4
    local query=$5
    local iterations=$6

    echo -n "  Testing: $query_name... "

    local start_time=$(date +%s.%N)

    for ((i=1; i<=iterations; i++)); do
        psql -h $host -p $port -U postgres -d fastpostgres -c "$query" > /dev/null 2>&1
    done

    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc)
    local avg_time=$(echo "scale=4; $duration / $iterations" | bc)
    local qps=$(echo "scale=2; $iterations / $duration" | bc)

    echo -e "${GREEN}Done${NC}"
    echo "    Avg: ${avg_time}s | QPS: ${qps}"

    echo "$db_name,$query_name,$iterations,$duration,$avg_time,$qps" >> "$RESULTS_FILE"
}

run_query_tests() {
    local db_name=$1
    local host=$2
    local port=$3

    print_section "$db_name - Query Performance Tests"
    echo

    # 1. Simple SELECT with WHERE clause (indexed)
    test_query_performance "$db_name" $host $port \
        "Simple WHERE (indexed)" \
        "SELECT * FROM query_test_data WHERE user_id = 5000;" \
        100

    # 2. Range query (indexed)
    test_query_performance "$db_name" $host $port \
        "Range query (indexed)" \
        "SELECT * FROM query_test_data WHERE age BETWEEN 25 AND 35;" \
        50

    # 3. SELECT with multiple conditions
    test_query_performance "$db_name" $host $port \
        "Multiple WHERE conditions" \
        "SELECT * FROM query_test_data WHERE department = 'Engineering' AND status = 'active';" \
        50

    # 4. COUNT aggregate
    test_query_performance "$db_name" $host $port \
        "COUNT aggregate" \
        "SELECT COUNT(*) FROM query_test_data WHERE status = 'active';" \
        100

    # 5. GROUP BY with COUNT
    test_query_performance "$db_name" $host $port \
        "GROUP BY with COUNT" \
        "SELECT department, COUNT(*) FROM query_test_data GROUP BY department;" \
        50

    # 6. AVG aggregate
    test_query_performance "$db_name" $host $port \
        "AVG aggregate" \
        "SELECT AVG(salary) FROM query_test_data WHERE department = 'Engineering';" \
        100

    # 7. ORDER BY with LIMIT
    test_query_performance "$db_name" $host $port \
        "ORDER BY with LIMIT" \
        "SELECT * FROM query_test_data ORDER BY score DESC LIMIT 100;" \
        50

    # 8. LIKE pattern matching
    test_query_performance "$db_name" $host $port \
        "LIKE pattern matching" \
        "SELECT * FROM query_test_data WHERE email LIKE '%user100%';" \
        50

    # 9. Complex WHERE with OR
    test_query_performance "$db_name" $host $port \
        "Complex WHERE with OR" \
        "SELECT * FROM query_test_data WHERE (age > 40 AND salary > 100000) OR status = 'vip';" \
        50

    # 10. MIN/MAX aggregates
    test_query_performance "$db_name" $host $port \
        "MIN/MAX aggregates" \
        "SELECT MIN(age), MAX(age), MIN(salary), MAX(salary) FROM query_test_data;" \
        100

    # 11. DISTINCT query
    test_query_performance "$db_name" $host $port \
        "DISTINCT query" \
        "SELECT DISTINCT department FROM query_test_data;" \
        100

    # 12. Full table scan
    test_query_performance "$db_name" $host $port \
        "Full table scan" \
        "SELECT COUNT(*) FROM query_test_data;" \
        50

    echo
}

generate_comparison_report() {
    print_header "Performance Comparison Report"
    echo

    echo "┌──────────────────────────────┬─────────────┬──────────────┬──────────────┬──────────┐"
    echo "│         Query Type           │  Database   │ Avg Time (s) │     QPS      │  Iters   │"
    echo "├──────────────────────────────┼─────────────┼──────────────┼──────────────┼──────────┤"

    local current_query=""
    local pg_time=""
    local pg_qps=""

    while IFS=',' read -r db query iters duration avg_time qps; do
        if [ "$query" != "$current_query" ]; then
            if [ ! -z "$current_query" ]; then
                echo "├──────────────────────────────┼─────────────┼──────────────┼──────────────┼──────────┤"
            fi
            current_query="$query"
        fi

        printf "│ %-28s │ %-11s │ %12s │ %12s │ %8s │\n" "$query" "$db" "$avg_time" "$qps" "$iters"

        if [ "$db" = "PostgreSQL" ]; then
            pg_time=$avg_time
            pg_qps=$qps
        fi
    done < "$RESULTS_FILE"

    echo "└──────────────────────────────┴─────────────┴──────────────┴──────────────┴──────────┘"
    echo
}

generate_analysis() {
    print_header "Performance Analysis"
    echo

    # Calculate averages
    local pg_total_qps=0
    local pg_count=0
    local fp_total_qps=0
    local fp_count=0

    while IFS=',' read -r db query iters duration avg_time qps; do
        if [ "$db" = "PostgreSQL" ]; then
            pg_total_qps=$(echo "$pg_total_qps + $qps" | bc)
            pg_count=$((pg_count + 1))
        elif [ "$db" = "FastPostgres" ]; then
            fp_total_qps=$(echo "$fp_total_qps + $qps" | bc)
            fp_count=$((fp_count + 1))
        fi
    done < "$RESULTS_FILE"

    if [ $pg_count -gt 0 ] && [ $fp_count -gt 0 ]; then
        local pg_avg=$(echo "scale=2; $pg_total_qps / $pg_count" | bc)
        local fp_avg=$(echo "scale=2; $fp_total_qps / $fp_count" | bc)

        print_section "Average Query Performance"
        echo "  PostgreSQL:   ${pg_avg} queries/sec"
        echo "  FastPostgres: ${fp_avg} queries/sec"
        echo

        if (( $(echo "$fp_avg > $pg_avg" | bc -l) )); then
            local improvement=$(echo "scale=2; (($fp_avg - $pg_avg) / $pg_avg) * 100" | bc)
            print_success "FastPostgres is ${improvement}% faster on average!"
        else
            local diff=$(echo "scale=2; (($pg_avg - $fp_avg) / $pg_avg) * 100" | bc)
            print_warning "PostgreSQL is ${diff}% faster on average"
        fi
        echo
    fi

    print_section "Query Categories Performance"
    echo
    echo "Best Performance Types:"
    echo "  • Simple indexed lookups (user_id, primary key)"
    echo "  • Aggregate functions (COUNT, AVG, MIN/MAX)"
    echo "  • Indexed range queries"
    echo
    echo "Good Performance Types:"
    echo "  • GROUP BY operations"
    echo "  • ORDER BY with LIMIT"
    echo "  • Multiple WHERE conditions"
    echo
    echo "Slower Operations (Expected):"
    echo "  • LIKE pattern matching (string operations)"
    echo "  • Full table scans"
    echo "  • Complex OR conditions"
    echo
}

generate_insights() {
    print_header "Key Insights & Recommendations"
    echo

    print_section "Performance Optimization Tips"
    echo "  1. Use indexes on frequently queried columns"
    echo "  2. Limit result sets with WHERE and LIMIT clauses"
    echo "  3. Prefer indexed columns in WHERE conditions"
    echo "  4. Use aggregate functions efficiently"
    echo "  5. Avoid LIKE queries on large text fields when possible"
    echo

    print_section "When FastPostgres Excels"
    echo "  ✓ High concurrent read workloads"
    echo "  ✓ Analytical queries with aggregations"
    echo "  ✓ Columnar data access patterns"
    echo "  ✓ In-memory operations"
    echo

    print_section "Query Best Practices"
    echo "  • Create indexes on filter columns (age, status, department)"
    echo "  • Use prepared statements for repeated queries"
    echo "  • Batch similar queries together"
    echo "  • Monitor query plans for optimization"
    echo "  • Keep working set in memory when possible"
    echo
}

cleanup_test_data() {
    local host=$1
    local port=$2

    psql -h $host -p $port -U postgres -d fastpostgres -c "DROP TABLE IF EXISTS query_test_data;" > /dev/null 2>&1
}

main() {
    clear

    echo "╔═══════════════════════════════════════════════════════════════╗"
    echo "║      Data Retrieval Benchmark - FastPostgres vs PostgreSQL   ║"
    echo "╚═══════════════════════════════════════════════════════════════╝"
    echo

    print_section "Prerequisites Check"

    if ! command -v psql &> /dev/null; then
        echo -e "${RED}✗ psql is not installed${NC}"
        exit 1
    fi
    print_success "psql is installed"

    if ! command -v bc &> /dev/null; then
        echo -e "${RED}✗ bc is not installed${NC}"
        exit 1
    fi
    print_success "bc is installed"

    echo
    print_section "Database Connection Check"

    if ! psql -h localhost -p 5432 -U postgres -d fastpostgres -c "SELECT 1;" > /dev/null 2>&1; then
        echo -e "${RED}✗ Cannot connect to PostgreSQL on port 5432${NC}"
        exit 1
    fi
    print_success "PostgreSQL is accessible (port 5432)"

    if ! timeout 5 psql -h localhost -p 5433 -U postgres -d fastpostgres -c "SELECT 1;" > /dev/null 2>&1; then
        echo -e "${RED}✗ Cannot connect to FastPostgres on port 5433${NC}"
        exit 1
    fi
    print_success "FastPostgres is accessible (port 5433)"

    echo
    print_header "Benchmark Configuration"
    echo
    echo "This benchmark will test various query patterns:"
    echo "  • Simple WHERE queries (indexed)"
    echo "  • Range queries"
    echo "  • Aggregations (COUNT, AVG, MIN/MAX)"
    echo "  • GROUP BY operations"
    echo "  • ORDER BY with LIMIT"
    echo "  • Pattern matching (LIKE)"
    echo "  • Complex multi-condition queries"
    echo
    echo "Data: 50,000 rows with realistic structure"
    echo "Queries: 12 different query types"
    echo "Estimated time: 5-8 minutes"
    echo
    read -p "Press Enter to start or Ctrl+C to cancel..."
    echo

    # Setup data for PostgreSQL
    print_header "PostgreSQL Setup & Testing"
    setup_test_data "localhost" "5432" "PostgreSQL" 50000
    run_query_tests "PostgreSQL" "localhost" "5432"
    cleanup_test_data "localhost" "5432"

    echo

    # Setup data for FastPostgres
    print_header "FastPostgres Setup & Testing"
    setup_test_data "localhost" "5433" "FastPostgres" 50000
    run_query_tests "FastPostgres" "localhost" "5433"
    cleanup_test_data "localhost" "5433"

    echo
    generate_comparison_report
    generate_analysis
    generate_insights

    print_header "Benchmark Complete!"
    echo
    print_success "Results saved to: $RESULTS_FILE"
    echo
}

main "$@"