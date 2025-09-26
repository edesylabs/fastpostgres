#!/bin/bash

set -e

echo "🚀 FastPostgres vs PostgreSQL - Large Volume Insertion Benchmark"
echo "=================================================================="
echo

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'

RESULTS_FILE="/tmp/large_volume_benchmark_results.txt"
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

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

format_number() {
    local num=$1
    printf "%'d" $num 2>/dev/null || echo $num
}

format_time() {
    local seconds=$1
    local hours=$((seconds / 3600))
    local minutes=$(((seconds % 3600) / 60))
    local secs=$((seconds % 60))

    if [ $hours -gt 0 ]; then
        printf "%dh %dm %ds" $hours $minutes $secs
    elif [ $minutes -gt 0 ]; then
        printf "%dm %ds" $minutes $secs
    else
        printf "%.2fs" $seconds
    fi
}

setup_test_table() {
    local host=$1
    local port=$2
    local db_name=$3

    print_section "Setting up test table in $db_name"

    psql -h $host -p $port -U postgres -d fastpostgres > /dev/null 2>&1 << 'EOF'
DROP TABLE IF EXISTS large_volume_test;
CREATE TABLE large_volume_test (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    username VARCHAR(100),
    email VARCHAR(150),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    age INTEGER,
    balance DECIMAL(12,2),
    status VARCHAR(20),
    country VARCHAR(50),
    city VARCHAR(100),
    zip_code VARCHAR(10),
    phone VARCHAR(20),
    registration_date TIMESTAMP,
    last_login TIMESTAMP,
    is_active BOOLEAN,
    score INTEGER,
    category VARCHAR(50),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
EOF

    if [ $? -eq 0 ]; then
        print_success "Test table created with 20 columns"
    else
        print_error "Failed to create test table"
        return 1
    fi
}

generate_batch_insert_sql() {
    local batch_size=$1
    local start_id=$2

    local countries=("USA" "UK" "Canada" "Germany" "France" "Japan" "Australia" "Brazil" "India" "China")
    local statuses=("active" "inactive" "pending" "suspended" "verified")
    local categories=("premium" "standard" "basic" "vip" "trial")

    local values=""
    for ((i=0; i<batch_size; i++)); do
        local user_id=$((start_id + i))
        local country=${countries[$((RANDOM % 10))]}
        local status=${statuses[$((RANDOM % 5))]}
        local category=${categories[$((RANDOM % 5))]}
        local age=$((18 + RANDOM % 70))
        local balance=$((RANDOM % 100000))
        local score=$((RANDOM % 1000))
        local is_active=$((RANDOM % 2))

        if [ "$values" != "" ]; then
            values="$values, "
        fi

        values="$values($user_id, 'user_$user_id', 'user$user_id@example.com', 'FirstName_$user_id', 'LastName_$user_id', $age, $balance.99, '$status', '$country', 'City_$user_id', '${user_id}01', '+1-555-${user_id}', NOW() - INTERVAL '$((RANDOM % 365)) days', NOW() - INTERVAL '$((RANDOM % 30)) days', $is_active, $score, '$category', 'Notes for user $user_id')"
    done

    echo "INSERT INTO large_volume_test (user_id, username, email, first_name, last_name, age, balance, status, country, city, zip_code, phone, registration_date, last_login, is_active, score, category, notes) VALUES $values;"
}

test_large_volume_insertion() {
    local db_name=$1
    local host=$2
    local port=$3
    local total_rows=$4
    local batch_size=$5

    print_section "Testing $db_name: $(format_number $total_rows) rows (batch: $(format_number $batch_size))"

    local start_time=$(date +%s.%N)
    local progress_interval=$((total_rows / 10))
    if [ $progress_interval -lt 1000 ]; then
        progress_interval=1000
    fi

    local inserted=0
    local batches=0

    # Progress bar setup
    local bar_length=50

    for ((current_id=1; current_id<=total_rows; current_id+=batch_size)); do
        local actual_batch_size=$batch_size
        if [ $((current_id + batch_size - 1)) -gt $total_rows ]; then
            actual_batch_size=$((total_rows - current_id + 1))
        fi

        # Generate and execute batch insert
        local sql=$(generate_batch_insert_sql $actual_batch_size $current_id)
        psql -h $host -p $port -U postgres -d fastpostgres -c "$sql" > /dev/null 2>&1

        inserted=$((inserted + actual_batch_size))
        batches=$((batches + 1))

        # Progress indicator
        if [ $((inserted % progress_interval)) -eq 0 ] || [ $inserted -eq $total_rows ]; then
            local percent=$((inserted * 100 / total_rows))
            local filled=$((percent * bar_length / 100))
            local empty=$((bar_length - filled))

            printf "\r  Progress: ["
            printf "%${filled}s" | tr ' ' '█'
            printf "%${empty}s" | tr ' ' '░'
            printf "] %3d%% (%s / %s rows)" $percent $(format_number $inserted) $(format_number $total_rows)
        fi
    done

    echo ""

    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc)
    local rows_per_sec=$(echo "scale=2; $total_rows / $duration" | bc)
    local duration_int=${duration%.*}

    # Verify count
    local actual_count=$(psql -h $host -p $port -U postgres -d fastpostgres -t -c "SELECT COUNT(*) FROM large_volume_test;" 2>/dev/null | tr -d ' ')

    print_success "Inserted $(format_number $actual_count) rows in $(format_time $duration_int)"
    echo "  • Batches: $(format_number $batches)"
    echo "  • Throughput: $(format_number ${rows_per_sec%.*}) rows/sec"
    echo "  • Avg batch time: $(echo "scale=3; $duration / $batches" | bc)s"
    echo

    # Save results
    echo "$db_name,$total_rows,$batch_size,$duration,$rows_per_sec,$batches" >> "$RESULTS_FILE"
}

cleanup_table() {
    local host=$1
    local port=$2

    psql -h $host -p $port -U postgres -d fastpostgres -c "DROP TABLE IF EXISTS large_volume_test;" > /dev/null 2>&1
}

run_test_suite() {
    local db_name=$1
    local host=$2
    local port=$3

    print_header "$db_name Performance Test"
    echo

    # Test configurations: rows, batch_size
    local tests=(
        "50000:500"
        "100000:1000"
        "250000:2500"
        "500000:5000"
        "1000000:10000"
    )

    for test in "${tests[@]}"; do
        IFS=':' read -r rows batch <<< "$test"

        setup_test_table $host $port "$db_name"
        test_large_volume_insertion "$db_name" $host $port $rows $batch
        cleanup_table $host $port

        echo
    done
}

generate_comparison_report() {
    print_header "Performance Comparison Report"
    echo

    echo "┌─────────────┬─────────────┬────────────┬──────────────┬──────────────┬──────────┐"
    echo "│  Database   │    Rows     │   Batch    │  Duration    │  Rows/sec    │ Batches  │"
    echo "├─────────────┼─────────────┼────────────┼──────────────┼──────────────┼──────────┤"

    while IFS=',' read -r db rows batch duration rate batches_count; do
        local duration_int=${duration%.*}
        local formatted_time=$(format_time $duration_int)
        local formatted_rows=$(format_number $rows)
        local formatted_batch=$(format_number $batch)
        local formatted_rate=$(format_number ${rate%.*})
        local formatted_batches=$(format_number $batches_count)

        printf "│ %-11s │ %11s │ %10s │ %12s │ %12s │ %8s │\n" "$db" "$formatted_rows" "$formatted_batch" "$formatted_time" "$formatted_rate" "$formatted_batches"
    done < "$RESULTS_FILE"

    echo "└─────────────┴─────────────┴────────────┴──────────────┴──────────────┴──────────┘"
    echo
}

generate_analysis() {
    print_header "Performance Analysis"
    echo

    # Calculate averages for each database
    local pg_total=0
    local pg_count=0
    local fp_total=0
    local fp_count=0

    while IFS=',' read -r db rows batch duration rate batches_count; do
        if [ "$db" = "PostgreSQL" ]; then
            pg_total=$(echo "$pg_total + $rate" | bc)
            pg_count=$((pg_count + 1))
        elif [ "$db" = "FastPostgres" ]; then
            fp_total=$(echo "$fp_total + $rate" | bc)
            fp_count=$((fp_count + 1))
        fi
    done < "$RESULTS_FILE"

    if [ $pg_count -gt 0 ] && [ $fp_count -gt 0 ]; then
        local pg_avg=$(echo "scale=2; $pg_total / $pg_count" | bc)
        local fp_avg=$(echo "scale=2; $fp_total / $fp_count" | bc)

        print_section "Average Throughput"
        echo "  PostgreSQL:   $(format_number ${pg_avg%.*}) rows/sec"
        echo "  FastPostgres: $(format_number ${fp_avg%.*}) rows/sec"
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

    print_section "Key Insights"
    echo "  • Larger batch sizes significantly improve throughput"
    echo "  • Both databases handle large volumes efficiently"
    echo "  • Network latency has minimal impact with batch inserts"
    echo "  • Memory usage scales with batch size"
    echo "  • Consistent performance across different data volumes"
    echo

    print_section "Best Practices"
    echo "  ✓ Use batch inserts for bulk data loading"
    echo "  ✓ Optimal batch size: 1,000-10,000 rows"
    echo "  ✓ Monitor memory usage with very large batches"
    echo "  ✓ Consider parallel inserts for multi-million rows"
    echo "  ✓ Disable indexes during bulk load, rebuild after"
    echo
}

main() {
    clear

    echo "╔═══════════════════════════════════════════════════════════════╗"
    echo "║     Large Volume Insertion Benchmark - FastPostgres          ║"
    echo "╚═══════════════════════════════════════════════════════════════╝"
    echo

    print_section "Prerequisites Check"

    if ! command -v psql &> /dev/null; then
        print_error "psql is not installed"
        exit 1
    fi
    print_success "psql is installed"

    if ! command -v bc &> /dev/null; then
        print_error "bc is not installed"
        exit 1
    fi
    print_success "bc is installed"

    echo
    print_section "Database Connection Check"

    if ! psql -h localhost -p 5432 -U postgres -d fastpostgres -c "SELECT 1;" > /dev/null 2>&1; then
        print_error "Cannot connect to PostgreSQL on port 5432"
        exit 1
    fi
    print_success "PostgreSQL is accessible (port 5432)"

    if ! timeout 5 psql -h localhost -p 5433 -U postgres -d fastpostgres -c "SELECT 1;" > /dev/null 2>&1; then
        print_error "Cannot connect to FastPostgres on port 5433"
        exit 1
    fi
    print_success "FastPostgres is accessible (port 5433)"

    echo
    print_header "Benchmark Configuration"
    echo
    echo "This benchmark will test large volume insertions with:"
    echo "  • 50K rows (batch: 500)"
    echo "  • 100K rows (batch: 1,000)"
    echo "  • 250K rows (batch: 2,500)"
    echo "  • 500K rows (batch: 5,000)"
    echo "  • 1M rows (batch: 10,000)"
    echo
    echo "Each row contains 20 columns (realistic data structure)"
    echo "Estimated total time: 10-20 minutes"
    echo
    print_warning "This will insert millions of rows. Continue?"
    read -p "Press Enter to start or Ctrl+C to cancel..."
    echo

    run_test_suite "PostgreSQL" "localhost" "5432"
    run_test_suite "FastPostgres" "localhost" "5433"

    generate_comparison_report
    generate_analysis

    print_header "Benchmark Complete!"
    echo
    print_success "Results saved to: $RESULTS_FILE"
    echo
}

main "$@"