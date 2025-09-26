#!/bin/bash

set -e

echo "📊 FastPostgres vs PostgreSQL - Incremental Insertion Benchmark"
echo "================================================================"
echo

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

RESULTS_FILE="/tmp/insertion_benchmark_results.txt"
> "$RESULTS_FILE"

print_header() {
    echo -e "${BLUE}$1${NC}"
    echo "────────────────────────────────────────────────────────────────"
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

setup_test_table() {
    local host=$1
    local port=$2
    local db_name=$3

    echo "Setting up test table in $db_name..."

    psql -h $host -p $port -U postgres -d fastpostgres > /dev/null 2>&1 << EOF
DROP TABLE IF EXISTS insertion_benchmark;
CREATE TABLE insertion_benchmark (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    age INTEGER,
    score DECIMAL(10,2),
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
EOF

    if [ $? -eq 0 ]; then
        print_success "Test table created successfully"
    else
        print_error "Failed to create test table in $db_name"
        return 1
    fi
}

test_insertion_performance() {
    local host=$1
    local port=$2
    local db_name=$3
    local row_count=$4
    local batch_size=${5:-1}

    echo -n "Inserting $row_count rows (batch size: $batch_size)... "

    start_time=$(date +%s.%N)

    if [ $batch_size -eq 1 ]; then
        for i in $(seq 1 $row_count); do
            psql -h $host -p $port -U postgres -d fastpostgres > /dev/null 2>&1 << EOF
INSERT INTO insertion_benchmark (name, email, age, score, status)
VALUES ('User_$i', 'user$i@example.com', $((20 + RANDOM % 50)), $((RANDOM % 100)).99, 'active');
EOF
        done
    else
        for batch_start in $(seq 1 $batch_size $row_count); do
            batch_end=$((batch_start + batch_size - 1))
            if [ $batch_end -gt $row_count ]; then
                batch_end=$row_count
            fi

            values=""
            for i in $(seq $batch_start $batch_end); do
                if [ "$values" != "" ]; then
                    values="$values, "
                fi
                values="$values('User_$i', 'user$i@example.com', $((20 + RANDOM % 50)), $((RANDOM % 100)).99, 'active')"
            done

            psql -h $host -p $port -U postgres -d fastpostgres > /dev/null 2>&1 << EOF
INSERT INTO insertion_benchmark (name, email, age, score, status) VALUES $values;
EOF
        done
    fi

    end_time=$(date +%s.%N)
    duration=$(echo "$end_time - $start_time" | bc)
    rows_per_sec=$(echo "scale=2; $row_count / $duration" | bc)

    echo -e "${GREEN}Done!${NC}"
    echo "  Duration: ${duration}s"
    echo "  Rate: ${rows_per_sec} rows/sec"

    echo "$db_name,$row_count,$batch_size,$duration,$rows_per_sec" >> "$RESULTS_FILE"

    return 0
}

verify_insertion() {
    local host=$1
    local port=$2
    local expected_count=$3

    actual_count=$(psql -h $host -p $port -U postgres -d fastpostgres -t -c "SELECT COUNT(*) FROM insertion_benchmark;" 2>/dev/null | tr -d ' ')

    if [ "$actual_count" -eq "$expected_count" ]; then
        print_success "Verified: $actual_count rows inserted"
        return 0
    else
        print_error "Expected $expected_count rows, found $actual_count"
        return 1
    fi
}

run_incremental_test() {
    local db_name=$1
    local host=$2
    local port=$3

    print_header "Testing $db_name on port $port"

    setup_test_table $host $port "$db_name"
    echo

    test_sizes=(10 50 100 500 1000 5000 10000)

    echo "Running incremental insertion tests..."
    echo

    for size in "${test_sizes[@]}"; do
        echo -e "${YELLOW}Test: $size rows (single inserts)${NC}"

        psql -h $host -p $port -U postgres -d fastpostgres -c "TRUNCATE TABLE insertion_benchmark;" > /dev/null 2>&1

        test_insertion_performance $host $port "$db_name" $size 1
        verify_insertion $host $port $size
        echo
    done

    echo -e "${YELLOW}Testing batch insertions...${NC}"
    echo

    batch_tests=(
        "1000:10"
        "1000:100"
        "5000:100"
        "5000:500"
        "10000:100"
        "10000:1000"
    )

    for test in "${batch_tests[@]}"; do
        IFS=':' read -r rows batch <<< "$test"
        echo -e "${YELLOW}Test: $rows rows (batch size: $batch)${NC}"

        psql -h $host -p $port -U postgres -d fastpostgres -c "TRUNCATE TABLE insertion_benchmark;" > /dev/null 2>&1

        test_insertion_performance $host $port "$db_name" $rows $batch
        verify_insertion $host $port $rows
        echo
    done
}

generate_comparison_report() {
    echo
    print_header "📈 Performance Comparison Report"
    echo

    echo "| Database | Rows | Batch | Duration (s) | Rows/sec |"
    echo "|----------|------|-------|--------------|----------|"

    while IFS=',' read -r db rows batch duration rate; do
        printf "| %-8s | %-4s | %-5s | %-12s | %-8s |\n" "$db" "$rows" "$batch" "$duration" "$rate"
    done < "$RESULTS_FILE"

    echo
    echo "Detailed results saved to: $RESULTS_FILE"
}

generate_summary() {
    echo
    print_header "📊 Summary & Insights"
    echo

    pg_avg=$(awk -F',' '$1=="PostgreSQL" {sum+=$5; count++} END {if(count>0) print sum/count; else print 0}' "$RESULTS_FILE")
    fp_avg=$(awk -F',' '$1=="FastPostgres" {sum+=$5; count++} END {if(count>0) print sum/count; else print 0}' "$RESULTS_FILE")

    echo "Average Performance:"
    echo "  PostgreSQL:   $(printf "%.2f" $pg_avg) rows/sec"
    echo "  FastPostgres: $(printf "%.2f" $fp_avg) rows/sec"

    if (( $(echo "$fp_avg > $pg_avg" | bc -l) )); then
        improvement=$(echo "scale=2; (($fp_avg - $pg_avg) / $pg_avg) * 100" | bc)
        print_success "FastPostgres is ${improvement}% faster on average"
    else
        diff=$(echo "scale=2; (($pg_avg - $fp_avg) / $pg_avg) * 100" | bc)
        print_warning "PostgreSQL is ${diff}% faster on average"
    fi

    echo
    echo "Key Findings:"
    echo "  • Single row inserts show the baseline performance"
    echo "  • Batch inserts significantly improve throughput"
    echo "  • Larger batches generally perform better"
    echo "  • Network latency affects single inserts more"
    echo
}

visualize_results() {
    echo
    print_header "📊 Performance Visualization"
    echo

    echo "Single Insert Performance (rows/sec):"
    echo "Row Count  | PostgreSQL | FastPostgres | Difference"
    echo "-----------|------------|--------------|------------"

    for size in 10 50 100 500 1000 5000 10000; do
        pg_rate=$(awk -F',' -v s=$size '$1=="PostgreSQL" && $2==s && $3==1 {print $5}' "$RESULTS_FILE")
        fp_rate=$(awk -F',' -v s=$size '$1=="FastPostgres" && $2==s && $3==1 {print $5}' "$RESULTS_FILE")

        if [ ! -z "$pg_rate" ] && [ ! -z "$fp_rate" ]; then
            diff=$(echo "scale=2; $fp_rate - $pg_rate" | bc)
            printf "%-10s | %10s | %12s | %+10s\n" "$size" "$pg_rate" "$fp_rate" "$diff"
        fi
    done

    echo
    echo "Batch Insert Performance (rows/sec):"
    echo "Rows/Batch | PostgreSQL | FastPostgres | Difference"
    echo "-----------|------------|--------------|------------"

    while IFS=',' read -r db rows batch duration rate; do
        if [ $batch -ne 1 ]; then
            if [ "$db" = "PostgreSQL" ]; then
                pg_rate=$rate
                test_key="${rows}:${batch}"
            fi

            if [ "$db" = "FastPostgres" ]; then
                fp_rate=$rate
                diff=$(echo "scale=2; $fp_rate - $pg_rate" | bc)
                printf "%-10s | %10s | %12s | %+10s\n" "$test_key" "$pg_rate" "$fp_rate" "$diff"
            fi
        fi
    done < "$RESULTS_FILE"

    echo
}

main() {
    echo "Prerequisites check..."

    if ! command -v psql &> /dev/null; then
        print_error "psql is not installed"
        exit 1
    fi

    if ! command -v bc &> /dev/null; then
        print_error "bc is not installed (required for calculations)"
        exit 1
    fi

    print_success "All prerequisites met"
    echo

    echo "Checking database connections..."

    if ! psql -h localhost -p 5432 -U postgres -d fastpostgres -c "SELECT 1;" > /dev/null 2>&1; then
        print_error "Cannot connect to PostgreSQL on port 5432"
        exit 1
    fi
    print_success "PostgreSQL is accessible"

    if ! timeout 5 psql -h localhost -p 5433 -U postgres -d fastpostgres -c "SELECT 1;" > /dev/null 2>&1; then
        print_error "Cannot connect to FastPostgres on port 5433"
        exit 1
    fi
    print_success "FastPostgres is accessible"
    echo

    echo "╔════════════════════════════════════════════════════════════════╗"
    echo "║           Starting Incremental Insertion Benchmark            ║"
    echo "╚════════════════════════════════════════════════════════════════╝"
    echo
    echo "This benchmark will test insertion performance with:"
    echo "  • Incremental row counts: 10, 50, 100, 500, 1K, 5K, 10K"
    echo "  • Single row inserts (baseline)"
    echo "  • Batch inserts with various batch sizes"
    echo
    read -p "Press Enter to continue or Ctrl+C to cancel..."
    echo

    run_incremental_test "PostgreSQL" "localhost" "5432"
    echo
    echo "═══════════════════════════════════════════════════════════════"
    echo
    run_incremental_test "FastPostgres" "localhost" "5433"

    generate_comparison_report
    visualize_results
    generate_summary

    echo
    print_success "Benchmark completed successfully!"
    echo
}

main "$@"