#!/bin/bash

set -e

echo "🚀 100K Data Insertion Benchmark: FastPostgres vs PostgreSQL"
echo "============================================================="
echo

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# Test configurations
TOTAL_ROWS=100000
BATCH_SIZE=1000
FASTPG_PORT=5433
POSTGRES_PORT=5432

print_header() {
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_info() {
    echo -e "${CYAN}ℹ️  $1${NC}"
}

format_number() {
    local num=$1
    printf "%'d" $num 2>/dev/null || echo $num
}

setup_test_table() {
    local host=$1
    local port=$2
    local db_name=$3

    echo "Setting up test table in $db_name..."

    if [ "$db_name" = "PostgreSQL" ]; then
        psql -h $host -p $port -U postgres -d testdb > /dev/null 2>&1 << 'EOF'
DROP TABLE IF EXISTS insertion_benchmark;
CREATE TABLE insertion_benchmark (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    username VARCHAR(50),
    email VARCHAR(100),
    first_name VARCHAR(30),
    last_name VARCHAR(30),
    age INTEGER,
    balance DECIMAL(10,2),
    status VARCHAR(20),
    country VARCHAR(30),
    city VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
EOF
    else
        psql -h $host -p $port -U postgres -d fastpostgres > /dev/null 2>&1 << 'EOF'
DROP TABLE IF EXISTS insertion_benchmark;
CREATE TABLE insertion_benchmark (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    username VARCHAR(50),
    email VARCHAR(100),
    first_name VARCHAR(30),
    last_name VARCHAR(30),
    age INTEGER,
    balance INTEGER,
    status VARCHAR(20),
    country VARCHAR(30),
    city VARCHAR(50),
    created_at INTEGER
);
EOF
    fi

    if [ $? -eq 0 ]; then
        print_success "Test table created in $db_name"
    else
        echo -e "${RED}❌ Failed to create test table in $db_name${NC}"
        return 1
    fi
}

generate_batch_data() {
    local batch_size=$1
    local start_id=$2
    local db_type=$3

    local countries=("USA" "UK" "Canada" "Germany" "France" "Japan" "Australia" "Brazil" "India" "China")
    local statuses=("active" "inactive" "pending" "verified" "suspended")

    local values=""
    for ((i=0; i<batch_size; i++)); do
        local user_id=$((start_id + i))
        local country=${countries[$((RANDOM % 10))]}
        local status=${statuses[$((RANDOM % 5))]}
        local age=$((18 + RANDOM % 70))
        local balance=$((RANDOM % 50000))
        local timestamp=$((1640995200 + RANDOM % 31536000)) # Random timestamp in 2022

        if [ "$values" != "" ]; then
            values="$values, "
        fi

        if [ "$db_type" = "PostgreSQL" ]; then
            values="$values($user_id, 'user_$user_id', 'user$user_id@test.com', 'First_$user_id', 'Last_$user_id', $age, $balance.99, '$status', '$country', 'City_$user_id')"
        else
            values="$values($user_id, $user_id, 'user_$user_id', 'user$user_id@test.com', 'First_$user_id', 'Last_$user_id', $age, $balance, '$status', '$country', 'City_$user_id', $timestamp)"
        fi
    done

    if [ "$db_type" = "PostgreSQL" ]; then
        echo "INSERT INTO insertion_benchmark (user_id, username, email, first_name, last_name, age, balance, status, country, city) VALUES $values;"
    else
        echo "INSERT INTO insertion_benchmark (id, user_id, username, email, first_name, last_name, age, balance, status, country, city, created_at) VALUES $values;"
    fi
}

run_insertion_benchmark() {
    local db_name=$1
    local host=$2
    local port=$3
    local database=$4

    print_header "$db_name Insertion Performance Test"
    echo

    print_info "Target: $(format_number $TOTAL_ROWS) rows with batch size $(format_number $BATCH_SIZE)"

    setup_test_table $host $port $db_name

    echo "Starting insertion benchmark..."
    local start_time=$(date +%s.%N)

    local inserted=0
    local batch_count=0
    local failed_batches=0

    # Progress tracking
    local progress_update=$((TOTAL_ROWS / 20)) # Update every 5%
    if [ $progress_update -lt 1000 ]; then
        progress_update=1000
    fi

    for ((current_id=1; current_id<=TOTAL_ROWS; current_id+=BATCH_SIZE)); do
        local actual_batch_size=$BATCH_SIZE
        if [ $((current_id + BATCH_SIZE - 1)) -gt $TOTAL_ROWS ]; then
            actual_batch_size=$((TOTAL_ROWS - current_id + 1))
        fi

        # Generate batch INSERT statement
        local sql=$(generate_batch_data $actual_batch_size $current_id $db_name)

        # Execute the batch insert
        if psql -h $host -p $port -U postgres -d $database -c "$sql" > /dev/null 2>&1; then
            inserted=$((inserted + actual_batch_size))
            batch_count=$((batch_count + 1))

            # Progress indicator
            if [ $((inserted % progress_update)) -eq 0 ] || [ $inserted -eq $TOTAL_ROWS ]; then
                local percent=$((inserted * 100 / TOTAL_ROWS))
                printf "\r  Progress: %3d%% (%s / %s rows)" $percent $(format_number $inserted) $(format_number $TOTAL_ROWS)
            fi
        else
            failed_batches=$((failed_batches + 1))
            echo -e "\n${YELLOW}⚠️  Batch failed at row $current_id${NC}"
        fi
    done

    echo "" # New line after progress

    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc)
    local rows_per_sec=$(echo "scale=0; $inserted / $duration" | bc 2>/dev/null || echo "0")

    # Verify actual count
    local actual_count
    if [ "$db_name" = "PostgreSQL" ]; then
        actual_count=$(psql -h $host -p $port -U postgres -d testdb -t -c "SELECT COUNT(*) FROM insertion_benchmark;" 2>/dev/null | tr -d ' ' | head -1)
    else
        actual_count=$(psql -h $host -p $port -U postgres -d fastpostgres -t -c "SELECT COUNT(*) FROM insertion_benchmark;" 2>/dev/null | tr -d ' ' | head -1)
    fi

    # Results summary
    echo
    print_success "$db_name Results:"
    echo "  • Total rows inserted: $(format_number ${actual_count:-$inserted})"
    echo "  • Duration: ${duration}s"
    echo "  • Throughput: $(format_number $rows_per_sec) rows/sec"
    echo "  • Successful batches: $(format_number $batch_count)"
    if [ $failed_batches -gt 0 ]; then
        echo -e "  • Failed batches: ${RED}$(format_number $failed_batches)${NC}"
    fi
    echo "  • Average batch time: $(echo "scale=4; $duration / $batch_count" | bc)s"
    echo

    # Store results for comparison
    echo "$db_name,$actual_count,$duration,$rows_per_sec,$batch_count,$failed_batches" >> /tmp/insertion_comparison.csv
}

generate_comparison() {
    print_header "Performance Comparison Analysis"
    echo

    if [[ ! -f /tmp/insertion_comparison.csv ]]; then
        echo "❌ No results found for comparison"
        return
    fi

    echo "Detailed Results:"
    echo "┌─────────────┬─────────────┬───────────┬────────────┬──────────┬─────────┐"
    echo "│  Database   │    Rows     │ Duration  │  Rows/sec  │ Batches  │ Failed  │"
    echo "├─────────────┼─────────────┼───────────┼────────────┼──────────┼─────────┤"

    while IFS=',' read -r db rows duration rate batches failed; do
        local formatted_rows=$(format_number $rows)
        local formatted_rate=$(format_number $rate)
        local formatted_batches=$(format_number $batches)
        local formatted_duration=$(printf "%.3f" $duration)

        printf "│ %-11s │ %11s │ %8ss │ %10s │ %8s │ %7s │\n" "$db" "$formatted_rows" "$formatted_duration" "$formatted_rate" "$formatted_batches" "$failed"
    done < /tmp/insertion_comparison.csv

    echo "└─────────────┴─────────────┴───────────┴────────────┴──────────┴─────────┘"
    echo

    # Performance analysis
    local pg_rate=""
    local fp_rate=""
    local pg_duration=""
    local fp_duration=""

    while IFS=',' read -r db rows duration rate batches failed; do
        if [ "$db" = "PostgreSQL" ]; then
            pg_rate=$rate
            pg_duration=$duration
        elif [ "$db" = "FastPostgres" ]; then
            fp_rate=$rate
            fp_duration=$duration
        fi
    done < /tmp/insertion_comparison.csv

    if [[ -n "$pg_rate" && -n "$fp_rate" ]]; then
        echo "Performance Analysis:"
        echo "─────────────────────"

        if (( $(echo "$fp_rate > $pg_rate" | bc -l) )); then
            local speedup=$(echo "scale=2; $fp_rate / $pg_rate" | bc)
            local improvement=$(echo "scale=1; (($fp_rate - $pg_rate) / $pg_rate) * 100" | bc)
            print_success "FastPostgres is ${improvement}% faster (${speedup}x speedup)"
        elif (( $(echo "$pg_rate > $fp_rate" | bc -l) )); then
            local speedup=$(echo "scale=2; $pg_rate / $fp_rate" | bc)
            local improvement=$(echo "scale=1; (($pg_rate - $fp_rate) / $fp_rate) * 100" | bc)
            echo -e "${YELLOW}PostgreSQL is ${improvement}% faster (${speedup}x speedup)${NC}"
        else
            echo "Both databases show similar performance"
        fi

        echo
        echo "Key Insights:"
        echo "• Batch size of $(format_number $BATCH_SIZE) provides good balance of speed and memory usage"
        echo "• Both databases handle $(format_number $TOTAL_ROWS) row insertions efficiently"
        echo "• Network latency is minimized with batched operations"

        if (( $(echo "$fp_rate > 20000" | bc -l) )) || (( $(echo "$pg_rate > 20000" | bc -l) )); then
            echo "• High throughput achieved (>20K rows/sec)"
        fi

        echo
        echo "Recommendations:"
        echo "• For bulk loading, use batches of 500-2000 rows"
        echo "• Monitor memory usage with larger batch sizes"
        echo "• Consider parallel inserts for multi-million row scenarios"
        echo "• Disable indexes during bulk load, rebuild after (if applicable)"
    fi
}

main() {
    clear
    echo "╔════════════════════════════════════════════════════════════════╗"
    echo "║              100K Row Insertion Performance Test               ║"
    echo "╚════════════════════════════════════════════════════════════════╝"
    echo

    # Prerequisites check
    print_info "Checking prerequisites..."

    if ! command -v psql &> /dev/null; then
        echo -e "${RED}❌ psql is not installed${NC}"
        exit 1
    fi

    if ! command -v bc &> /dev/null; then
        echo -e "${RED}❌ bc is not installed${NC}"
        exit 1
    fi

    print_success "Prerequisites OK"
    echo

    # Database connectivity check
    print_info "Checking database connectivity..."

    if ! psql -h localhost -p $POSTGRES_PORT -U postgres -d testdb -c "SELECT 1;" > /dev/null 2>&1; then
        echo -e "${RED}❌ Cannot connect to PostgreSQL on port $POSTGRES_PORT${NC}"
        exit 1
    fi
    print_success "PostgreSQL connection OK"

    if ! timeout 10 psql -h localhost -p $FASTPG_PORT -U postgres -d fastpostgres -c "SELECT 1;" > /dev/null 2>&1; then
        echo -e "${RED}❌ Cannot connect to FastPostgres on port $FASTPG_PORT${NC}"
        exit 1
    fi
    print_success "FastPostgres connection OK"

    echo

    # Initialize results file
    echo "Database,Rows,Duration,Rows_per_sec,Batches,Failed" > /tmp/insertion_comparison.csv

    print_info "Starting benchmark with $(format_number $TOTAL_ROWS) rows in batches of $(format_number $BATCH_SIZE)"
    echo

    # Run benchmarks
    run_insertion_benchmark "PostgreSQL" "localhost" $POSTGRES_PORT "testdb"
    run_insertion_benchmark "FastPostgres" "localhost" $FASTPG_PORT "fastpostgres"

    # Generate comparison report
    generate_comparison

    # Cleanup
    rm -f /tmp/insertion_comparison.csv

    print_header "Benchmark Complete!"
    print_success "100K insertion benchmark finished successfully!"
    echo
}

main "$@"