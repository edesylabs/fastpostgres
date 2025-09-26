#!/bin/bash

set -e

echo "⚡ Connection Stress Test - Finding Maximum Concurrent Connections"
echo "===================================================================="
echo

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'

RESULTS_FILE="/tmp/connection_stress_test_results.txt"
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
    printf "%'d" $1 2>/dev/null || echo $1
}

test_concurrent_connections() {
    local db_name=$1
    local host=$2
    local port=$3
    local target_connections=$4
    local duration=${5:-5}

    echo -e "${CYAN}Testing $db_name: $(format_number $target_connections) concurrent connections${NC}"

    local start_time=$(date +%s.%N)
    local successful=0
    local failed=0
    local pids=()

    # Launch connections
    for ((i=1; i<=target_connections; i++)); do
        {
            if timeout $duration psql -h $host -p $port -U postgres -d fastpostgres -c "SELECT 1, pg_sleep(${duration});" > /dev/null 2>&1; then
                echo "1" > /tmp/conn_success_${db_name}_$i
            else
                echo "0" > /tmp/conn_success_${db_name}_$i
            fi
        } &
        pids+=($!)

        # Progress indicator every 100 connections
        if [ $((i % 100)) -eq 0 ]; then
            echo -ne "\r  Launched: $(format_number $i) / $(format_number $target_connections)"
        fi
    done

    echo -ne "\r  Launched: $(format_number $target_connections) / $(format_number $target_connections)"
    echo

    # Wait for all connections to complete
    echo -n "  Waiting for connections to complete... "
    for pid in "${pids[@]}"; do
        wait $pid 2>/dev/null || true
    done
    echo -e "${GREEN}Done${NC}"

    local end_time=$(date +%s.%N)
    local total_time=$(echo "$end_time - $start_time" | bc)

    # Count results
    for ((i=1; i<=target_connections; i++)); do
        if [ -f /tmp/conn_success_${db_name}_$i ]; then
            local result=$(cat /tmp/conn_success_${db_name}_$i)
            if [ "$result" = "1" ]; then
                successful=$((successful + 1))
            else
                failed=$((failed + 1))
            fi
            rm -f /tmp/conn_success_${db_name}_$i
        else
            failed=$((failed + 1))
        fi
    done

    local success_rate=$(echo "scale=2; ($successful * 100) / $target_connections" | bc)

    echo "  Results:"
    echo "    Successful: $(format_number $successful) (${success_rate}%)"
    echo "    Failed:     $(format_number $failed)"
    echo "    Duration:   ${total_time}s"

    echo "$db_name,$target_connections,$successful,$failed,$success_rate,$total_time" >> "$RESULTS_FILE"

    echo
}

find_maximum_connections() {
    local db_name=$1
    local host=$2
    local port=$3

    print_header "Finding Maximum Connections for $db_name"
    echo

    local test_levels=(100 250 500 1000 2500 5000 10000)
    local max_successful=0
    local max_connections_tested=0

    for connections in "${test_levels[@]}"; do
        test_concurrent_connections "$db_name" $host $port $connections 3

        # Get results
        local last_line=$(tail -1 "$RESULTS_FILE")
        IFS=',' read -r db conns success fail rate time <<< "$last_line"

        max_connections_tested=$connections

        # Check if success rate is still good (>90%)
        if (( $(echo "$rate < 90" | bc -l) )); then
            print_warning "Success rate dropped below 90% ($rate%)"
            print_warning "Maximum reliable connections for $db_name: $(format_number $max_successful)"
            break
        fi

        max_successful=$success

        # Safety check - if we're getting too many failures, stop
        if [ $fail -gt $((connections / 5)) ]; then
            print_warning "Too many failures detected"
            break
        fi
    done

    echo
}

generate_comparison_report() {
    print_header "Connection Stress Test Results"
    echo

    echo "┌─────────────┬─────────────┬─────────────┬─────────────┬──────────────┬────────────┐"
    echo "│  Database   │   Target    │ Successful  │   Failed    │ Success Rate │  Duration  │"
    echo "├─────────────┼─────────────┼─────────────┼─────────────┼──────────────┼────────────┤"

    while IFS=',' read -r db target success fail rate time; do
        local formatted_target=$(format_number $target)
        local formatted_success=$(format_number $success)
        local formatted_fail=$(format_number $fail)

        printf "│ %-11s │ %11s │ %11s │ %11s │ %11s%% │ %9.2fs │\n" \
            "$db" "$formatted_target" "$formatted_success" "$formatted_fail" "$rate" "$time"
    done < "$RESULTS_FILE"

    echo "└─────────────┴─────────────┴─────────────┴─────────────┴──────────────┴────────────┘"
    echo
}

generate_analysis() {
    print_header "Performance Analysis"
    echo

    # Find maximum successful connections for each database
    local pg_max=0
    local fp_max=0

    while IFS=',' read -r db target success fail rate time; do
        # Only count if success rate is >= 90%
        if (( $(echo "$rate >= 90" | bc -l) )); then
            if [ "$db" = "PostgreSQL" ] && [ $success -gt $pg_max ]; then
                pg_max=$success
            elif [ "$db" = "FastPostgres" ] && [ $success -gt $fp_max ]; then
                fp_max=$success
            fi
        fi
    done < "$RESULTS_FILE"

    print_section "Maximum Concurrent Connections (≥90% success rate)"
    echo "  PostgreSQL:   $(format_number $pg_max) connections"
    echo "  FastPostgres: $(format_number $fp_max) connections"
    echo

    if [ $fp_max -gt $pg_max ]; then
        local ratio=$(echo "scale=2; $fp_max / $pg_max" | bc)
        local improvement=$(echo "scale=2; (($fp_max - $pg_max) / $pg_max) * 100" | bc)
        print_success "FastPostgres handles ${ratio}x more connections (+${improvement}%)"
    else
        print_warning "PostgreSQL handled more connections in this test"
    fi

    echo
}

generate_insights() {
    print_header "Key Insights & Recommendations"
    echo

    print_section "Understanding Connection Limits"
    echo "  • Connection limits depend on system resources (RAM, file descriptors)"
    echo "  • Each connection consumes memory and system resources"
    echo "  • Network latency affects concurrent connection handling"
    echo "  • Docker resource limits may restrict maximum connections"
    echo

    print_section "System Configuration Tips"
    echo "  1. Increase system file descriptor limit:"
    echo "     ulimit -n 65536"
    echo
    echo "  2. Increase Docker container limits:"
    echo "     docker run --ulimit nofile=65536:65536"
    echo
    echo "  3. PostgreSQL specific:"
    echo "     max_connections = 1000 (in postgresql.conf)"
    echo
    echo "  4. Monitor system resources during testing:"
    echo "     - Memory usage (free -h)"
    echo "     - Open file descriptors (lsof -u postgres | wc -l)"
    echo "     - CPU usage (top)"
    echo

    print_section "Connection Pooling Best Practices"
    echo "  ✓ Use connection pooling (pgBouncer, PgPool)"
    echo "  ✓ Keep connection pool size reasonable (50-200)"
    echo "  ✓ Set connection timeout appropriately"
    echo "  ✓ Monitor connection usage and adjust"
    echo "  ✓ Close idle connections"
    echo

    print_section "When to Scale"
    echo "  • If reaching 70% of max connections regularly"
    echo "  • If connection errors increase"
    echo "  • If response times degrade under load"
    echo "  • Consider read replicas or sharding"
    echo
}

check_system_limits() {
    print_section "System Configuration Check"

    # Check ulimit
    local ulimit_value=$(ulimit -n)
    echo "  File descriptor limit: $ulimit_value"
    if [ $ulimit_value -lt 10000 ]; then
        print_warning "Consider increasing: ulimit -n 65536"
    else
        print_success "File descriptor limit looks good"
    fi

    # Check available memory
    local free_mem=$(free -m | awk '/^Mem:/ {print $7}')
    echo "  Available memory: ${free_mem}MB"
    if [ $free_mem -lt 1000 ]; then
        print_warning "Low available memory may limit connections"
    fi

    echo
}

main() {
    clear

    echo "╔═══════════════════════════════════════════════════════════════╗"
    echo "║    Connection Stress Test - Maximum Concurrent Connections   ║"
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
    check_system_limits

    print_header "Stress Test Configuration"
    echo
    echo "⚠️  WARNING: This test will stress your system!"
    echo
    echo "This benchmark will test connection limits:"
    echo "  • Progressively increase concurrent connections"
    echo "  • Test levels: 100, 250, 500, 1K, 2.5K, 5K, 10K"
    echo "  • Each connection holds for 3 seconds"
    echo "  • Stops when success rate drops below 90%"
    echo
    echo "Estimated time: 5-10 minutes per database"
    echo "System impact: HIGH CPU and memory usage"
    echo
    print_warning "Ensure no critical workloads are running!"
    echo
    read -p "Press Enter to start or Ctrl+C to cancel..."
    echo

    find_maximum_connections "PostgreSQL" "localhost" "5432"

    echo
    echo "═══════════════════════════════════════════════════════════════"
    echo

    find_maximum_connections "FastPostgres" "localhost" "5433"

    generate_comparison_report
    generate_analysis
    generate_insights

    print_header "Stress Test Complete!"
    echo
    print_success "Results saved to: $RESULTS_FILE"
    echo
}

main "$@"