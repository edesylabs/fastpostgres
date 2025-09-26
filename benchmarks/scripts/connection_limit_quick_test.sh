#!/bin/bash

set -e

echo "⚡ Connection Limit - Quick Test"
echo "==============================="
echo

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

test_connection_capacity() {
    local db_name=$1
    local host=$2
    local port=$3
    local connections=$4

    echo -e "${BLUE}Testing $db_name with $connections concurrent connections${NC}"

    local start_time=$(date +%s.%N)
    local pids=()
    local successful=0

    # Launch concurrent connections
    for ((i=1; i<=connections; i++)); do
        {
            if timeout 2 psql -h $host -p $port -U postgres -d fastpostgres -c "SELECT 1, pg_sleep(2);" > /dev/null 2>&1; then
                echo "success" > /tmp/quick_conn_$i
            fi
        } &
        pids+=($!)
    done

    echo -n "  Waiting for connections to complete... "

    # Wait for all
    for pid in "${pids[@]}"; do
        wait $pid 2>/dev/null || true
    done

    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc)

    # Count successes
    for ((i=1; i<=connections; i++)); do
        if [ -f /tmp/quick_conn_$i ]; then
            successful=$((successful + 1))
            rm -f /tmp/quick_conn_$i
        fi
    done

    local failed=$((connections - successful))
    local success_rate=$(echo "scale=1; ($successful * 100) / $connections" | bc)

    echo -e "${GREEN}Done${NC}"
    echo "  Results:"
    echo "    Successful: $successful ($success_rate%)"
    echo "    Failed:     $failed"
    echo "    Duration:   ${duration}s"
    echo

    return $successful
}

echo "Quick test of connection handling capacity..."
echo

# Test with increasing connection counts
for connections in 50 100 200; do
    echo "═══ Test: $connections concurrent connections ═══"

    test_connection_capacity "PostgreSQL" "localhost" "5432" $connections
    pg_success=$?

    test_connection_capacity "FastPostgres" "localhost" "5433" $connections
    fp_success=$?

    echo "  Summary:"
    if [ $fp_success -gt $pg_success ]; then
        improvement=$(echo "scale=1; (($fp_success - $pg_success) * 100) / $pg_success" | bc)
        echo -e "${GREEN}  ✓ FastPostgres: +$improvement% more successful connections${NC}"
    elif [ $pg_success -gt $fp_success ]; then
        echo -e "${YELLOW}  ⚠ PostgreSQL handled more connections in this test${NC}"
    else
        echo "  ≈ Similar performance"
    fi
    echo
done

echo -e "${GREEN}✅ Quick connection test completed!${NC}"
echo
echo "For comprehensive stress testing (up to 10K connections), run:"
echo "  ./benchmarks/scripts/connection_stress_test.sh"
echo
echo -e "${YELLOW}Note: System limits (ulimit -n) may restrict maximum connections${NC}"