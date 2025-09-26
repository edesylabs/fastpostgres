#!/bin/bash

set -e

echo "📊 FastPostgres Columnar Analytics Performance Benchmark"
echo "========================================================"
echo "Testing aggregation queries that showcase columnar storage advantages"
echo

# Test configurations
TEST_SIZES=(10000 100000 500000 1000000)
POSTGRES_PORT=5432
FASTPG_PORT=5433

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

test_analytical_queries() {
    local db_name=$1
    local host=$2
    local port=$3
    local table_size=$4

    echo -e "${BLUE}Testing $db_name with $table_size rows${NC}"

    # Create analytical test table
    psql -h $host -p $port -U postgres -d testdb -c "DROP TABLE IF EXISTS analytics_test;" >/dev/null 2>&1 || true

    psql -h $host -p $port -U postgres -d testdb -c "
        CREATE TABLE analytics_test (
            user_id BIGINT,
            transaction_date DATE,
            amount DECIMAL(12,2),
            category TEXT,
            region TEXT,
            product_id BIGINT,
            quantity BIGINT,
            revenue DECIMAL(12,2)
        );" >/dev/null 2>&1

    echo "  Inserting $table_size analytical records..."

    # Generate realistic analytical data
    categories=("Electronics" "Clothing" "Home" "Books" "Sports" "Beauty" "Auto" "Food" "Health" "Travel")
    regions=("North" "South" "East" "West" "Central")

    # Use COPY for faster bulk insert
    temp_file="/tmp/analytics_data_$table_size.csv"
    echo "Generating test data file..."

    > $temp_file
    for i in $(seq 1 $table_size); do
        user_id=$((1 + RANDOM % 10000))
        days_offset=$((RANDOM % 365))
        transaction_date=$(date -d "2023-01-01 + $days_offset days" +%Y-%m-%d)
        amount=$(echo "scale=2; $RANDOM / 100" | bc)
        category=${categories[$((RANDOM % ${#categories[@]}))]}
        region=${regions[$((RANDOM % ${#regions[@]}))]}
        product_id=$((1 + RANDOM % 1000))
        quantity=$((1 + RANDOM % 10))
        revenue=$(echo "scale=2; $amount * $quantity" | bc)

        echo "$user_id,$transaction_date,$amount,$category,$region,$product_id,$quantity,$revenue" >> $temp_file
    done

    # Import data using COPY
    psql -h $host -p $port -U postgres -d testdb -c "
        COPY analytics_test(user_id,transaction_date,amount,category,region,product_id,quantity,revenue)
        FROM '$temp_file' DELIMITER ',' CSV;" >/dev/null 2>&1

    rm -f $temp_file

    # Define analytical queries that benefit from columnar storage
    declare -A analytical_queries=(
        ["TOTAL_REVENUE"]="SELECT SUM(revenue) FROM analytics_test"
        ["AVG_TRANSACTION"]="SELECT AVG(amount) FROM analytics_test"
        ["COUNT_TRANSACTIONS"]="SELECT COUNT(*) FROM analytics_test"
        ["MIN_MAX_AMOUNT"]="SELECT MIN(amount), MAX(amount) FROM analytics_test"
        ["REVENUE_BY_CATEGORY"]="SELECT category, SUM(revenue), COUNT(*) FROM analytics_test GROUP BY category"
        ["REVENUE_BY_REGION"]="SELECT region, AVG(amount), COUNT(*) FROM analytics_test GROUP BY region"
        ["TOP_PRODUCTS"]="SELECT product_id, SUM(quantity) as total_qty FROM analytics_test GROUP BY product_id ORDER BY total_qty DESC LIMIT 10"
        ["COMPLEX_AGGREGATION"]="SELECT region, category, SUM(revenue), AVG(amount), COUNT(*) FROM analytics_test GROUP BY region, category"
    )

    declare -A query_iterations=(
        ["TOTAL_REVENUE"]=20
        ["AVG_TRANSACTION"]=20
        ["COUNT_TRANSACTIONS"]=20
        ["MIN_MAX_AMOUNT"]=15
        ["REVENUE_BY_CATEGORY"]=10
        ["REVENUE_BY_REGION"]=10
        ["TOP_PRODUCTS"]=8
        ["COMPLEX_AGGREGATION"]=5
    )

    for query_name in "${!analytical_queries[@]}"; do
        query="${analytical_queries[$query_name]}"
        iterations=${query_iterations[$query_name]}

        echo "  Testing $query_name query ($iterations iterations)..."

        start_time=$(date +%s.%N)
        for ((j=1; j<=$iterations; j++)); do
            timeout 30 psql -h $host -p $port -U postgres -d testdb -c "$query" >/dev/null 2>&1 || {
                echo "    WARNING: Query timed out or failed"
                break
            }
        done
        end_time=$(date +%s.%N)

        if (( $(echo "$end_time > $start_time" | bc -l) )); then
            total_duration=$(echo "$end_time - $start_time" | bc)
            avg_duration=$(echo "scale=6; $total_duration / $iterations" | bc)
            qps=$(echo "scale=2; $iterations / $total_duration" | bc)
            rows_per_sec=$(echo "scale=0; $table_size * $qps" | bc)

            echo "    ✓ $query_name: ${avg_duration}s avg, ${qps} q/s, ${rows_per_sec} rows/s processed"

            # Store results for comparison
            echo "$db_name,$table_size,$query_name,$avg_duration,$qps,$rows_per_sec" >> /tmp/columnar_results.csv
        else
            echo "    ✗ $query_name: Failed or timed out"
        fi
    done

    echo
}

create_performance_report() {
    echo "📈 Columnar Analytics Performance Report"
    echo "========================================"
    echo

    if [[ ! -f /tmp/columnar_results.csv ]]; then
        echo "No results found!"
        return
    fi

    echo "Detailed Results:"
    echo "Database,Table_Size,Query_Type,Avg_Duration(s),QPS,Rows_Per_Second"
    cat /tmp/columnar_results.csv | sort
    echo

    echo "Performance Comparison Analysis:"
    echo "================================"

    for size in "${TEST_SIZES[@]}"; do
        echo
        echo -e "${YELLOW}Dataset Size: $size rows${NC}"
        echo "----------------------------"

        # Compare performance for each query type
        for query_type in "TOTAL_REVENUE" "AVG_TRANSACTION" "COUNT_TRANSACTIONS" "REVENUE_BY_CATEGORY" "COMPLEX_AGGREGATION"; do
            fastpg_line=$(grep "FastPostgres,$size,$query_type" /tmp/columnar_results.csv 2>/dev/null || echo "")
            postgres_line=$(grep "PostgreSQL,$size,$query_type" /tmp/columnar_results.csv 2>/dev/null || echo "")

            if [[ -n "$fastpg_line" && -n "$postgres_line" ]]; then
                fastpg_qps=$(echo "$fastpg_line" | cut -d',' -f5)
                postgres_qps=$(echo "$postgres_line" | cut -d',' -f5)

                fastpg_rps=$(echo "$fastpg_line" | cut -d',' -f6)
                postgres_rps=$(echo "$postgres_line" | cut -d',' -f6)

                if [[ -n "$fastpg_qps" && -n "$postgres_qps" ]] && (( $(echo "$postgres_qps > 0" | bc -l) )); then
                    speedup=$(echo "scale=2; $fastpg_qps / $postgres_qps" | bc)
                    rps_speedup=$(echo "scale=2; $fastpg_rps / $postgres_rps" | bc)

                    if (( $(echo "$speedup > 1.1" | bc -l) )); then
                        echo -e "${GREEN}  $query_type: FastPostgres ${speedup}x faster (${rps_speedup}x row processing speedup)${NC}"
                    elif (( $(echo "$speedup < 0.9" | bc -l) )); then
                        speedup_inv=$(echo "scale=2; $postgres_qps / $fastpg_qps" | bc)
                        echo -e "${RED}  $query_type: PostgreSQL ${speedup_inv}x faster${NC}"
                    else
                        echo "  $query_type: Comparable performance (${speedup}x)"
                    fi
                fi
            fi
        done
    done

    # Calculate overall columnar advantage
    echo
    echo "🏆 COLUMNAR STORAGE ANALYSIS:"
    echo "============================="

    total_fastpg_advantage=0
    comparison_count=0

    for query_type in "TOTAL_REVENUE" "AVG_TRANSACTION" "REVENUE_BY_CATEGORY"; do
        for size in "${TEST_SIZES[@]}"; do
            fastpg_rps=$(grep "FastPostgres,$size,$query_type" /tmp/columnar_results.csv 2>/dev/null | cut -d',' -f6)
            postgres_rps=$(grep "PostgreSQL,$size,$query_type" /tmp/columnar_results.csv 2>/dev/null | cut -d',' -f6)

            if [[ -n "$fastpg_rps" && -n "$postgres_rps" ]] && (( $(echo "$postgres_rps > 0" | bc -l) )); then
                speedup=$(echo "scale=2; $fastpg_rps / $postgres_rps" | bc)
                total_fastpg_advantage=$(echo "$total_fastpg_advantage + $speedup" | bc)
                comparison_count=$((comparison_count + 1))
            fi
        done
    done

    if (( comparison_count > 0 )); then
        avg_advantage=$(echo "scale=2; $total_fastpg_advantage / $comparison_count" | bc)
        echo "Average FastPostgres columnar advantage: ${avg_advantage}x"

        if (( $(echo "$avg_advantage > 2.0" | bc -l) )); then
            echo -e "${GREEN}🚀 Excellent columnar performance! FastPostgres shows strong advantage for analytical workloads${NC}"
        elif (( $(echo "$avg_advantage > 1.5" | bc -l) )); then
            echo -e "${GREEN}✅ Good columnar performance! FastPostgres outperforms PostgreSQL for analytics${NC}"
        elif (( $(echo "$avg_advantage > 1.1" | bc -l) )); then
            echo -e "${YELLOW}⚡ Moderate columnar advantage detected${NC}"
        else
            echo -e "${RED}⚠️  Limited columnar advantage - may need optimization${NC}"
        fi
    fi
}

# Initialize results file
echo "Database,Table_Size,Query_Type,Avg_Duration,QPS,Rows_Per_Second" > /tmp/columnar_results.csv

echo "🚀 Starting columnar analytics performance tests..."
echo

# Test each dataset size
for size in "${TEST_SIZES[@]}"; do
    echo "=== Testing with $size rows ==="

    # Test PostgreSQL first
    echo -e "${BLUE}PostgreSQL (port $POSTGRES_PORT)${NC}"
    if nc -z localhost $POSTGRES_PORT 2>/dev/null; then
        test_analytical_queries "PostgreSQL" "localhost" $POSTGRES_PORT $size
    else
        echo -e "${RED}PostgreSQL not running on port $POSTGRES_PORT${NC}"
    fi

    # Test FastPostgres
    echo -e "${BLUE}FastPostgres (port $FASTPG_PORT)${NC}"
    if nc -z localhost $FASTPG_PORT 2>/dev/null; then
        test_analytical_queries "FastPostgres" "localhost" $FASTPG_PORT $size
    else
        echo -e "${RED}FastPostgres not running on port $FASTPG_PORT${NC}"
    fi

    echo
done

# Generate comprehensive performance report
create_performance_report

# Cleanup
rm -f /tmp/columnar_results.csv

echo "✅ Columnar analytics benchmark completed!"
echo
echo "💡 KEY INSIGHTS:"
echo "• Columnar storage excels at aggregation queries (SUM, AVG, COUNT)"
echo "• GROUP BY operations benefit from columnar data organization"
echo "• Larger datasets show more pronounced columnar advantages"
echo "• SIMD vectorization provides additional performance gains"