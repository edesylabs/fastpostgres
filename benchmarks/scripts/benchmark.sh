#!/bin/bash

set -e

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║        FastPostgres vs PostgreSQL Benchmark Suite             ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if service is ready
wait_for_service() {
    local service_name=$1
    local url=$2
    local max_attempts=30
    local attempt=1

    print_status "Waiting for $service_name to be ready..."

    while [ $attempt -le $max_attempts ]; do
        if curl -s -f "$url" > /dev/null 2>&1; then
            print_success "$service_name is ready!"
            return 0
        fi

        print_status "Attempt $attempt/$max_attempts: $service_name not ready yet..."
        sleep 2
        attempt=$((attempt + 1))
    done

    print_error "$service_name failed to start within $((max_attempts * 2)) seconds"
    return 1
}

# Function to run benchmark tests
run_benchmark() {
    local db_name=$1
    local api_base=$2

    echo
    echo "🚀 Running benchmarks for $db_name"
    echo "═══════════════════════════════════════════════"

    # Test 1: Simple query performance
    print_status "Test 1: Simple SELECT query performance"
    time curl -s "$api_base/users?limit=1000" | jq '.count' > /dev/null

    # Test 2: Aggregation query performance
    print_status "Test 2: Aggregation query performance"
    time curl -s "$api_base/analytics/summary" > /dev/null

    # Test 3: JOIN query performance
    print_status "Test 3: Complex JOIN query performance"
    time curl -s "$api_base/complex/user-orders" > /dev/null

    # Test 4: Concurrent queries
    print_status "Test 4: Concurrent query performance (50 concurrent)"
    time curl -s "$api_base/perf/concurrent/50" | jq '.queries_per_sec'

    echo
}

# Function to measure insertion performance
test_insertion_performance() {
    local db_name=$1
    local api_base=$2

    echo "📊 Testing insertion performance for $db_name"
    echo "════════════════════════════════════════════════"

    print_status "Testing bulk insertion performance..."
    time curl -s "$api_base/perf/bulk-insert/1000" | jq '.rows_per_sec'
    echo
}

# Main execution
main() {
    print_status "Starting Docker Compose stack..."

    # Start all services
    docker-compose up -d --build

    # Wait for services to be ready
    wait_for_service "FastPostgres API" "http://localhost:8080/api/health"
    wait_for_service "PostgreSQL API" "http://localhost:8081/api/health"

    echo
    echo "🏁 All services are ready! Starting benchmark tests..."
    echo

    # Run benchmarks for FastPostgres
    run_benchmark "FastPostgres" "http://localhost:8080/api"
    test_insertion_performance "FastPostgres" "http://localhost:8080/api"

    # Run benchmarks for PostgreSQL
    run_benchmark "PostgreSQL" "http://localhost:8081/api"
    test_insertion_performance "PostgreSQL" "http://localhost:8081/api"

    echo
    echo "📈 Benchmark Results Summary"
    echo "════════════════════════════"

    print_success "FastPostgres benchmarks completed"
    print_success "PostgreSQL benchmarks completed"

    echo
    print_status "Access the APIs:"
    echo "  • FastPostgres API: http://localhost:8080/api"
    echo "  • PostgreSQL API:   http://localhost:8081/api"
    echo "  • FastPostgres DB:  localhost:5435"
    echo "  • PostgreSQL DB:    localhost:5434"
    echo

    print_warning "Press Ctrl+C to stop all services"

    # Keep running
    trap 'echo && print_status "Stopping services..." && docker-compose down && exit 0' INT

    while true; do
        sleep 1
    done
}

# Run the main function
main "$@"