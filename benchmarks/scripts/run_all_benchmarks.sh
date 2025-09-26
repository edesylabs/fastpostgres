#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║     FastPostgres vs PostgreSQL - Complete Benchmark Suite     ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo

check_prerequisites() {
    echo "🔍 Checking prerequisites..."

    if ! command -v psql &> /dev/null; then
        echo "❌ Error: psql is not installed. Please install PostgreSQL client."
        exit 1
    fi

    if ! command -v docker &> /dev/null; then
        echo "❌ Error: docker is not installed. Please install Docker."
        exit 1
    fi

    if ! command -v docker-compose &> /dev/null; then
        echo "❌ Error: docker-compose is not installed. Please install Docker Compose."
        exit 1
    fi

    echo "✅ All prerequisites met!"
    echo
}

check_databases() {
    echo "🗄️  Checking database connections..."

    if psql -h localhost -p 5432 -U postgres -d fastpostgres -c "SELECT 1;" > /dev/null 2>&1; then
        echo "✅ PostgreSQL is running on port 5432"
    else
        echo "❌ PostgreSQL is not accessible on port 5432"
        echo "   Please start PostgreSQL using: docker-compose up -d postgres"
        exit 1
    fi

    if timeout 5 psql -h localhost -p 5433 -U postgres -d fastpostgres -c "SELECT 1;" > /dev/null 2>&1; then
        echo "✅ FastPostgres is running on port 5433"
    else
        echo "❌ FastPostgres is not accessible on port 5433"
        echo "   Please start FastPostgres using: docker-compose up -d fastpostgres"
        exit 1
    fi

    echo
}

run_benchmarks() {
    echo "🚀 Running all benchmarks..."
    echo "═══════════════════════════════════════════════════════════════"
    echo

    echo "📊 Benchmark 1: Basic Performance Test"
    echo "────────────────────────────────────────────────────────────────"
    bash "$SCRIPT_DIR/basic_benchmark.sh"
    echo

    echo "🔗 Benchmark 2: Concurrent Connections Test"
    echo "────────────────────────────────────────────────────────────────"
    bash "$SCRIPT_DIR/concurrent_connections_benchmark.sh"
    echo

    echo "⚡ Benchmark 3: Query Performance Test"
    echo "────────────────────────────────────────────────────────────────"
    bash "$SCRIPT_DIR/query_performance_benchmark.sh"
    echo
}

print_summary() {
    echo "╔════════════════════════════════════════════════════════════════╗"
    echo "║                     Benchmark Complete!                        ║"
    echo "╚════════════════════════════════════════════════════════════════╝"
    echo
    echo "📝 Results have been displayed above."
    echo "💡 Tip: You can run individual benchmarks from benchmarks/scripts/"
    echo "🔧 To stop databases: docker-compose down"
    echo
}

main() {
    check_prerequisites
    check_databases
    run_benchmarks
    print_summary
}

main "$@"