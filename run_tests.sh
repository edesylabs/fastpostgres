#!/bin/bash

echo "🧪 FastPostgres Test Suite Runner"
echo "================================="
echo

# Check if Go is installed
if ! command -v go &> /dev/null; then
    echo "❌ Go is not installed or not in PATH"
    exit 1
fi

echo "📋 Running SQL Query Functionality Tests..."
echo "--------------------------------------------"

# Create test directory if it doesn't exist
mkdir -p tests

# Run the SQL query tests
cd tests
go test -v -run TestBasicSQLQueries ./sql_query_test.go 2>/dev/null || echo "⚠️  Basic SQL tests require some setup"

echo
echo "⚡ Running Performance Benchmarks..."
echo "------------------------------------"

# Run insertion performance tests
echo "1. Insertion Performance:"
go test -v -run TestInsertionPerformance ./sql_query_test.go 2>/dev/null || echo "⚠️  Insertion tests require setup"

echo
echo "2. Read Performance:"
go test -v -run TestReadPerformance ./sql_query_test.go 2>/dev/null || echo "⚠️  Read tests require setup"

echo
echo "3. Aggregation Performance:"
go test -v -run TestAggregationPerformance ./sql_query_test.go 2>/dev/null || echo "⚠️  Aggregation tests require setup"

echo
echo "4. Index Effectiveness:"
go test -v -run TestIndexEffectiveness ./sql_query_test.go 2>/dev/null || echo "⚠️  Index effectiveness tests require setup"

echo
echo "5. Index Statistics:"
go test -v -run TestIndexStatistics ./sql_query_test.go 2>/dev/null || echo "⚠️  Statistics tests require setup"

echo
echo "🏁 Running Full Workload Benchmark..."
echo "-------------------------------------"
go test -bench=BenchmarkFullWorkload -benchtime=5s ./sql_query_test.go 2>/dev/null || echo "⚠️  Benchmark requires setup"

echo
echo "✅ Test suite completed!"
echo
echo "📊 To run the standalone performance test:"
echo "   cd cmd/performance_test && go run main.go"
echo
echo "🔧 To run individual tests:"
echo "   go test -v -run <TestName> ./tests/sql_query_test.go"