#!/bin/bash

set -e

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║                    FastPostgres Gin App Test                 ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# Function to test if server is running
test_server() {
    echo "🔍 Testing if FastPostgres server is running..."

    if curl -s http://localhost:5432 > /dev/null 2>&1; then
        echo "✓ FastPostgres server is running on port 5432"
        return 0
    else
        echo "⚠ FastPostgres server not responding on port 5432"
        return 1
    fi
}

# Function to start FastPostgres server
start_fastpostgres() {
    echo ""
    echo "🚀 Starting FastPostgres server..."

    if [ -f "./bin/fastpostgres" ]; then
        echo "  → Using existing binary: ./bin/fastpostgres"
        ./bin/fastpostgres server 5432 &
    elif [ -f "./fastpostgres" ]; then
        echo "  → Using: ./fastpostgres"
        ./fastpostgres server 5432 &
    else
        echo "  → Building FastPostgres..."
        go build -o fastpostgres ./cmd/fastpostgres/
        ./fastpostgres server 5432 &
    fi

    FASTPOSTGRES_PID=$!
    echo "  → FastPostgres PID: $FASTPOSTGRES_PID"

    # Wait for server to start
    echo "  → Waiting for server to start..."
    sleep 3

    return 0
}

# Function to run the Gin application
run_gin_app() {
    echo ""
    echo "🎯 Starting Gin REST API application..."

    cd examples/gin_rest_api
    go run main.go &
    GIN_PID=$!

    echo "  → Gin App PID: $GIN_PID"
    echo "  → Waiting for Gin app to start..."
    sleep 5

    return 0
}

# Function to test API endpoints
test_api() {
    echo ""
    echo "🧪 Testing API endpoints..."

    BASE_URL="http://localhost:8080/api"

    echo "  → Testing health check..."
    curl -s "$BASE_URL/health" | jq . || echo "Failed"

    echo ""
    echo "  → Testing database stats..."
    curl -s "$BASE_URL/stats" | jq . || echo "Failed"

    echo ""
    echo "  → Testing users endpoint..."
    curl -s "$BASE_URL/users?limit=5" | jq . || echo "Failed"

    echo ""
    echo "  → Testing products endpoint..."
    curl -s "$BASE_URL/products?limit=5" | jq . || echo "Failed"

    echo ""
    echo "  → Testing analytics summary..."
    curl -s "$BASE_URL/analytics/summary" | jq . || echo "Failed"

    echo ""
    echo "  → Testing complex query (user orders)..."
    curl -s "$BASE_URL/complex/user-orders" | jq . || echo "Failed"

    echo ""
    echo "  → Testing performance endpoint..."
    curl -s "$BASE_URL/perf/concurrent/50" | jq . || echo "Failed"
}

# Function to cleanup
cleanup() {
    echo ""
    echo "🧹 Cleaning up..."

    if [ ! -z "$GIN_PID" ]; then
        echo "  → Stopping Gin app (PID: $GIN_PID)..."
        kill $GIN_PID 2>/dev/null || true
    fi

    if [ ! -z "$FASTPOSTGRES_PID" ]; then
        echo "  → Stopping FastPostgres (PID: $FASTPOSTGRES_PID)..."
        kill $FASTPOSTGRES_PID 2>/dev/null || true
    fi

    # Also kill any other instances
    pkill -f "fastpostgres server" 2>/dev/null || true
    pkill -f "gin_rest_api" 2>/dev/null || true

    echo "✓ Cleanup completed"
}

# Trap to ensure cleanup on exit
trap cleanup EXIT

# Main execution
echo "Phase 1: FastPostgres Server"
echo "──────────────────────────────"

if ! test_server; then
    start_fastpostgres

    # Test again after starting
    sleep 2
    if ! test_server; then
        echo "❌ Failed to start FastPostgres server"
        exit 1
    fi
fi

echo ""
echo "Phase 2: Gin Application"
echo "────────────────────────"

run_gin_app

# Test if Gin app is running
echo "  → Testing Gin app health..."
for i in {1..10}; do
    if curl -s http://localhost:8080/api/health > /dev/null 2>&1; then
        echo "✓ Gin app is running on port 8080"
        break
    fi

    if [ $i -eq 10 ]; then
        echo "❌ Gin app failed to start"
        exit 1
    fi

    echo "    Waiting... ($i/10)"
    sleep 1
done

echo ""
echo "Phase 3: API Testing"
echo "────────────────────"

# Check if jq is available for pretty JSON output
if ! command -v jq &> /dev/null; then
    echo "⚠ jq not found - JSON output will not be formatted"
    echo "  Install with: brew install jq (macOS) or apt-get install jq (Linux)"
fi

test_api

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "✅ Test completed successfully!"
echo ""
echo "🌐 Access the API at: http://localhost:8080/api"
echo "📖 Available endpoints:"
echo "   • GET /api/health              - Health check"
echo "   • GET /api/stats               - Database statistics"
echo "   • GET /api/users               - List users"
echo "   • GET /api/products            - List products"
echo "   • GET /api/analytics/summary   - Analytics summary"
echo "   • GET /api/complex/user-orders - Complex JOIN query"
echo "   • GET /api/perf/concurrent/N   - Performance test"
echo ""
echo "Press Ctrl+C to stop the servers..."

# Keep running until user presses Ctrl+C
wait