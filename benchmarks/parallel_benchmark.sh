#!/bin/bash

set -e

echo "======================================"
echo "FastPostgres Parallel Execution Benchmark"
echo "======================================"
echo ""

PORT=5436
SERVER_PID=""

cleanup() {
    echo "Cleaning up..."
    if [ ! -z "$SERVER_PID" ]; then
        kill $SERVER_PID 2>/dev/null || true
        wait $SERVER_PID 2>/dev/null || true
    fi
    rm -rf ./data
}

trap cleanup EXIT

echo "Step 1: Building FastPostgres..."
go build -o bin/fastpostgres cmd/fastpostgres/main.go
echo "✓ Build complete"
echo ""

echo "Step 2: Starting server on port $PORT..."
PORT=$PORT ./bin/fastpostgres server > /tmp/parallel_benchmark.log 2>&1 &
SERVER_PID=$!

sleep 3

if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo "✗ Server failed to start"
    cat /tmp/parallel_benchmark.log
    exit 1
fi

echo "✓ Server started (PID: $SERVER_PID)"
echo ""

echo "Step 3: Waiting for server to be ready..."
for i in {1..10}; do
    if psql -h localhost -p $PORT -U postgres -d fastpostgres -c "SELECT COUNT(*) FROM users;" > /dev/null 2>&1; then
        echo "✓ Server ready"
        break
    fi
    if [ $i -eq 10 ]; then
        echo "✗ Server not responding"
        exit 1
    fi
    sleep 1
done
echo ""

echo "======================================"
echo "Benchmark 1: SELECT * (All Rows)"
echo "======================================"
echo ""

echo "Query: SELECT * FROM users;"
echo ""
START_TIME=$(date +%s%N)
RESULT=$(psql -h localhost -p $PORT -U postgres -d fastpostgres -t -c "SELECT * FROM users;" 2>&1)
END_TIME=$(date +%s%N)
DURATION=$(( ($END_TIME - $START_TIME) / 1000000 ))

echo "$RESULT" | head -5
ROW_COUNT=$(echo "$RESULT" | grep -v "^$" | wc -l | tr -d ' ')
echo "Rows returned: $ROW_COUNT"
echo "Execution time: ${DURATION}ms"
echo ""

echo "======================================"
echo "Benchmark 2: SELECT with Filter"
echo "======================================"
echo ""

echo "Query: SELECT * FROM users WHERE age > 25;"
echo ""
START_TIME=$(date +%s%N)
RESULT=$(psql -h localhost -p $PORT -U postgres -d fastpostgres -t -c "SELECT * FROM users WHERE age > 25;" 2>&1)
END_TIME=$(date +%s%N)
DURATION=$(( ($END_TIME - $START_TIME) / 1000000 ))

echo "$RESULT" | head -5
ROW_COUNT=$(echo "$RESULT" | grep -v "^$" | wc -l | tr -d ' ')
echo "Rows returned: $ROW_COUNT"
echo "Execution time: ${DURATION}ms"
echo ""

echo "======================================"
echo "Benchmark 3: COUNT(*)"
echo "======================================"
echo ""

echo "Query: SELECT COUNT(*) FROM users;"
echo ""
START_TIME=$(date +%s%N)
RESULT=$(psql -h localhost -p $PORT -U postgres -d fastpostgres -t -c "SELECT COUNT(*) FROM users;" 2>&1)
END_TIME=$(date +%s%N)
DURATION=$(( ($END_TIME - $START_TIME) / 1000000 ))

echo "Result: $RESULT"
echo "Execution time: ${DURATION}ms"
echo ""

echo "======================================"
echo "Benchmark 4: SUM(age)"
echo "======================================"
echo ""

echo "Query: SELECT SUM(age) FROM users;"
echo ""
START_TIME=$(date +%s%N)
RESULT=$(psql -h localhost -p $PORT -U postgres -d fastpostgres -t -c "SELECT SUM(age) FROM users;" 2>&1)
END_TIME=$(date +%s%N)
DURATION=$(( ($END_TIME - $START_TIME) / 1000000 ))

echo "Result: $RESULT"
echo "Execution time: ${DURATION}ms"
echo ""

echo "======================================"
echo "Benchmark 5: AVG(age)"
echo "======================================"
echo ""

echo "Query: SELECT AVG(age) FROM users;"
echo ""
START_TIME=$(date +%s%N)
RESULT=$(psql -h localhost -p $PORT -U postgres -d fastpostgres -t -c "SELECT AVG(age) FROM users;" 2>&1)
END_TIME=$(date +%s%N)
DURATION=$(( ($END_TIME - $START_TIME) / 1000000 ))

echo "Result: $RESULT"
echo "Execution time: ${DURATION}ms"
echo ""

echo "======================================"
echo "Benchmark 6: MIN/MAX"
echo "======================================"
echo ""

echo "Query: SELECT MIN(age), MAX(age) FROM users;"
echo ""
START_TIME=$(date +%s%N)
RESULT=$(psql -h localhost -p $PORT -U postgres -d fastpostgres -t -c "SELECT MIN(age), MAX(age) FROM users;" 2>&1)
END_TIME=$(date +%s%N)
DURATION=$(( ($END_TIME - $START_TIME) / 1000000 ))

echo "Result: $RESULT"
echo "Execution time: ${DURATION}ms"
echo ""

echo "======================================"
echo "Benchmark Complete!"
echo "======================================"
echo ""
echo "Server log available at: /tmp/parallel_benchmark.log"
echo ""