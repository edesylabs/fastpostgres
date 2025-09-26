#!/bin/bash

set -e

echo "🚀 FastPostgres vs PostgreSQL Basic Benchmark"
echo "=============================================="
echo

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}Testing PostgreSQL (Port 5432)${NC}"
echo "------------------------------------"

echo -n "Connection test: "
start_time=$(date +%s.%N)
psql -h localhost -p 5432 -U postgres -d fastpostgres -c "SELECT 1;" > /dev/null 2>&1
end_time=$(date +%s.%N)
pg_conn_time=$(echo "$end_time - $start_time" | bc)
echo -e "${GREEN}${pg_conn_time}s${NC}"

echo -n "Create table and insert 1000 rows: "
start_time=$(date +%s.%N)
psql -h localhost -p 5432 -U postgres -d fastpostgres -c "
DROP TABLE IF EXISTS bench_test;
CREATE TABLE bench_test (id SERIAL PRIMARY KEY, name VARCHAR(50), value INTEGER);
" > /dev/null 2>&1

for i in {1..1000}; do
  psql -h localhost -p 5432 -U postgres -d fastpostgres -c "INSERT INTO bench_test (name, value) VALUES ('name_$i', $i);" > /dev/null 2>&1
done

end_time=$(date +%s.%N)
pg_insert_time=$(echo "$end_time - $start_time" | bc)
echo -e "${GREEN}${pg_insert_time}s${NC}"

echo -n "SELECT query (1000 rows): "
start_time=$(date +%s.%N)
psql -h localhost -p 5432 -U postgres -d fastpostgres -c "SELECT COUNT(*) FROM bench_test;" > /dev/null 2>&1
end_time=$(date +%s.%N)
pg_select_time=$(echo "$end_time - $start_time" | bc)
echo -e "${GREEN}${pg_select_time}s${NC}"

echo
echo -e "${BLUE}Testing FastPostgres (Port 5433)${NC}"
echo "-------------------------------------"

echo -n "Connection test: "
start_time=$(date +%s.%N)
timeout 5 psql -h localhost -p 5433 -U postgres -d fastpostgres -c "SELECT 1;" > /dev/null 2>&1
end_time=$(date +%s.%N)
fp_conn_time=$(echo "$end_time - $start_time" | bc)
echo -e "${GREEN}${fp_conn_time}s${NC}"

echo -n "Basic query test: "
start_time=$(date +%s.%N)
timeout 5 psql -h localhost -p 5433 -U postgres -d fastpostgres -c "SELECT version();" > /dev/null 2>&1
end_time=$(date +%s.%N)
fp_query_time=$(echo "$end_time - $start_time" | bc)
echo -e "${GREEN}${fp_query_time}s${NC}"

echo
echo -e "${YELLOW}Benchmark Summary:${NC}"
echo "=================="
echo "PostgreSQL:"
echo "  - Connection: ${pg_conn_time}s"
echo "  - Insert 1000 rows: ${pg_insert_time}s"
echo "  - SELECT query: ${pg_select_time}s"
echo
echo "FastPostgres:"
echo "  - Connection: ${fp_conn_time}s"
echo "  - Basic query: ${fp_query_time}s"
echo