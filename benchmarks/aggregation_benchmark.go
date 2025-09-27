package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"time"

	_ "github.com/lib/pq"
)

type BenchmarkResult struct {
	Query     string
	Database  string
	Duration  time.Duration
	RowsCount int64
}

func connectDB(host string, port int, dbname string) (*sql.DB, error) {
	connStr := fmt.Sprintf("host=%s port=%d user=postgres password=postgres dbname=%s sslmode=disable", host, port, dbname)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func createTables(db *sql.DB, dbName string) error {
	tables := []string{
		"sales", "customers", "products", "orders", "inventory",
		"transactions", "employees", "suppliers", "shipments", "payments",
	}

	fmt.Printf("Creating 10 tables in %s...\n", dbName)

	for _, tableName := range tables {
		dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
		_, err := db.Exec(dropSQL)
		if err != nil {
			return fmt.Errorf("error dropping table %s: %v", tableName, err)
		}

		createSQL := fmt.Sprintf(`
			CREATE TABLE %s (
				id SERIAL PRIMARY KEY,
				category VARCHAR(50),
				amount DECIMAL(10,2),
				quantity INTEGER,
				region VARCHAR(50),
				status VARCHAR(20),
				created_at TIMESTAMP
			)
		`, tableName)

		_, err = db.Exec(createSQL)
		if err != nil {
			return fmt.Errorf("error creating table %s: %v", tableName, err)
		}
	}

	fmt.Printf("Successfully created 10 tables in %s\n", dbName)
	return nil
}

func insertData(db *sql.DB, dbName string) error {
	tables := []string{
		"sales", "customers", "products", "orders", "inventory",
		"transactions", "employees", "suppliers", "shipments", "payments",
	}

	categories := []string{"Electronics", "Clothing", "Food", "Books", "Toys"}
	regions := []string{"North", "South", "East", "West", "Central"}
	statuses := []string{"Active", "Pending", "Completed", "Cancelled"}

	fmt.Printf("Inserting data into %s...\n", dbName)

	for _, tableName := range tables {
		fmt.Printf("  Inserting 10,000 rows into %s...\n", tableName)

		startTime := time.Now()

		batchSize := 100
		for batch := 0; batch < 100; batch++ {
			values := ""
			for i := 0; i < batchSize; i++ {
				category := categories[rand.Intn(len(categories))]
				amount := float64(rand.Intn(10000)) / 100.0
				quantity := rand.Intn(100) + 1
				region := regions[rand.Intn(len(regions))]
				status := statuses[rand.Intn(len(statuses))]
				createdAt := time.Now().Add(-time.Duration(rand.Intn(365)) * 24 * time.Hour).Format("2006-01-02 15:04:05")

				if i > 0 {
					values += ", "
				}
				values += fmt.Sprintf("('%s', %.2f, %d, '%s', '%s', '%s')",
					category, amount, quantity, region, status, createdAt)
			}

			insertSQL := fmt.Sprintf(`
				INSERT INTO %s (category, amount, quantity, region, status, created_at)
				VALUES %s
			`, tableName, values)

			_, err := db.Exec(insertSQL)
			if err != nil {
				return fmt.Errorf("error inserting batch into %s: %v", tableName, err)
			}
		}

		elapsed := time.Since(startTime)
		fmt.Printf("    Completed in %v (%.0f rows/sec)\n", elapsed, 10000.0/elapsed.Seconds())
	}

	return nil
}

func runAggregationQuery(db *sql.DB, query string, description string) (time.Duration, int64, error) {
	startTime := time.Now()

	rows, err := db.Query(query)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()

	var count int64
	for rows.Next() {
		count++
		var dummy interface{}
		cols, _ := rows.Columns()
		values := make([]interface{}, len(cols))
		for i := range values {
			values[i] = &dummy
		}
		rows.Scan(values...)
	}

	elapsed := time.Since(startTime)
	return elapsed, count, nil
}

func runBenchmarks(fastDB *sql.DB, pgDB *sql.DB) {
	queries := []struct {
		name  string
		query string
	}{
		{
			name:  "SUM aggregation on sales",
			query: "SELECT SUM(amount) FROM sales",
		},
		{
			name:  "AVG aggregation on orders",
			query: "SELECT AVG(quantity) FROM orders",
		},
		{
			name:  "COUNT with GROUP BY on products",
			query: "SELECT category, COUNT(*) FROM products GROUP BY category",
		},
		{
			name:  "Multiple aggregations on transactions",
			query: "SELECT region, SUM(amount), AVG(quantity), COUNT(*) FROM transactions GROUP BY region",
		},
		{
			name:  "MAX/MIN aggregation on inventory",
			query: "SELECT category, MAX(amount), MIN(amount) FROM inventory GROUP BY category",
		},
		{
			name:  "Complex aggregation on employees",
			query: "SELECT region, status, SUM(amount), COUNT(*) FROM employees WHERE quantity > 50 GROUP BY region, status",
		},
		{
			name:  "HAVING clause on suppliers",
			query: "SELECT category, SUM(amount) as total FROM suppliers GROUP BY category HAVING SUM(amount) > 1000",
		},
		{
			name:  "COUNT DISTINCT on shipments",
			query: "SELECT COUNT(DISTINCT region) FROM shipments",
		},
		{
			name:  "Multiple tables - payments aggregation",
			query: "SELECT status, SUM(amount), AVG(amount) FROM payments GROUP BY status ORDER BY SUM(amount) DESC",
		},
		{
			name:  "Date-based aggregation on customers",
			query: "SELECT DATE(created_at), COUNT(*), SUM(amount) FROM customers GROUP BY DATE(created_at) ORDER BY DATE(created_at) DESC LIMIT 10",
		},
	}

	fmt.Println("\n" + "================================================================================")
	fmt.Println("AGGREGATION BENCHMARK RESULTS")
	fmt.Println("================================================================================")

	var totalFastTime, totalPgTime time.Duration

	for i, q := range queries {
		fmt.Printf("\n[%d/%d] %s\n", i+1, len(queries), q.name)
		fmt.Println("--------------------------------------------------------------------------------")

		fastDuration, fastRows, err := runAggregationQuery(fastDB, q.query, q.name)
		if err != nil {
			fmt.Printf("FastPostgres ERROR: %v\n", err)
		} else {
			fmt.Printf("FastPostgres: %v (returned %d rows)\n", fastDuration, fastRows)
			totalFastTime += fastDuration
		}

		pgDuration, pgRows, err := runAggregationQuery(pgDB, q.query, q.name)
		if err != nil {
			fmt.Printf("PostgreSQL ERROR: %v\n", err)
		} else {
			fmt.Printf("PostgreSQL:   %v (returned %d rows)\n", pgDuration, pgRows)
			totalPgTime += pgDuration
		}

		if err == nil && fastDuration > 0 && pgDuration > 0 {
			improvement := ((float64(pgDuration) - float64(fastDuration)) / float64(pgDuration)) * 100
			if improvement > 0 {
				fmt.Printf("Improvement:  %.2f%% faster\n", improvement)
			} else {
				fmt.Printf("Performance:  %.2f%% slower\n", -improvement)
			}
		}
	}

	fmt.Println("\n" + "================================================================================")
	fmt.Println("SUMMARY")
	fmt.Println("================================================================================")
	fmt.Printf("FastPostgres Total Time: %v\n", totalFastTime)
	fmt.Printf("PostgreSQL Total Time:   %v\n", totalPgTime)

	if totalFastTime > 0 && totalPgTime > 0 {
		improvement := ((float64(totalPgTime) - float64(totalFastTime)) / float64(totalPgTime)) * 100
		if improvement > 0 {
			fmt.Printf("Overall Improvement:     %.2f%% faster\n", improvement)
			fmt.Printf("Speedup Factor:          %.2fx\n", float64(totalPgTime)/float64(totalFastTime))
		} else {
			fmt.Printf("Overall Performance:     %.2f%% slower\n", -improvement)
		}
	}
	fmt.Println("================================================================================")
}

func main() {
	fmt.Println("FastPostgres vs PostgreSQL - Aggregation Benchmark")
	fmt.Println("================================================================================")

	fmt.Println("\nConnecting to databases...")
	fastDB, err := connectDB("localhost", 5433, "fastpostgres")
	if err != nil {
		log.Fatalf("Failed to connect to FastPostgres: %v", err)
	}
	defer fastDB.Close()
	fmt.Println("✓ Connected to FastPostgres (port 5433)")

	pgDB, err := connectDB("localhost", 5432, "fastpostgres")
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer pgDB.Close()
	fmt.Println("✓ Connected to PostgreSQL (port 5432)")

	fmt.Println("\nSetting up test data...")

	err = createTables(fastDB, "FastPostgres")
	if err != nil {
		log.Fatalf("Failed to create tables in FastPostgres: %v", err)
	}

	err = createTables(pgDB, "PostgreSQL")
	if err != nil {
		log.Fatalf("Failed to create tables in PostgreSQL: %v", err)
	}

	rand.Seed(time.Now().UnixNano())

	err = insertData(fastDB, "FastPostgres")
	if err != nil {
		log.Fatalf("Failed to insert data into FastPostgres: %v", err)
	}

	rand.Seed(time.Now().UnixNano())

	err = insertData(pgDB, "PostgreSQL")
	if err != nil {
		log.Fatalf("Failed to insert data into PostgreSQL: %v", err)
	}

	fmt.Println("\nStarting aggregation benchmarks...\n")
	time.Sleep(2 * time.Second)

	runBenchmarks(fastDB, pgDB)

	fmt.Println("\nBenchmark completed!")
}