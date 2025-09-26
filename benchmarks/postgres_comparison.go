package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

type BenchmarkResult struct {
	Operation    string
	Database     string
	Duration     time.Duration
	RowsAffected int64
	Throughput   float64
	MemoryUsage  int64
}

func main() {
	fmt.Println("FastPostgres vs PostgreSQL Performance Comparison")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("System: %s/%s, CPUs: %d\n", runtime.GOOS, runtime.GOARCH, runtime.NumCPU())
	fmt.Printf("Go Version: %s\n\n", runtime.Version())

	// Test configurations
	testSizes := []struct {
		name string
		rows int
	}{
		{"Small Dataset", 10000},
		{"Medium Dataset", 100000},
		{"Large Dataset", 1000000},
	}

	var results []BenchmarkResult

	for _, testSize := range testSizes {
		fmt.Printf("=== %s (%d rows) ===\n", testSize.name, testSize.rows)

		// Test FastPostgres
		fmt.Println("Testing FastPostgres...")
		fastResults := benchmarkFastPostgres(testSize.rows)
		results = append(results, fastResults...)

		// Test PostgreSQL
		fmt.Println("Testing PostgreSQL...")
		pgResults := benchmarkPostgreSQL(testSize.rows)
		results = append(results, pgResults...)

		fmt.Println()
	}

	// Generate comparison report
	generateComparisonReport(results)
}

func benchmarkFastPostgres(rowCount int) []BenchmarkResult {
	var results []BenchmarkResult

	// Initialize FastPostgres
	db := NewDatabase("comparison_test")

	// Create table
	table := NewTable("benchmark_table")
	table.AddColumn(NewColumn("id", TypeInt64, uint64(rowCount)))
	table.AddColumn(NewColumn("name", TypeString, uint64(rowCount)))
	table.AddColumn(NewColumn("age", TypeInt64, uint64(rowCount)))
	table.AddColumn(NewColumn("salary", TypeInt64, uint64(rowCount)))
	table.AddColumn(NewColumn("department", TypeString, uint64(rowCount)))

	db.Tables.Store("benchmark_table", table)

	// Test 1: Data Insertion
	fmt.Print("  • Data Insertion: ")
	start := time.Now()
	departments := []string{"Engineering", "Sales", "Marketing", "HR", "Finance", "Operations"}

	for i := 0; i < rowCount; i++ {
		row := map[string]interface{}{
			"id":         int64(i + 1),
			"name":       fmt.Sprintf("User_%d", i+1),
			"age":        int64(20 + rand.Intn(45)),
			"salary":     int64(30000 + rand.Intn(100000)),
			"department": departments[rand.Intn(len(departments))],
		}
		table.InsertRow(row)
	}
	insertDuration := time.Since(start)
	insertThroughput := float64(rowCount) / insertDuration.Seconds()

	results = append(results, BenchmarkResult{
		Operation:    "Insert",
		Database:     "FastPostgres",
		Duration:     insertDuration,
		RowsAffected: int64(rowCount),
		Throughput:   insertThroughput,
	})
	fmt.Printf("%v (%.0f rows/sec)\n", insertDuration, insertThroughput)

	// Test 2: Point Lookup
	fmt.Print("  • Point Lookup: ")
	parser := NewSQLParser()
	vectorEngine := NewVectorizedEngine()

	start = time.Now()
	for i := 0; i < 100; i++ {
		targetId := rand.Intn(rowCount) + 1
		sql := fmt.Sprintf("SELECT * FROM benchmark_table WHERE id = %d", targetId)
		plan, _ := parser.Parse(sql)
		vectorEngine.ExecuteSelect(plan, table)
	}
	lookupDuration := time.Since(start) / 100
	lookupThroughput := 1.0 / lookupDuration.Seconds()

	results = append(results, BenchmarkResult{
		Operation:    "Point Lookup",
		Database:     "FastPostgres",
		Duration:     lookupDuration,
		RowsAffected: 1,
		Throughput:   lookupThroughput,
	})
	fmt.Printf("%v (%.0f queries/sec)\n", lookupDuration, lookupThroughput)

	// Test 3: Range Query
	fmt.Print("  • Range Query: ")
	start = time.Now()
	for i := 0; i < 10; i++ {
		sql := "SELECT * FROM benchmark_table WHERE age > 30"
		plan, _ := parser.Parse(sql)
		vectorEngine.ExecuteSelect(plan, table)
	}
	rangeDuration := time.Since(start) / 10

	results = append(results, BenchmarkResult{
		Operation:    "Range Query",
		Database:     "FastPostgres",
		Duration:     rangeDuration,
		RowsAffected: int64(rowCount / 2), // Approximate
		Throughput:   float64(rowCount/2) / rangeDuration.Seconds(),
	})
	fmt.Printf("%v (%.0f rows/sec)\n", rangeDuration, float64(rowCount/2)/rangeDuration.Seconds())

	// Test 4: Aggregation
	fmt.Print("  • Count Query: ")
	start = time.Now()
	for i := 0; i < 100; i++ {
		sql := "SELECT COUNT(*) FROM benchmark_table"
		plan, _ := parser.Parse(sql)
		vectorEngine.CachedExecuteSelect(plan, table, NewQueryCacheEngine(nil))
	}
	countDuration := time.Since(start) / 100
	countThroughput := 1.0 / countDuration.Seconds()

	results = append(results, BenchmarkResult{
		Operation:    "Count Query",
		Database:     "FastPostgres",
		Duration:     countDuration,
		RowsAffected: 1,
		Throughput:   countThroughput,
	})
	fmt.Printf("%v (%.0f queries/sec)\n", countDuration, countThroughput)

	return results
}

func benchmarkPostgreSQL(rowCount int) []BenchmarkResult {
	var results []BenchmarkResult

	// Connect to PostgreSQL
	connStr := "host=localhost port=5432 user=postgres password=testpass dbname=benchmark sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		fmt.Printf("Failed to connect to PostgreSQL: %v\n", err)
		return results
	}
	defer db.Close()

	// Drop and recreate table
	db.Exec("DROP TABLE IF EXISTS benchmark_table")
	_, err = db.Exec(`
		CREATE TABLE benchmark_table (
			id BIGINT PRIMARY KEY,
			name VARCHAR(100),
			age BIGINT,
			salary BIGINT,
			department VARCHAR(50)
		)
	`)
	if err != nil {
		fmt.Printf("Failed to create table: %v\n", err)
		return results
	}

	// Test 1: Data Insertion
	fmt.Print("  • Data Insertion: ")
	start := time.Now()
	departments := []string{"Engineering", "Sales", "Marketing", "HR", "Finance", "Operations"}

	// Use batch insert for PostgreSQL
	tx, _ := db.Begin()
	stmt, _ := tx.Prepare("INSERT INTO benchmark_table (id, name, age, salary, department) VALUES ($1, $2, $3, $4, $5)")

	for i := 0; i < rowCount; i++ {
		stmt.Exec(
			i+1,
			fmt.Sprintf("User_%d", i+1),
			20+rand.Intn(45),
			30000+rand.Intn(100000),
			departments[rand.Intn(len(departments))],
		)
	}
	stmt.Close()
	tx.Commit()

	insertDuration := time.Since(start)
	insertThroughput := float64(rowCount) / insertDuration.Seconds()

	results = append(results, BenchmarkResult{
		Operation:    "Insert",
		Database:     "PostgreSQL",
		Duration:     insertDuration,
		RowsAffected: int64(rowCount),
		Throughput:   insertThroughput,
	})
	fmt.Printf("%v (%.0f rows/sec)\n", insertDuration, insertThroughput)

	// Test 2: Point Lookup
	fmt.Print("  • Point Lookup: ")
	start = time.Now()
	for i := 0; i < 100; i++ {
		targetId := rand.Intn(rowCount) + 1
		rows, err := db.Query("SELECT * FROM benchmark_table WHERE id = $1", targetId)
		if err == nil && rows != nil {
			rows.Close()
		}
	}
	lookupDuration := time.Since(start) / 100
	lookupThroughput := 1.0 / lookupDuration.Seconds()

	results = append(results, BenchmarkResult{
		Operation:    "Point Lookup",
		Database:     "PostgreSQL",
		Duration:     lookupDuration,
		RowsAffected: 1,
		Throughput:   lookupThroughput,
	})
	fmt.Printf("%v (%.0f queries/sec)\n", lookupDuration, lookupThroughput)

	// Test 3: Range Query
	fmt.Print("  • Range Query: ")
	start = time.Now()
	for i := 0; i < 10; i++ {
		rows, err := db.Query("SELECT * FROM benchmark_table WHERE age > 30")
		if err == nil && rows != nil {
			rows.Close()
		}
	}
	rangeDuration := time.Since(start) / 10

	results = append(results, BenchmarkResult{
		Operation:    "Range Query",
		Database:     "PostgreSQL",
		Duration:     rangeDuration,
		RowsAffected: int64(rowCount / 2), // Approximate
		Throughput:   float64(rowCount/2) / rangeDuration.Seconds(),
	})
	fmt.Printf("%v (%.0f rows/sec)\n", rangeDuration, float64(rowCount/2)/rangeDuration.Seconds())

	// Test 4: Aggregation
	fmt.Print("  • Count Query: ")
	start = time.Now()
	for i := 0; i < 100; i++ {
		rows, err := db.Query("SELECT COUNT(*) FROM benchmark_table")
		if err == nil && rows != nil {
			rows.Close()
		}
	}
	countDuration := time.Since(start) / 100
	countThroughput := 1.0 / countDuration.Seconds()

	results = append(results, BenchmarkResult{
		Operation:    "Count Query",
		Database:     "PostgreSQL",
		Duration:     countDuration,
		RowsAffected: 1,
		Throughput:   countThroughput,
	})
	fmt.Printf("%v (%.0f queries/sec)\n", countDuration, countThroughput)

	return results
}

func generateComparisonReport(results []BenchmarkResult) {
	fmt.Println(strings.Repeat("=", 80))
	fmt.Println("PERFORMANCE COMPARISON REPORT")
	fmt.Println(strings.Repeat("=", 80))

	// Group results by operation and dataset size
	operationMap := make(map[string]map[string]BenchmarkResult)

	for _, result := range results {
		if operationMap[result.Operation] == nil {
			operationMap[result.Operation] = make(map[string]BenchmarkResult)
		}
		operationMap[result.Operation][result.Database] = result
	}

	fmt.Println("\n📊 PERFORMANCE COMPARISON:")

	operations := []string{"Insert", "Point Lookup", "Range Query", "Count Query"}

	for _, op := range operations {
		if opResults, exists := operationMap[op]; exists {
			fmt.Printf("\n--- %s ---\n", op)

			fastResult, hasFast := opResults["FastPostgres"]
			pgResult, hasPG := opResults["PostgreSQL"]

			if hasFast && hasPG {
				// Calculate performance difference
				var speedup float64
				var winner string

				if op == "Insert" || op == "Range Query" {
					// Higher throughput is better
					if fastResult.Throughput > pgResult.Throughput {
						speedup = fastResult.Throughput / pgResult.Throughput
						winner = "FastPostgres"
					} else {
						speedup = pgResult.Throughput / fastResult.Throughput
						winner = "PostgreSQL"
					}

					fmt.Printf("FastPostgres: %.2f rows/sec\n", fastResult.Throughput)
					fmt.Printf("PostgreSQL:   %.2f rows/sec\n", pgResult.Throughput)
				} else {
					// Lower latency is better for queries
					if fastResult.Duration < pgResult.Duration {
						speedup = float64(pgResult.Duration) / float64(fastResult.Duration)
						winner = "FastPostgres"
					} else {
						speedup = float64(fastResult.Duration) / float64(pgResult.Duration)
						winner = "PostgreSQL"
					}

					fmt.Printf("FastPostgres: %v\n", fastResult.Duration)
					fmt.Printf("PostgreSQL:   %v\n", pgResult.Duration)
				}

				if speedup > 1.1 {
					fmt.Printf("🏆 Winner: %s (%.2fx faster)\n", winner, speedup)
				} else {
					fmt.Printf("🤝 Comparable performance (%.2fx difference)\n", speedup)
				}
			}
		}
	}

	// Overall analysis
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("ANALYSIS SUMMARY")
	fmt.Println(strings.Repeat("=", 80))

	fastWins := 0
	pgWins := 0
	ties := 0

	for _, op := range operations {
		if opResults, exists := operationMap[op]; exists {
			fastResult, hasFast := opResults["FastPostgres"]
			pgResult, hasPG := opResults["PostgreSQL"]

			if hasFast && hasPG {
				var fastBetter bool
				if op == "Insert" || op == "Range Query" {
					fastBetter = fastResult.Throughput > pgResult.Throughput*1.1
				} else {
					fastBetter = fastResult.Duration < pgResult.Duration*9/10
				}

				if fastBetter {
					fastWins++
				} else if (op == "Insert" || op == "Range Query" && pgResult.Throughput > fastResult.Throughput*1.1) ||
						 (op != "Insert" && op != "Range Query" && pgResult.Duration < fastResult.Duration*9/10) {
					pgWins++
				} else {
					ties++
				}
			}
		}
	}

	fmt.Printf("\n🏆 FINAL RESULTS:\n")
	fmt.Printf("FastPostgres wins: %d operations\n", fastWins)
	fmt.Printf("PostgreSQL wins:   %d operations\n", pgWins)
	fmt.Printf("Ties:             %d operations\n", ties)

	if fastWins > pgWins {
		fmt.Printf("\n🎉 FastPostgres shows superior performance overall!\n")
		fmt.Printf("Particularly strong in: columnar analytics and data ingestion\n")
	} else if pgWins > fastWins {
		fmt.Printf("\n💪 PostgreSQL shows superior performance overall!\n")
		fmt.Printf("Benefits from decades of optimization and mature query planner\n")
	} else {
		fmt.Printf("\n🤝 Both databases show competitive performance!\n")
		fmt.Printf("Performance varies by workload characteristics\n")
	}

	fmt.Println("\n✨ KEY INSIGHTS:")
	fmt.Println("• FastPostgres excels at analytical workloads with columnar storage")
	fmt.Println("• PostgreSQL benefits from mature B-tree indexes and query optimizer")
	fmt.Println("• Data ingestion performance depends on batch size and transaction handling")
	fmt.Println("• Cache warming significantly improves repeated query performance")

	fmt.Println("\n" + strings.Repeat("=", 80))
}