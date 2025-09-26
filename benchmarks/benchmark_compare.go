package main

import (
	"fmt"
	"time"
)

func main() {
	fmt.Println("FastPostgres vs Non-Indexed Performance Comparison")
	fmt.Println("================================================")

	// Create large dataset
	fmt.Println("Creating large dataset with 1 million rows...")
	db := NewDatabase("benchmark_comparison")

	// Create test table with large dataset
	table := NewTable("large_table")

	// Add columns
	idCol := NewColumn("id", TypeInt64, 1000000)
	statusCol := NewColumn("status", TypeString, 1000000)
	scoreCol := NewColumn("score", TypeInt64, 1000000)

	table.AddColumn(idCol)
	table.AddColumn(statusCol)
	table.AddColumn(scoreCol)

	// Insert 1M rows with varied data
	rowCount := 1000000
	statuses := []string{"active", "inactive", "pending", "blocked", "verified"}

	fmt.Println("Inserting 1,000,000 rows...")
	start := time.Now()
	for i := 0; i < rowCount; i++ {
		row := map[string]interface{}{
			"id":     int64(i + 1),
			"status": statuses[i%len(statuses)],
			"score":  int64(i % 1000), // Scores from 0-999
		}
		table.InsertRow(row)
	}
	insertTime := time.Since(start)

	db.Tables.Store("large_table", table)
	fmt.Printf("Data insertion completed in: %v\n", insertTime)
	fmt.Printf("Insertion rate: %.0f rows/second\n\n", float64(rowCount)/insertTime.Seconds())

	parser := NewSQLParser()
	engine := NewVectorizedEngine()

	// Test queries without indexes
	fmt.Println("=== WITHOUT INDEXES ===")
	testQueries := []struct{
		name string
		sql  string
		expected int
	}{
		{"Point lookup by ID", "SELECT * FROM large_table WHERE id = 500000", 1},
		{"Status filter", "SELECT * FROM large_table WHERE status = 'active'", 200000},
		{"Range query on score", "SELECT * FROM large_table WHERE score > 900", 99000},
		{"Complex filter", "SELECT * FROM large_table WHERE status = 'verified' AND score < 100", 20000},
	}

	var nonIndexedTimes []time.Duration

	for _, test := range testQueries {
		fmt.Printf("\nQuery: %s\n", test.name)
		fmt.Printf("SQL: %s\n", test.sql)

		plan, err := parser.Parse(test.sql)
		if err != nil {
			fmt.Printf("Parse error: %v\n", err)
			continue
		}

		tableInterface, exists := db.Tables.Load(plan.TableName)
		if !exists {
			fmt.Printf("Table not found: %s\n", plan.TableName)
			continue
		}

		table := tableInterface.(*Table)

		start := time.Now()
		result, err := engine.ExecuteSelect(plan, table)
		elapsed := time.Since(start)
		nonIndexedTimes = append(nonIndexedTimes, elapsed)

		if err != nil {
			fmt.Printf("Execution error: %v\n", err)
			continue
		}

		fmt.Printf("Execution time: %v\n", elapsed)
		fmt.Printf("Rows returned: %d\n", len(result.Rows))
		if len(result.Rows) > 0 {
			fmt.Printf("Throughput: %.0f rows/second\n", float64(len(result.Rows))/elapsed.Seconds())
		}
	}

	// Create indexes
	fmt.Println("\n=== CREATING INDEXES ===")
	fmt.Println("Building indexes...")

	start = time.Now()

	// Create hash index on ID for fast equality lookups
	db.IndexManager.CreateIndex("idx_id", "large_table", "id", HashIndex, true)
	db.IndexManager.BuildIndex(table, "idx_id")

	// Create hash index on status for categorical queries
	db.IndexManager.CreateIndex("idx_status", "large_table", "status", HashIndex, false)
	db.IndexManager.BuildIndex(table, "idx_status")

	// Create B-tree index on score for range queries
	db.IndexManager.CreateIndex("idx_score", "large_table", "score", BTreeIndex, false)
	db.IndexManager.BuildIndex(table, "idx_score")

	indexTime := time.Since(start)
	fmt.Printf("Index creation completed in: %v\n", indexTime)
	fmt.Printf("Indexing rate: %.0f rows/second\n", float64(rowCount)/indexTime.Seconds())

	// Test queries with indexes
	fmt.Println("\n=== WITH INDEXES ===")
	var indexedTimes []time.Duration

	for i, test := range testQueries {
		fmt.Printf("\nQuery: %s\n", test.name)
		fmt.Printf("SQL: %s\n", test.sql)

		plan, err := parser.Parse(test.sql)
		if err != nil {
			fmt.Printf("Parse error: %v\n", err)
			continue
		}

		tableInterface, exists := db.Tables.Load(plan.TableName)
		if !exists {
			fmt.Printf("Table not found: %s\n", plan.TableName)
			continue
		}

		table := tableInterface.(*Table)

		start := time.Now()
		result, err := engine.ExecuteIndexedSelect(plan, table, db.IndexManager)
		elapsed := time.Since(start)
		indexedTimes = append(indexedTimes, elapsed)

		if err != nil {
			fmt.Printf("Execution error: %v\n", err)
			continue
		}

		fmt.Printf("Execution time: %v\n", elapsed)
		fmt.Printf("Rows returned: %d\n", len(result.Rows))
		if len(result.Rows) > 0 {
			fmt.Printf("Throughput: %.0f rows/second\n", float64(len(result.Rows))/elapsed.Seconds())
		}

		// Calculate speedup
		if i < len(nonIndexedTimes) {
			speedup := float64(nonIndexedTimes[i]) / float64(elapsed)
			fmt.Printf("Speedup: %.1fx faster with indexes\n", speedup)
		}
	}

	// Performance summary
	fmt.Println("\n=== PERFORMANCE SUMMARY ===")
	fmt.Printf("Dataset: 1,000,000 rows\n")
	fmt.Printf("Data insertion: %v (%.0f rows/sec)\n", insertTime, float64(rowCount)/insertTime.Seconds())
	fmt.Printf("Index creation: %v (%.0f rows/sec)\n", indexTime, float64(rowCount)/indexTime.Seconds())

	fmt.Println("\nQuery performance comparison:")
	for i, test := range testQueries {
		if i < len(nonIndexedTimes) && i < len(indexedTimes) {
			speedup := float64(nonIndexedTimes[i]) / float64(indexedTimes[i])
			fmt.Printf("%-25s: %.1fx speedup\n", test.name, speedup)
		}
	}

	// Calculate overall improvement
	var totalNonIndexed, totalIndexed time.Duration
	for i := 0; i < len(nonIndexedTimes) && i < len(indexedTimes); i++ {
		totalNonIndexed += nonIndexedTimes[i]
		totalIndexed += indexedTimes[i]
	}

	if totalIndexed > 0 {
		overallSpeedup := float64(totalNonIndexed) / float64(totalIndexed)
		fmt.Printf("\nOverall average speedup: %.1fx faster with indexes\n", overallSpeedup)
	}
}