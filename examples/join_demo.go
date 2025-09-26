package main

import (
	"fmt"
	"strings"
	"time"
)

func main() {
	fmt.Println("FastPostgres JOIN Operations Demo")
	fmt.Println("=================================")

	// Create database with multiple tables
	db := NewDatabase("join_demo")

	// Create users table
	usersTable := NewTable("users")
	usersTable.AddColumn(NewColumn("id", TypeInt64, 10000))
	usersTable.AddColumn(NewColumn("name", TypeString, 10000))
	usersTable.AddColumn(NewColumn("department_id", TypeInt64, 10000))

	// Create departments table
	deptsTable := NewTable("departments")
	deptsTable.AddColumn(NewColumn("id", TypeInt64, 1000))
	deptsTable.AddColumn(NewColumn("dept_name", TypeString, 1000))
	deptsTable.AddColumn(NewColumn("budget", TypeInt64, 1000))

	// Insert users data
	userData := []map[string]interface{}{
		{"id": int64(1), "name": "Alice", "department_id": int64(1)},
		{"id": int64(2), "name": "Bob", "department_id": int64(1)},
		{"id": int64(3), "name": "Charlie", "department_id": int64(2)},
		{"id": int64(4), "name": "Diana", "department_id": int64(2)},
		{"id": int64(5), "name": "Eve", "department_id": int64(3)},
		{"id": int64(6), "name": "Frank", "department_id": int64(3)},
		{"id": int64(7), "name": "Grace", "department_id": int64(1)},
		{"id": int64(8), "name": "Henry", "department_id": int64(2)},
	}

	for _, row := range userData {
		usersTable.InsertRow(row)
	}

	// Insert departments data
	deptData := []map[string]interface{}{
		{"id": int64(1), "dept_name": "Engineering", "budget": int64(1000000)},
		{"id": int64(2), "dept_name": "Sales", "budget": int64(500000)},
		{"id": int64(3), "dept_name": "Marketing", "budget": int64(300000)},
		{"id": int64(4), "dept_name": "HR", "budget": int64(200000)},
	}

	for _, row := range deptData {
		deptsTable.InsertRow(row)
	}

	db.Tables.Store("users", usersTable)
	db.Tables.Store("departments", deptsTable)

	fmt.Printf("Created users table with %d rows\n", usersTable.RowCount)
	fmt.Printf("Created departments table with %d rows\n", deptsTable.RowCount)

	// Create JOIN engine
	joinEngine := NewJoinEngine()

	fmt.Println("\n=== HASH JOIN DEMONSTRATION ===")
	fmt.Println("Query: SELECT users.name, departments.dept_name FROM users JOIN departments ON users.department_id = departments.id")

	// Create join expression for hash join
	hashJoinExpr := &JoinExpression{
		Type:     InnerJoin,
		Table:    "departments",
		LeftCol:  "department_id", // users.department_id
		RightCol: "id",            // departments.id
	}

	start := time.Now()
	hashJoinResult, err := joinEngine.ExecuteHashJoin(usersTable, deptsTable, hashJoinExpr)
	hashJoinTime := time.Since(start)

	if err != nil {
		fmt.Printf("Hash join error: %v\n", err)
	} else {
		fmt.Printf("Hash Join completed in: %v\n", hashJoinTime)
		fmt.Printf("Rows matched: %d\n", hashJoinResult.Stats.RowsMatched)
		fmt.Println("\nResults:")
		printJoinResults(hashJoinResult, 10)
	}

	fmt.Println("\n=== SORT-MERGE JOIN DEMONSTRATION ===")
	fmt.Println("Same query using sort-merge join algorithm")

	start = time.Now()
	sortMergeResult, err := joinEngine.ExecuteSortMergeJoin(usersTable, deptsTable, hashJoinExpr)
	sortMergeTime := time.Since(start)

	if err != nil {
		fmt.Printf("Sort-merge join error: %v\n", err)
	} else {
		fmt.Printf("Sort-Merge Join completed in: %v\n", sortMergeTime)
		fmt.Printf("Rows matched: %d\n", sortMergeResult.Stats.RowsMatched)
		fmt.Println("\nResults:")
		printJoinResults(sortMergeResult, 10)
	}

	fmt.Println("\n=== NESTED LOOP JOIN DEMONSTRATION ===")
	fmt.Println("Same query using nested loop join algorithm")

	start = time.Now()
	nestedLoopResult, err := joinEngine.ExecuteNestedLoopJoin(usersTable, deptsTable, hashJoinExpr)
	nestedLoopTime := time.Since(start)

	if err != nil {
		fmt.Printf("Nested loop join error: %v\n", err)
	} else {
		fmt.Printf("Nested Loop Join completed in: %v\n", nestedLoopTime)
		fmt.Printf("Rows matched: %d\n", nestedLoopResult.Stats.RowsMatched)
		fmt.Println("\nResults:")
		printJoinResults(nestedLoopResult, 10)
	}

	// Performance comparison
	fmt.Println("\n=== PERFORMANCE COMPARISON ===")
	fmt.Printf("Hash Join:        %v\n", hashJoinTime)
	fmt.Printf("Sort-Merge Join:  %v\n", sortMergeTime)
	fmt.Printf("Nested Loop Join: %v\n", nestedLoopTime)

	if hashJoinTime > 0 {
		fmt.Printf("\nSpeedup vs Hash Join:\n")
		if sortMergeTime > 0 {
			fmt.Printf("Sort-Merge: %.1fx %s\n",
				float64(sortMergeTime)/float64(hashJoinTime),
				func() string {
					if sortMergeTime > hashJoinTime { return "slower" } else { return "faster" }
				}())
		}
		if nestedLoopTime > 0 {
			fmt.Printf("Nested Loop: %.1fx %s\n",
				float64(nestedLoopTime)/float64(hashJoinTime),
				func() string {
					if nestedLoopTime > hashJoinTime { return "slower" } else { return "faster" }
				}())
		}
	}

	fmt.Println("\n=== LARGE DATASET BENCHMARK ===")
	benchmarkLargeJoins()
}

func printJoinResults(result *JoinResult, limit int) {
	if len(result.Rows) == 0 {
		fmt.Println("No results")
		return
	}

	// Print header
	for _, col := range result.Columns {
		fmt.Printf("%-20s", col)
	}
	fmt.Println()
	fmt.Println(strings.Repeat("-", len(result.Columns)*20))

	// Print rows (up to limit)
	rowCount := len(result.Rows)
	if limit > 0 && rowCount > limit {
		rowCount = limit
	}

	for i := 0; i < rowCount; i++ {
		for _, value := range result.Rows[i] {
			if value == nil {
				fmt.Printf("%-20s", "NULL")
			} else {
				fmt.Printf("%-20v", value)
			}
		}
		fmt.Println()
	}

	if len(result.Rows) > limit {
		fmt.Printf("... and %d more rows\n", len(result.Rows)-limit)
	}
}

func benchmarkLargeJoins() {
	fmt.Println("Creating large dataset with 50K users and 100 departments...")

	// Create large tables
	largeUsers := NewTable("large_users")
	largeUsers.AddColumn(NewColumn("id", TypeInt64, 50000))
	largeUsers.AddColumn(NewColumn("name", TypeString, 50000))
	largeUsers.AddColumn(NewColumn("department_id", TypeInt64, 50000))

	largeDepts := NewTable("large_departments")
	largeDepts.AddColumn(NewColumn("id", TypeInt64, 100))
	largeDepts.AddColumn(NewColumn("dept_name", TypeString, 100))
	largeDepts.AddColumn(NewColumn("budget", TypeInt64, 100))

	// Generate large user dataset
	start := time.Now()
	for i := 0; i < 50000; i++ {
		row := map[string]interface{}{
			"id":            int64(i + 1),
			"name":          fmt.Sprintf("User_%d", i+1),
			"department_id": int64((i % 100) + 1), // Distribute users across 100 departments
		}
		largeUsers.InsertRow(row)
	}

	// Generate department dataset
	for i := 0; i < 100; i++ {
		row := map[string]interface{}{
			"id":        int64(i + 1),
			"dept_name": fmt.Sprintf("Department_%d", i+1),
			"budget":    int64((i+1) * 10000),
		}
		largeDepts.InsertRow(row)
	}

	dataGenTime := time.Since(start)
	fmt.Printf("Dataset creation completed in: %v\n", dataGenTime)

	joinEngine := NewJoinEngine()
	joinExpr := &JoinExpression{
		Type:     InnerJoin,
		Table:    "large_departments",
		LeftCol:  "department_id",
		RightCol: "id",
	}

	// Benchmark each join type
	fmt.Println("\nBenchmarking join algorithms on large dataset:")

	// Hash Join
	start = time.Now()
	hashResult, err := joinEngine.ExecuteHashJoin(largeUsers, largeDepts, joinExpr)
	hashTime := time.Since(start)

	if err != nil {
		fmt.Printf("Hash Join error: %v\n", err)
	} else {
		fmt.Printf("Hash Join:        %v (%d rows matched, %.0f rows/sec)\n",
			hashTime, hashResult.Stats.RowsMatched, float64(hashResult.Stats.RowsMatched)/hashTime.Seconds())
	}

	// Sort-Merge Join
	start = time.Now()
	sortResult, err := joinEngine.ExecuteSortMergeJoin(largeUsers, largeDepts, joinExpr)
	sortTime := time.Since(start)

	if err != nil {
		fmt.Printf("Sort-Merge Join error: %v\n", err)
	} else {
		fmt.Printf("Sort-Merge Join:  %v (%d rows matched, %.0f rows/sec)\n",
			sortTime, sortResult.Stats.RowsMatched, float64(sortResult.Stats.RowsMatched)/sortTime.Seconds())
	}

	// Skip nested loop join for large dataset (would be too slow)
	fmt.Println("Nested Loop Join: (skipped for large dataset - would be O(n*m))")

	// Performance summary
	fmt.Println("\n=== LARGE DATASET PERFORMANCE SUMMARY ===")
	fmt.Printf("Dataset: 50,000 users × 100 departments = 50,000 joined rows\n")
	if hashTime > 0 && sortTime > 0 {
		if hashTime < sortTime {
			fmt.Printf("Hash Join is %.1fx faster than Sort-Merge Join\n", float64(sortTime)/float64(hashTime))
		} else {
			fmt.Printf("Sort-Merge Join is %.1fx faster than Hash Join\n", float64(hashTime)/float64(sortTime))
		}
	}
}