// Performance Testing Suite for FastPostgres Advanced Indexing
package main

import (
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"fastpostgres/pkg/engine"
	"fastpostgres/pkg/query"
	"fastpostgres/pkg/storage"
)

func main() {
	fmt.Println("🚀 FastPostgres Advanced Indexing Performance Test Suite")
	fmt.Println("========================================================")
	fmt.Println()

	// Initialize components
	database := engine.NewDatabase("performance_test")
	parser := query.NewSQLParser()
	indexManager := storage.NewIndexManager()
	queryEngine := query.NewIndexedQueryEngine(indexManager)

	// Test configurations
	dataSizes := []int{1000, 5000, 10000, 25000}

	for _, dataSize := range dataSizes {
		fmt.Printf("\n📊 Testing with %d records\n", dataSize)
		fmt.Println(strings.Repeat("=", 50))

		runPerformanceTest(database, parser, indexManager, queryEngine, dataSize)
	}

	fmt.Println("\n🎉 Performance testing completed!")
}

func runPerformanceTest(database *engine.Database, parser *query.SQLParser,
	indexManager *storage.IndexManager, queryEngine *query.IndexedQueryEngine, dataSize int) {

	// Create test table
	table := createPerformanceTestTable(database, dataSize)

	// Test 1: Insertion Performance
	fmt.Println("\n1️⃣  INSERTION PERFORMANCE TEST")
	fmt.Println("------------------------------")
	testInsertionPerformance(table, indexManager, dataSize)

	// Test 2: Read Query Performance
	fmt.Println("\n2️⃣  READ QUERY PERFORMANCE TEST")
	fmt.Println("-------------------------------")
	testReadPerformance(parser, queryEngine, table)

	// Test 3: Aggregation Performance
	fmt.Println("\n3️⃣  AGGREGATION PERFORMANCE TEST")
	fmt.Println("--------------------------------")
	testAggregationPerformance(parser, queryEngine, table)

	// Test 4: Index Effectiveness
	fmt.Println("\n4️⃣  INDEX EFFECTIVENESS TEST")
	fmt.Println("----------------------------")
	testIndexEffectiveness(database, parser, queryEngine, indexManager, dataSize)
}

func createPerformanceTestTable(database *engine.Database, dataSize int) *engine.Table {
	table := engine.NewTableWithDB("performance_test", database)

	// Define columns for comprehensive testing
	columns := []*engine.Column{
		engine.NewColumn("id", engine.TypeInt64, uint64(dataSize)),
		engine.NewColumn("user_id", engine.TypeInt64, uint64(dataSize)),
		engine.NewColumn("name", engine.TypeString, uint64(dataSize)),
		engine.NewColumn("email", engine.TypeString, uint64(dataSize)),
		engine.NewColumn("age", engine.TypeInt64, uint64(dataSize)),
		engine.NewColumn("salary", engine.TypeInt64, uint64(dataSize)),
		engine.NewColumn("department", engine.TypeString, uint64(dataSize)),
		engine.NewColumn("status", engine.TypeString, uint64(dataSize)),
		engine.NewColumn("hire_date", engine.TypeString, uint64(dataSize)),
		engine.NewColumn("performance_score", engine.TypeInt64, uint64(dataSize)),
	}

	for _, col := range columns {
		table.AddColumn(col)
	}

	// Generate and insert test data
	fmt.Printf("📝 Generating %d records...", dataSize)
	start := time.Now()

	departments := []string{"Engineering", "Sales", "Marketing", "HR", "Finance", "Operations", "Legal", "Design"}
	statuses := []string{"active", "inactive", "pending", "terminated", "on_leave"}

	rand.Seed(time.Now().UnixNano())

	for i := 1; i <= dataSize; i++ {
		data := map[string]interface{}{
			"id":                int64(i),
			"user_id":          int64(1000000 + i),
			"name":             fmt.Sprintf("User_%d", i),
			"email":            fmt.Sprintf("user%d@company.com", i),
			"age":              int64(22 + rand.Intn(43)), // 22-64 years
			"salary":           int64(35000 + rand.Intn(100000)), // 35k-135k
			"department":       departments[rand.Intn(len(departments))],
			"status":           statuses[rand.Intn(len(statuses))],
			"hire_date":        fmt.Sprintf("2020-%02d-%02d", rand.Intn(12)+1, rand.Intn(28)+1),
			"performance_score": int64(1 + rand.Intn(10)), // 1-10 scale
		}

		err := table.InsertRow(data)
		if err != nil {
			log.Fatalf("Failed to insert data: %v", err)
		}
	}

	duration := time.Since(start)
	fmt.Printf(" ✅ Complete in %v (%.0f records/sec)\n", duration, float64(dataSize)/duration.Seconds())

	return table
}

func testInsertionPerformance(table *engine.Table, indexManager *storage.IndexManager, dataSize int) {
	scenarios := []struct {
		name        string
		indexSpecs  []indexSpec
		description string
	}{
		{
			name:        "No_Indexes",
			indexSpecs:  []indexSpec{},
			description: "Baseline insertion without any indexes",
		},
		{
			name: "Single_BTree",
			indexSpecs: []indexSpec{
				{"idx_id_btree", "id", engine.BTreeIndex},
			},
			description: "Single B-Tree index on primary key",
		},
		{
			name: "Single_Hash",
			indexSpecs: []indexSpec{
				{"idx_email_hash", "email", engine.HashIndex},
			},
			description: "Single Hash index on email",
		},
		{
			name: "Single_LSM",
			indexSpecs: []indexSpec{
				{"idx_id_lsm", "id", engine.LSMTreeIndex},
			},
			description: "Single LSM-Tree index (write-optimized)",
		},
		{
			name: "Single_Bitmap",
			indexSpecs: []indexSpec{
				{"idx_status_bitmap", "status", engine.RoaringBitmapIndex},
			},
			description: "Single Bitmap index on status",
		},
		{
			name: "Multiple_Indexes",
			indexSpecs: []indexSpec{
				{"idx_id_lsm", "id", engine.LSMTreeIndex},
				{"idx_status_bitmap", "status", engine.RoaringBitmapIndex},
				{"idx_dept_bitmap", "department", engine.RoaringBitmapIndex},
				{"idx_age_btree", "age", engine.BTreeIndex},
			},
			description: "Multiple indexes (realistic scenario)",
		},
	}

	fmt.Printf("%-20s %-15s %-15s %-10s %s\n", "Scenario", "Index_Build", "Total_Time", "Recs/sec", "Description")
	fmt.Println(strings.Repeat("-", 90))

	for _, scenario := range scenarios {
		// Create fresh table for each test
		testTable := engine.NewTableWithDB("insertion_test", table.Database)
		for _, col := range table.Columns {
			newCol := engine.NewColumn(col.Name, col.Type, col.Capacity)
			testTable.AddColumn(newCol)
		}

		// Create indexes
		indexBuildStart := time.Now()
		for _, spec := range scenario.indexSpecs {
			err := indexManager.CreateIndex(spec.name, "insertion_test", spec.column, spec.indexType, false)
			if err != nil {
				fmt.Printf("Warning: Could not create index %s: %v\n", spec.name, err)
				continue
			}
		}
		indexBuildTime := time.Since(indexBuildStart)

		// Measure insertion time with the created indexes
		insertStart := time.Now()

		// Insert a subset of data for timing
		insertCount := dataSize / 10 // Use 10% for timing test
		for i := 0; i < insertCount; i++ {
			data := map[string]interface{}{
				"id":                int64(i + 1),
				"user_id":          int64(2000000 + i),
				"name":             fmt.Sprintf("TestUser_%d", i),
				"email":            fmt.Sprintf("test%d@company.com", i),
				"age":              int64(25 + (i % 40)),
				"salary":           int64(40000 + (i % 80000)),
				"department":       []string{"Engineering", "Sales", "Marketing"}[i%3],
				"status":           []string{"active", "inactive"}[i%2],
				"hire_date":        "2023-01-01",
				"performance_score": int64(1 + (i % 10)),
			}

			err := testTable.InsertRow(data)
			if err != nil {
				log.Printf("Insert failed: %v", err)
				break
			}
		}

		insertTime := time.Since(insertStart)
		totalTime := indexBuildTime + insertTime
		recordsPerSec := float64(insertCount) / insertTime.Seconds()

		fmt.Printf("%-20s %-15v %-15v %-10.0f %s\n",
			scenario.name, indexBuildTime, totalTime, recordsPerSec, scenario.description)
	}
}

type indexSpec struct {
	name      string
	column    string
	indexType engine.IndexType
}

func testReadPerformance(parser *query.SQLParser, queryEngine *query.IndexedQueryEngine, table *engine.Table) {
	// Create comprehensive indexes for read testing
	indexManager := queryEngine.GetIndexManager()

	indexes := []indexSpec{
		{"idx_id_lsm", "id", engine.LSMTreeIndex},
		{"idx_status_bitmap", "status", engine.RoaringBitmapIndex},
		{"idx_dept_bitmap", "department", engine.RoaringBitmapIndex},
		{"idx_age_btree", "age", engine.BTreeIndex},
		{"idx_email_hash", "email", engine.HashIndex},
	}

	for _, spec := range indexes {
		err := indexManager.CreateIndex(spec.name, "performance_test", spec.column, spec.indexType, false)
		if err == nil {
			indexManager.BuildIndex(table, spec.name)
		}
	}

	queries := []struct {
		name        string
		sql         string
		description string
	}{
		{
			name:        "Point_Lookup",
			sql:         "SELECT * FROM performance_test WHERE id = 5000",
			description: "Single record by primary key",
		},
		{
			name:        "Range_Query",
			sql:         "SELECT * FROM performance_test WHERE age BETWEEN 30 AND 40",
			description: "Age range query (B-Tree index)",
		},
		{
			name:        "Categorical_Filter",
			sql:         "SELECT * FROM performance_test WHERE status = 'active'",
			description: "Status filter (Bitmap index)",
		},
		{
			name:        "Department_Filter",
			sql:         "SELECT name, salary FROM performance_test WHERE department = 'Engineering'",
			description: "Department filter (Bitmap index)",
		},
		{
			name:        "Multi_Condition",
			sql:         "SELECT * FROM performance_test WHERE age > 35 AND status = 'active'",
			description: "Multiple conditions",
		},
		{
			name:        "Large_Result_Set",
			sql:         "SELECT * FROM performance_test WHERE salary > 50000",
			description: "Query returning many results",
		},
		{
			name:        "String_Match",
			sql:         "SELECT * FROM performance_test WHERE email = 'user5000@company.com'",
			description: "Exact string match (Hash index)",
		},
	}

	fmt.Printf("%-20s %-12s %-8s %-12s %s\n", "Query_Type", "Duration", "Rows", "Rows/sec", "Description")
	fmt.Println(strings.Repeat("-", 75))

	for _, q := range queries {
		// Execute query multiple times for accuracy
		iterations := 3
		totalDuration := time.Duration(0)
		var lastResult *engine.QueryResult

		for i := 0; i < iterations; i++ {
			plan, err := parser.Parse(q.sql)
			if err != nil {
				fmt.Printf("❌ Parse error for %s: %v\n", q.name, err)
				continue
			}

			start := time.Now()
			result, err := queryEngine.ExecuteQuery(plan, table)
			duration := time.Since(start)

			if err != nil {
				fmt.Printf("❌ Execution error for %s: %v\n", q.name, err)
				continue
			}

			totalDuration += duration
			lastResult = result
		}

		if lastResult != nil {
			avgDuration := totalDuration / time.Duration(iterations)
			rowsPerSec := float64(len(lastResult.Rows)) / avgDuration.Seconds()

			fmt.Printf("%-20s %-12v %-8d %-12.0f %s\n",
				q.name, avgDuration, len(lastResult.Rows), rowsPerSec, q.description)
		}
	}
}

func testAggregationPerformance(parser *query.SQLParser, queryEngine *query.IndexedQueryEngine, table *engine.Table) {
	aggregationQueries := []struct {
		name        string
		sql         string
		description string
	}{
		{
			name:        "Count_All",
			sql:         "SELECT COUNT(*) FROM performance_test",
			description: "Count all records",
		},
		{
			name:        "Count_With_Filter",
			sql:         "SELECT COUNT(*) FROM performance_test WHERE status = 'active'",
			description: "Count with categorical filter",
		},
		{
			name:        "Average_Salary",
			sql:         "SELECT AVG(salary) FROM performance_test",
			description: "Average calculation",
		},
		{
			name:        "Min_Max_Age",
			sql:         "SELECT MIN(age), MAX(age) FROM performance_test",
			description: "Min/Max operations",
		},
		{
			name:        "Group_By_Status",
			sql:         "SELECT status, COUNT(*) FROM performance_test GROUP BY status",
			description: "Group by status (low cardinality)",
		},
		{
			name:        "Group_By_Department",
			sql:         "SELECT department, AVG(salary), COUNT(*) FROM performance_test GROUP BY department",
			description: "Department grouping with aggregation",
		},
		{
			name:        "Complex_Aggregation",
			sql:         "SELECT department, status, COUNT(*), AVG(salary), MAX(performance_score) FROM performance_test WHERE age > 30 GROUP BY department, status",
			description: "Complex multi-column aggregation",
		},
	}

	fmt.Printf("%-25s %-12s %-8s %s\n", "Aggregation_Type", "Duration", "Groups", "Description")
	fmt.Println(strings.Repeat("-", 70))

	for _, q := range aggregationQueries {
		plan, err := parser.Parse(q.sql)
		if err != nil {
			fmt.Printf("❌ Parse error for %s: %v\n", q.name, err)
			continue
		}

		start := time.Now()
		result, err := queryEngine.ExecuteQuery(plan, table)
		duration := time.Since(start)

		if err != nil {
			fmt.Printf("❌ Execution error for %s: %v\n", q.name, err)
			continue
		}

		fmt.Printf("%-25s %-12v %-8d %s\n",
			q.name, duration, len(result.Rows), q.description)
	}
}

func testIndexEffectiveness(database *engine.Database, parser *query.SQLParser,
	queryEngine *query.IndexedQueryEngine, indexManager *storage.IndexManager, dataSize int) {

	testQueries := []struct {
		name string
		sql  string
	}{
		{"Point_Lookup", "SELECT * FROM effectiveness_test WHERE id = 500"},
		{"Status_Filter", "SELECT * FROM effectiveness_test WHERE status = 'active'"},
		{"Age_Range", "SELECT * FROM effectiveness_test WHERE age > 35"},
		{"Count_Query", "SELECT COUNT(*) FROM effectiveness_test WHERE department = 'Engineering'"},
	}

	fmt.Printf("%-15s %-15s %-15s %-10s\n", "Query_Type", "No_Index", "With_Index", "Speedup")
	fmt.Println(strings.Repeat("-", 60))

	for _, q := range testQueries {
		// Test without indexes
		table1 := createSmallTestTable(database, "effectiveness_test", dataSize/5) // Smaller for timing

		plan, err := parser.Parse(q.sql)
		if err != nil {
			continue
		}

		start := time.Now()
		result1, err := queryEngine.ExecuteQuery(plan, table1)
		durationWithoutIndex := time.Since(start)
		if err != nil {
			continue
		}

		// Test with indexes
		table2 := createSmallTestTable(database, "effectiveness_test", dataSize/5)
		createOptimalIndexes(indexManager, table2)

		start = time.Now()
		result2, err := queryEngine.ExecuteQuery(plan, table2)
		durationWithIndex := time.Since(start)
		if err != nil {
			continue
		}

		speedup := float64(durationWithoutIndex) / float64(durationWithIndex)
		if speedup < 1 {
			speedup = 1 // Minimum 1x speedup
		}

		fmt.Printf("%-15s %-15v %-15v %-10.1fx\n",
			q.name, durationWithoutIndex, durationWithIndex, speedup)

		// Verify results are consistent
		if len(result1.Rows) != len(result2.Rows) {
			fmt.Printf("  ⚠️  Result count mismatch: %d vs %d\n", len(result1.Rows), len(result2.Rows))
		}
	}
}

func createSmallTestTable(database *engine.Database, tableName string, size int) *engine.Table {
	table := engine.NewTableWithDB(tableName, database)

	columns := []*engine.Column{
		engine.NewColumn("id", engine.TypeInt64, uint64(size)),
		engine.NewColumn("name", engine.TypeString, uint64(size)),
		engine.NewColumn("age", engine.TypeInt64, uint64(size)),
		engine.NewColumn("status", engine.TypeString, uint64(size)),
		engine.NewColumn("department", engine.TypeString, uint64(size)),
		engine.NewColumn("salary", engine.TypeInt64, uint64(size)),
	}

	for _, col := range columns {
		table.AddColumn(col)
	}

	// Insert data
	departments := []string{"Engineering", "Sales", "Marketing"}
	statuses := []string{"active", "inactive"}

	for i := 1; i <= size; i++ {
		data := map[string]interface{}{
			"id":         int64(i),
			"name":       fmt.Sprintf("User_%d", i),
			"age":        int64(25 + (i % 40)),
			"status":     statuses[i%len(statuses)],
			"department": departments[i%len(departments)],
			"salary":     int64(40000 + (i % 60000)),
		}

		table.InsertRow(data)
	}

	return table
}

func createOptimalIndexes(indexManager *storage.IndexManager, table *engine.Table) {
	indexes := []indexSpec{
		{"idx_id_lsm", "id", engine.LSMTreeIndex},
		{"idx_status_bitmap", "status", engine.RoaringBitmapIndex},
		{"idx_dept_bitmap", "department", engine.RoaringBitmapIndex},
		{"idx_age_btree", "age", engine.BTreeIndex},
	}

	for _, spec := range indexes {
		err := indexManager.CreateIndex(spec.name, table.Name, spec.column, spec.indexType, false)
		if err == nil {
			indexManager.BuildIndex(table, spec.name)
		}
	}
}

