// Comprehensive SQL Query Test Suite for FastPostgres
package tests

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"fastpostgres/pkg/engine"
	"fastpostgres/pkg/query"
	"fastpostgres/pkg/storage"
)

// TestSuite holds the test infrastructure
type TestSuite struct {
	database     *engine.Database
	parser       *query.SQLParser
	indexManager *storage.IndexManager
	queryEngine  *query.IndexedQueryEngine
}

// SetupTestSuite initializes the test environment
func SetupTestSuite(t *testing.T) *TestSuite {
	database := engine.NewDatabase("test_db")
	parser := query.NewSQLParser()
	indexManager := storage.NewIndexManager()
	queryEngine := query.NewIndexedQueryEngine(indexManager)

	return &TestSuite{
		database:     database,
		parser:       parser,
		indexManager: indexManager,
		queryEngine:  queryEngine,
	}
}

// createTestTable creates a sample table with test data
func (ts *TestSuite) createTestTable(t *testing.T) *engine.Table {
	// Create users table
	table := engine.NewTableWithDB("users", ts.database)

	// Define columns
	idCol := engine.NewColumn("id", engine.TypeInt64, 10000)
	nameCol := engine.NewColumn("name", engine.TypeString, 10000)
	ageCol := engine.NewColumn("age", engine.TypeInt64, 10000)
	emailCol := engine.NewColumn("email", engine.TypeString, 10000)
	statusCol := engine.NewColumn("status", engine.TypeString, 10000)
	salaryCol := engine.NewColumn("salary", engine.TypeInt64, 10000)
	departmentCol := engine.NewColumn("department", engine.TypeString, 10000)

	table.AddColumn(idCol)
	table.AddColumn(nameCol)
	table.AddColumn(ageCol)
	table.AddColumn(emailCol)
	table.AddColumn(statusCol)
	table.AddColumn(salaryCol)
	table.AddColumn(departmentCol)

	// Store table in database
	ts.database.Tables.Store("users", table)

	return table
}

// insertTestData populates the table with sample data
func (ts *TestSuite) insertTestData(t *testing.T, table *engine.Table, count int) {
	departments := []string{"Engineering", "Sales", "Marketing", "HR", "Finance"}
	statuses := []string{"active", "inactive", "pending", "terminated"}

	for i := 1; i <= count; i++ {
		data := map[string]interface{}{
			"id":         int64(i),
			"name":       fmt.Sprintf("User%d", i),
			"age":        int64(20 + (i%50)),           // Ages 20-69
			"email":      fmt.Sprintf("user%d@test.com", i),
			"status":     statuses[i%len(statuses)],
			"salary":     int64(30000 + (i%100)*1000),  // Salaries 30k-129k
			"department": departments[i%len(departments)],
		}

		err := table.InsertRow(data)
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}
}

// createTestIndexes creates various types of indexes for testing
func (ts *TestSuite) createTestIndexes(t *testing.T, table *engine.Table) {
	// Create LSM-Tree index on ID (primary key, write-heavy)
	err := ts.indexManager.CreateIndex("idx_id_lsm", "users", "id", engine.LSMTreeIndex, true)
	if err != nil {
		t.Logf("Warning: Could not create LSM index: %v", err)
	} else {
		err = ts.indexManager.BuildIndex(table, "idx_id_lsm")
		if err != nil {
			t.Logf("Warning: Could not build LSM index: %v", err)
		}
	}

	// Create Bitmap index on status (low cardinality)
	err = ts.indexManager.CreateIndex("idx_status_bitmap", "users", "status", engine.RoaringBitmapIndex, false)
	if err != nil {
		t.Logf("Warning: Could not create bitmap index: %v", err)
	} else {
		err = ts.indexManager.BuildIndex(table, "idx_status_bitmap")
		if err != nil {
			t.Logf("Warning: Could not build bitmap index: %v", err)
		}
	}

	// Create Bitmap index on department (low cardinality)
	err = ts.indexManager.CreateIndex("idx_dept_bitmap", "users", "department", engine.RoaringBitmapIndex, false)
	if err != nil {
		t.Logf("Warning: Could not create department bitmap index: %v", err)
	} else {
		err = ts.indexManager.BuildIndex(table, "idx_dept_bitmap")
		if err != nil {
			t.Logf("Warning: Could not build department bitmap index: %v", err)
		}
	}

	// Create B-Tree index on age (range queries)
	err = ts.indexManager.CreateIndex("idx_age_btree", "users", "age", engine.BTreeIndex, false)
	if err != nil {
		t.Logf("Warning: Could not create B-Tree index: %v", err)
	} else {
		err = ts.indexManager.BuildIndex(table, "idx_age_btree")
		if err != nil {
			t.Logf("Warning: Could not build B-Tree index: %v", err)
		}
	}

	// Create Hash index on email (equality lookups)
	err = ts.indexManager.CreateIndex("idx_email_hash", "users", "email", engine.HashIndex, true)
	if err != nil {
		t.Logf("Warning: Could not create Hash index: %v", err)
	} else {
		err = ts.indexManager.BuildIndex(table, "idx_email_hash")
		if err != nil {
			t.Logf("Warning: Could not build Hash index: %v", err)
		}
	}
}

// TestBasicSQLQueries tests fundamental SQL operations
func TestBasicSQLQueries(t *testing.T) {
	ts := SetupTestSuite(t)
	table := ts.createTestTable(t)
	ts.insertTestData(t, table, 1000)
	ts.createTestIndexes(t, table)

	testCases := []struct {
		name        string
		sql         string
		expectError bool
		description string
	}{
		{
			name:        "Simple SELECT *",
			sql:         "SELECT * FROM users",
			expectError: false,
			description: "Basic select all columns",
		},
		{
			name:        "SELECT specific columns",
			sql:         "SELECT name, age FROM users",
			expectError: false,
			description: "Select specific columns",
		},
		{
			name:        "SELECT with WHERE clause",
			sql:         "SELECT * FROM users WHERE age > 30",
			expectError: false,
			description: "Filter with comparison operator",
		},
		{
			name:        "SELECT with multiple conditions",
			sql:         "SELECT name, salary FROM users WHERE age > 25 AND status = 'active'",
			expectError: false,
			description: "Multiple filter conditions",
		},
		{
			name:        "SELECT with LIMIT",
			sql:         "SELECT * FROM users LIMIT 10",
			expectError: false,
			description: "Limit result set",
		},
		{
			name:        "COUNT aggregation",
			sql:         "SELECT COUNT(*) FROM users",
			expectError: false,
			description: "Count all rows",
		},
		{
			name:        "AVG aggregation",
			sql:         "SELECT AVG(salary) FROM users",
			expectError: false,
			description: "Average salary",
		},
		{
			name:        "MAX/MIN aggregation",
			sql:         "SELECT MAX(age), MIN(age) FROM users",
			expectError: false,
			description: "Maximum and minimum age",
		},
		{
			name:        "GROUP BY",
			sql:         "SELECT department, COUNT(*) FROM users GROUP BY department",
			expectError: false,
			description: "Group by department",
		},
		{
			name:        "Complex aggregation",
			sql:         "SELECT status, AVG(salary), COUNT(*) FROM users WHERE age > 25 GROUP BY status",
			expectError: false,
			description: "Complex aggregation with filtering",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Testing: %s - %s", tc.sql, tc.description)

			plan, err := ts.parser.Parse(tc.sql)
			if tc.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Failed to parse SQL: %v", err)
			}

			// Execute the query
			start := time.Now()
			result, err := ts.queryEngine.ExecuteQuery(plan, table)
			duration := time.Since(start)

			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}

			t.Logf("✅ Query executed successfully in %v", duration)
			t.Logf("   Columns: %v", result.Columns)
			t.Logf("   Rows returned: %d", len(result.Rows))

			// Log first few rows for verification
			if len(result.Rows) > 0 {
				t.Logf("   Sample results:")
				maxRows := 3
				if len(result.Rows) < maxRows {
					maxRows = len(result.Rows)
				}
				for i := 0; i < maxRows; i++ {
					t.Logf("     %v", result.Rows[i])
				}
			}
		})
	}
}

// TestInsertionPerformance benchmarks insertion performance with different index types
func TestInsertionPerformance(t *testing.T) {
	ts := SetupTestSuite(t)

	scenarios := []struct {
		name        string
		indexTypes  []engine.IndexType
		description string
	}{
		{
			name:        "No_Indexes",
			indexTypes:  []engine.IndexType{},
			description: "Baseline - no indexes",
		},
		{
			name:        "BTree_Only",
			indexTypes:  []engine.IndexType{engine.BTreeIndex},
			description: "Traditional B-Tree index",
		},
		{
			name:        "Hash_Only",
			indexTypes:  []engine.IndexType{engine.HashIndex},
			description: "Hash index for equality",
		},
		{
			name:        "LSM_Tree",
			indexTypes:  []engine.IndexType{engine.LSMTreeIndex},
			description: "LSM-Tree for write optimization",
		},
		{
			name:        "Bitmap_Only",
			indexTypes:  []engine.IndexType{engine.RoaringBitmapIndex},
			description: "Bitmap index for categorical",
		},
		{
			name:        "All_Indexes",
			indexTypes:  []engine.IndexType{engine.BTreeIndex, engine.HashIndex, engine.LSMTreeIndex, engine.RoaringBitmapIndex},
			description: "All index types combined",
		},
	}

	insertCounts := []int{100, 1000, 5000}

	t.Logf("\n=== INSERTION PERFORMANCE BENCHMARK ===")
	t.Logf("%-15s %-8s %-12s %-15s %-20s", "Scenario", "Records", "Duration", "Records/sec", "Description")
	t.Logf(strings.Repeat("-", 80))

	for _, scenario := range scenarios {
		for _, count := range insertCounts {
			t.Run(fmt.Sprintf("%s_%d", scenario.name, count), func(t *testing.T) {
				// Create fresh table for each test
				table := ts.createTestTable(t)

				// Create indexes based on scenario
				for i, indexType := range scenario.indexTypes {
					indexName := fmt.Sprintf("idx_%s_%d", scenario.name, i)
					column := "id"
					if indexType == engine.RoaringBitmapIndex {
						column = "status"
					}

					err := ts.indexManager.CreateIndex(indexName, "users", column, indexType, false)
					if err != nil {
						t.Logf("Warning: Could not create index: %v", err)
					}
				}

				// Benchmark insertion
				start := time.Now()
				ts.insertTestData(t, table, count)
				duration := time.Since(start)

				recordsPerSec := float64(count) / duration.Seconds()

				t.Logf("%-15s %-8d %-12v %-15.0f %-20s",
					scenario.name, count, duration, recordsPerSec, scenario.description)
			})
		}
	}
}

// TestReadPerformance benchmarks read query performance with different index strategies
func TestReadPerformance(t *testing.T) {
	ts := SetupTestSuite(t)
	table := ts.createTestTable(t)
	ts.insertTestData(t, table, 10000) // Larger dataset for read tests
	ts.createTestIndexes(t, table)

	queries := []struct {
		name        string
		sql         string
		description string
		expectRows  int // Approximate expected rows
	}{
		{
			name:        "Point_Lookup",
			sql:         "SELECT * FROM users WHERE id = 5000",
			description: "Single row lookup by primary key",
			expectRows:  1,
		},
		{
			name:        "Status_Filter",
			sql:         "SELECT * FROM users WHERE status = 'active'",
			description: "Filter by categorical column (bitmap index)",
			expectRows:  2500, // ~25% of records
		},
		{
			name:        "Age_Range",
			sql:         "SELECT * FROM users WHERE age BETWEEN 30 AND 40",
			description: "Range query on numeric column",
			expectRows:  1000, // ~10% of records
		},
		{
			name:        "Department_Filter",
			sql:         "SELECT name, salary FROM users WHERE department = 'Engineering'",
			description: "Department filter (bitmap index)",
			expectRows:  2000, // ~20% of records
		},
		{
			name:        "Complex_Filter",
			sql:         "SELECT * FROM users WHERE age > 35 AND status = 'active' AND salary > 50000",
			description: "Multiple conditions requiring index intersection",
			expectRows:  500, // ~5% of records
		},
		{
			name:        "Count_Aggregation",
			sql:         "SELECT COUNT(*) FROM users WHERE status = 'active'",
			description: "Count with filter using bitmap index",
			expectRows:  1,
		},
		{
			name:        "Group_By_Performance",
			sql:         "SELECT department, COUNT(*), AVG(salary) FROM users GROUP BY department",
			description: "Grouping by low-cardinality column",
			expectRows:  5, // 5 departments
		},
	}

	t.Logf("\n=== READ PERFORMANCE BENCHMARK ===")
	t.Logf("%-20s %-12s %-12s %-10s %-30s", "Query Type", "Duration", "Rows", "Rows/sec", "Description")
	t.Logf(strings.Repeat("-", 90))

	for _, query := range queries {
		t.Run(query.name, func(t *testing.T) {
			// Parse the query
			plan, err := ts.parser.Parse(query.sql)
			if err != nil {
				t.Fatalf("Failed to parse SQL: %v", err)
			}

			// Execute multiple times for more accurate timing
			iterations := 5
			totalDuration := time.Duration(0)
			var lastResult *engine.QueryResult

			for i := 0; i < iterations; i++ {
				start := time.Now()
				result, err := ts.queryEngine.ExecuteQuery(plan, table)
				duration := time.Since(start)
				totalDuration += duration

				if err != nil {
					t.Fatalf("Failed to execute query: %v", err)
				}
				lastResult = result
			}

			avgDuration := totalDuration / time.Duration(iterations)
			rowsPerSec := float64(len(lastResult.Rows)) / avgDuration.Seconds()

			t.Logf("%-20s %-12v %-12d %-10.0f %-30s",
				query.name, avgDuration, len(lastResult.Rows), rowsPerSec, query.description)

			// Verify approximate result count
			actualRows := len(lastResult.Rows)
			if query.expectRows > 0 {
				tolerance := 0.5 // 50% tolerance for approximation
				if float64(actualRows) < float64(query.expectRows)*(1-tolerance) ||
					float64(actualRows) > float64(query.expectRows)*(1+tolerance) {
					t.Logf("Warning: Expected ~%d rows, got %d rows", query.expectRows, actualRows)
				}
			}
		})
	}
}

// TestAggregationPerformance specifically tests aggregation query performance
func TestAggregationPerformance(t *testing.T) {
	ts := SetupTestSuite(t)
	table := ts.createTestTable(t)
	ts.insertTestData(t, table, 10000)
	ts.createTestIndexes(t, table)

	aggregationQueries := []struct {
		name        string
		sql         string
		description string
	}{
		{
			name:        "Simple_Count",
			sql:         "SELECT COUNT(*) FROM users",
			description: "Count all records",
		},
		{
			name:        "Conditional_Count",
			sql:         "SELECT COUNT(*) FROM users WHERE status = 'active'",
			description: "Count with bitmap index filter",
		},
		{
			name:        "Average_Salary",
			sql:         "SELECT AVG(salary) FROM users",
			description: "Average calculation",
		},
		{
			name:        "Min_Max_Age",
			sql:         "SELECT MIN(age), MAX(age) FROM users",
			description: "Min/Max with potential index optimization",
		},
		{
			name:        "Group_Count",
			sql:         "SELECT status, COUNT(*) FROM users GROUP BY status",
			description: "Group by categorical column",
		},
		{
			name:        "Group_Avg",
			sql:         "SELECT department, AVG(salary), COUNT(*) FROM users GROUP BY department",
			description: "Group by with multiple aggregations",
		},
		{
			name:        "Complex_Aggregation",
			sql:         "SELECT status, department, COUNT(*), AVG(salary), MAX(age) FROM users WHERE salary > 40000 GROUP BY status, department",
			description: "Complex multi-column grouping with filter",
		},
	}

	t.Logf("\n=== AGGREGATION PERFORMANCE BENCHMARK ===")
	t.Logf("%-25s %-12s %-12s %-30s", "Query Type", "Duration", "Groups", "Description")
	t.Logf(strings.Repeat("-", 85))

	for _, query := range aggregationQueries {
		t.Run(query.name, func(t *testing.T) {
			// Parse the query
			plan, err := ts.parser.Parse(query.sql)
			if err != nil {
				t.Fatalf("Failed to parse SQL: %v", err)
			}

			// Execute multiple times for accurate timing
			iterations := 3
			totalDuration := time.Duration(0)
			var lastResult *engine.QueryResult

			for i := 0; i < iterations; i++ {
				start := time.Now()
				result, err := ts.queryEngine.ExecuteQuery(plan, table)
				duration := time.Since(start)
				totalDuration += duration

				if err != nil {
					t.Fatalf("Failed to execute query: %v", err)
				}
				lastResult = result
			}

			avgDuration := totalDuration / time.Duration(iterations)

			t.Logf("%-25s %-12v %-12d %-30s",
				query.name, avgDuration, len(lastResult.Rows), query.description)

			// Log sample results for verification
			if len(lastResult.Rows) > 0 && len(lastResult.Rows) <= 10 {
				t.Logf("  Sample results: %v", lastResult.Rows)
			}
		})
	}
}

// TestIndexEffectiveness compares query performance with and without indexes
func TestIndexEffectiveness(t *testing.T) {
	ts := SetupTestSuite(t)

	testQueries := []struct {
		name string
		sql  string
	}{
		{"Point_Lookup", "SELECT * FROM users WHERE id = 5000"},
		{"Status_Filter", "SELECT * FROM users WHERE status = 'active'"},
		{"Age_Range", "SELECT * FROM users WHERE age > 35"},
	}

	for _, query := range testQueries {
		t.Run(query.name, func(t *testing.T) {
			t.Logf("\n--- Testing %s ---", query.name)

			// Test without indexes
			table1 := ts.createTestTable(t)
			ts.insertTestData(t, table1, 5000)

			plan, err := ts.parser.Parse(query.sql)
			if err != nil {
				t.Fatalf("Failed to parse SQL: %v", err)
			}

			start := time.Now()
			result1, err := ts.queryEngine.ExecuteQuery(plan, table1)
			durationWithoutIndex := time.Since(start)
			if err != nil {
				t.Fatalf("Failed to execute query without index: %v", err)
			}

			// Test with indexes
			table2 := ts.createTestTable(t)
			ts.insertTestData(t, table2, 5000)
			ts.createTestIndexes(t, table2)

			start = time.Now()
			result2, err := ts.queryEngine.ExecuteQuery(plan, table2)
			durationWithIndex := time.Since(start)
			if err != nil {
				t.Fatalf("Failed to execute query with index: %v", err)
			}

			// Calculate speedup
			speedup := float64(durationWithoutIndex) / float64(durationWithIndex)

			t.Logf("Without indexes: %v (%d rows)", durationWithoutIndex, len(result1.Rows))
			t.Logf("With indexes:    %v (%d rows)", durationWithIndex, len(result2.Rows))
			t.Logf("Speedup:         %.2fx", speedup)

			// Verify results are the same
			if len(result1.Rows) != len(result2.Rows) {
				t.Errorf("Result count mismatch: %d vs %d", len(result1.Rows), len(result2.Rows))
			}
		})
	}
}

// TestIndexStatistics verifies index statistics and monitoring
func TestIndexStatistics(t *testing.T) {
	ts := SetupTestSuite(t)
	table := ts.createTestTable(t)
	ts.insertTestData(t, table, 1000)
	ts.createTestIndexes(t, table)

	t.Logf("\n=== INDEX STATISTICS ===")

	stats := ts.indexManager.GetIndexStats()

	if traditionalCount, ok := stats["traditional_indexes"].(int); ok {
		t.Logf("Traditional indexes: %d", traditionalCount)
	}

	if lsmStats, ok := stats["lsm_indexes"].(map[string]interface{}); ok {
		t.Logf("LSM-Tree indexes: %d", len(lsmStats))
		for name, info := range lsmStats {
			if infoMap, ok := info.(map[string]interface{}); ok {
				t.Logf("  - %s: %v rows", name, infoMap["row_count"])
			}
		}
	}

	if bitmapStats, ok := stats["bitmap_indexes"].(map[string]interface{}); ok {
		t.Logf("Bitmap indexes: %d", len(bitmapStats))
		for name, stats := range bitmapStats {
			if bitmapInfo, ok := stats.(storage.BitmapIndexStats); ok {
				t.Logf("  - %s: %d values, %d rows, %.2f compression, %d bytes",
					name, bitmapInfo.Cardinality, bitmapInfo.RowCount,
					bitmapInfo.CompressionRatio, bitmapInfo.MemoryUsage)
			}
		}
	}

	// Test query execution stats
	queryStats := ts.queryEngine.GetStats()
	t.Logf("\nQuery execution statistics:")
	t.Logf("  Total queries: %d", queryStats.TotalQueries)
	t.Logf("  Index scans: %d", queryStats.IndexScans)
	t.Logf("  Sequential scans: %d", queryStats.SequentialScans)
	t.Logf("  LSM index usage: %d", queryStats.LSMIndexUsage)
	t.Logf("  Bitmap index usage: %d", queryStats.BitmapIndexUsage)
	t.Logf("  Average response time: %v", queryStats.AvgResponseTime)
}

// BenchmarkFullWorkload runs a comprehensive workload benchmark
func BenchmarkFullWorkload(b *testing.B) {
	ts := &TestSuite{
		database:     engine.NewDatabase("bench_db"),
		parser:       query.NewSQLParser(),
		indexManager: storage.NewIndexManager(),
	}
	ts.queryEngine = query.NewIndexedQueryEngine(ts.indexManager)

	table := ts.createTestTable(&testing.T{})
	ts.insertTestData(&testing.T{}, table, 10000)
	ts.createTestIndexes(&testing.T{}, table)

	queries := []string{
		"SELECT * FROM users WHERE id = 5000",
		"SELECT * FROM users WHERE status = 'active'",
		"SELECT COUNT(*) FROM users WHERE age > 30",
		"SELECT department, AVG(salary) FROM users GROUP BY department",
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, sql := range queries {
			plan, err := ts.parser.Parse(sql)
			if err != nil {
				b.Fatalf("Parse error: %v", err)
			}

			_, err = ts.queryEngine.ExecuteQuery(plan, table)
			if err != nil {
				b.Fatalf("Execution error: %v", err)
			}
		}
	}
}