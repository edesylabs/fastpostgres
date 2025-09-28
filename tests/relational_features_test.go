package tests

import (
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

const (
	testHost     = "localhost"
	testPort     = "5433"
	testUser     = "postgres"
	testPassword = "postgres"
	testDB       = "fastpostgres"
)

func getTestConnection(t *testing.T) *sql.DB {
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		testHost, testPort, testUser, testPassword, testDB)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	err = db.Ping()
	if err != nil {
		t.Fatalf("Failed to ping database: %v", err)
	}

	return db
}

func TestCreateTable(t *testing.T) {
	db := getTestConnection(t)
	defer db.Close()

	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "Simple table with int and string",
			query: "CREATE TABLE test_users (id INT, name TEXT, age INT)",
		},
		{
			name:  "Table with multiple columns",
			query: "CREATE TABLE test_products (id INT, name TEXT, price INT, quantity INT)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := db.Exec(tt.query)
			if err != nil {
				t.Errorf("CREATE TABLE failed: %v", err)
			}
		})
	}
}

func TestInsertOperations(t *testing.T) {
	db := getTestConnection(t)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE test_insert (id INT, name TEXT, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	t.Run("Single INSERT", func(t *testing.T) {
		_, err := db.Exec("INSERT INTO test_insert (id, name, value) VALUES (1, 'Alice', 100)")
		if err != nil {
			t.Errorf("Single INSERT failed: %v", err)
		}
	})

	t.Run("Multiple INSERTs", func(t *testing.T) {
		for i := 2; i <= 10; i++ {
			_, err := db.Exec(fmt.Sprintf("INSERT INTO test_insert (id, name, value) VALUES (%d, 'User%d', %d)", i, i, i*10))
			if err != nil {
				t.Errorf("Multiple INSERT failed at row %d: %v", i, err)
				break
			}
		}
	})

	t.Run("Verify inserted data", func(t *testing.T) {
		var count int
		err := db.QueryRow("SELECT COUNT(*) FROM test_insert").Scan(&count)
		if err != nil {
			t.Errorf("COUNT query failed: %v", err)
		}
		if count != 10 {
			t.Errorf("Expected 10 rows, got %d", count)
		}
	})
}

func TestSelectWithWhere(t *testing.T) {
	db := getTestConnection(t)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE test_select (id INT, name TEXT, age INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	testData := []struct {
		id   int
		name string
		age  int
	}{
		{1, "Alice", 25},
		{2, "Bob", 30},
		{3, "Charlie", 35},
		{4, "David", 25},
		{5, "Eve", 40},
	}

	for _, data := range testData {
		_, err := db.Exec("INSERT INTO test_select (id, name, age) VALUES ($1, $2, $3)", data.id, data.name, data.age)
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	tests := []struct {
		name          string
		query         string
		expectedCount int
	}{
		{
			name:          "SELECT with equality",
			query:         "SELECT * FROM test_select WHERE age = 25",
			expectedCount: 2,
		},
		{
			name:          "SELECT with greater than",
			query:         "SELECT * FROM test_select WHERE age > 30",
			expectedCount: 2,
		},
		{
			name:          "SELECT with less than",
			query:         "SELECT * FROM test_select WHERE age < 30",
			expectedCount: 2,
		},
		{
			name:          "SELECT all",
			query:         "SELECT * FROM test_select",
			expectedCount: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Errorf("Query failed: %v", err)
				return
			}
			defer rows.Close()

			count := 0
			for rows.Next() {
				count++
				var id, age int
				var name string
				err := rows.Scan(&id, &name, &age)
				if err != nil {
					t.Errorf("Scan failed: %v", err)
				}
			}

			if count != tt.expectedCount {
				t.Errorf("Expected %d rows, got %d", tt.expectedCount, count)
			}
		})
	}
}

func TestAggregateFunctions(t *testing.T) {
	db := getTestConnection(t)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE test_agg (id INT, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	values := []int{10, 20, 30, 40, 50}
	for i, v := range values {
		_, err := db.Exec("INSERT INTO test_agg (id, value) VALUES ($1, $2)", i+1, v)
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	t.Run("COUNT", func(t *testing.T) {
		var count int64
		err := db.QueryRow("SELECT COUNT(*) FROM test_agg").Scan(&count)
		if err != nil {
			t.Errorf("COUNT query failed: %v", err)
		}
		if count != 5 {
			t.Errorf("Expected COUNT = 5, got %d", count)
		}
	})

	t.Run("SUM", func(t *testing.T) {
		var sum int64
		err := db.QueryRow("SELECT SUM(value) FROM test_agg").Scan(&sum)
		if err != nil {
			t.Errorf("SUM query failed: %v", err)
		}
		if sum != 150 {
			t.Errorf("Expected SUM = 150, got %d", sum)
		}
	})

	t.Run("AVG", func(t *testing.T) {
		var avg float64
		err := db.QueryRow("SELECT AVG(value) FROM test_agg").Scan(&avg)
		if err != nil {
			t.Errorf("AVG query failed: %v", err)
		}
		if avg != 30.0 {
			t.Errorf("Expected AVG = 30.0, got %f", avg)
		}
	})

	t.Run("MIN", func(t *testing.T) {
		var min int64
		err := db.QueryRow("SELECT MIN(value) FROM test_agg").Scan(&min)
		if err != nil {
			t.Errorf("MIN query failed: %v", err)
		}
		if min != 10 {
			t.Errorf("Expected MIN = 10, got %d", min)
		}
	})

	t.Run("MAX", func(t *testing.T) {
		var max int64
		err := db.QueryRow("SELECT MAX(value) FROM test_agg").Scan(&max)
		if err != nil {
			t.Errorf("MAX query failed: %v", err)
		}
		if max != 50 {
			t.Errorf("Expected MAX = 50, got %d", max)
		}
	})
}

func TestGroupBy(t *testing.T) {
	db := getTestConnection(t)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE test_groupby (category TEXT, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	testData := []struct {
		category string
		value    int
	}{
		{"A", 10},
		{"A", 20},
		{"B", 30},
		{"B", 40},
		{"C", 50},
	}

	for _, data := range testData {
		_, err := db.Exec("INSERT INTO test_groupby (category, value) VALUES ($1, $2)", data.category, data.value)
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	t.Run("GROUP BY with COUNT", func(t *testing.T) {
		rows, err := db.Query("SELECT category, COUNT(*) FROM test_groupby GROUP BY category")
		if err != nil {
			t.Errorf("GROUP BY COUNT query failed: %v", err)
			return
		}
		defer rows.Close()

		results := make(map[string]int64)
		for rows.Next() {
			var category string
			var count int64
			err := rows.Scan(&category, &count)
			if err != nil {
				t.Errorf("Scan failed: %v", err)
			}
			results[category] = count
		}

		expected := map[string]int64{"A": 2, "B": 2, "C": 1}
		for cat, expectedCount := range expected {
			if results[cat] != expectedCount {
				t.Errorf("Category %s: expected count %d, got %d", cat, expectedCount, results[cat])
			}
		}
	})

	t.Run("GROUP BY with SUM", func(t *testing.T) {
		rows, err := db.Query("SELECT category, SUM(value) FROM test_groupby GROUP BY category")
		if err != nil {
			t.Errorf("GROUP BY SUM query failed: %v", err)
			return
		}
		defer rows.Close()

		results := make(map[string]int64)
		for rows.Next() {
			var category string
			var sum int64
			err := rows.Scan(&category, &sum)
			if err != nil {
				t.Errorf("Scan failed: %v", err)
			}
			results[category] = sum
		}

		expected := map[string]int64{"A": 30, "B": 70, "C": 50}
		for cat, expectedSum := range expected {
			if results[cat] != expectedSum {
				t.Errorf("Category %s: expected sum %d, got %d", cat, expectedSum, results[cat])
			}
		}
	})
}

func TestOrderByAndLimit(t *testing.T) {
	db := getTestConnection(t)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE test_order (id INT, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	values := []int{50, 10, 30, 20, 40}
	for i, v := range values {
		_, err := db.Exec("INSERT INTO test_order (id, value) VALUES ($1, $2)", i+1, v)
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	t.Run("ORDER BY ASC", func(t *testing.T) {
		rows, err := db.Query("SELECT value FROM test_order ORDER BY value")
		if err != nil {
			t.Errorf("ORDER BY query failed: %v", err)
			return
		}
		defer rows.Close()

		var values []int
		for rows.Next() {
			var value int
			rows.Scan(&value)
			values = append(values, value)
		}

		expected := []int{10, 20, 30, 40, 50}
		for i, v := range expected {
			if i >= len(values) || values[i] != v {
				t.Errorf("Expected sorted value at position %d to be %d, got %d", i, v, values[i])
			}
		}
	})

	t.Run("LIMIT", func(t *testing.T) {
		rows, err := db.Query("SELECT value FROM test_order LIMIT 3")
		if err != nil {
			t.Errorf("LIMIT query failed: %v", err)
			return
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			count++
			var value int
			rows.Scan(&value)
		}

		if count != 3 {
			t.Errorf("Expected 3 rows with LIMIT, got %d", count)
		}
	})
}

func TestConcurrentConnections(t *testing.T) {
	t.Run("Multiple concurrent queries", func(t *testing.T) {
		numConnections := 10
		var wg sync.WaitGroup

		for i := 0; i < numConnections; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				db := getTestConnection(t)
				defer db.Close()

				tableName := fmt.Sprintf("test_concurrent_%d", id)
				_, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (id INT, value INT)", tableName))
				if err != nil {
					t.Errorf("Connection %d: CREATE TABLE failed: %v", id, err)
					return
				}

				_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (id, value) VALUES (1, %d)", tableName, id*100))
				if err != nil {
					t.Errorf("Connection %d: INSERT failed: %v", id, err)
					return
				}

				var value int
				err = db.QueryRow(fmt.Sprintf("SELECT value FROM %s WHERE id = 1", tableName)).Scan(&value)
				if err != nil {
					t.Errorf("Connection %d: SELECT failed: %v", id, err)
					return
				}

				if value != id*100 {
					t.Errorf("Connection %d: Expected value %d, got %d", id, id*100, value)
				}
			}(i)
		}

		wg.Wait()
	})
}

func TestBasicPerformance(t *testing.T) {
	db := getTestConnection(t)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE test_perf (id INT, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	t.Run("Bulk INSERT performance", func(t *testing.T) {
		numRows := 1000
		start := time.Now()

		for i := 1; i <= numRows; i++ {
			_, err := db.Exec("INSERT INTO test_perf (id, value) VALUES ($1, $2)", i, i*10)
			if err != nil {
				t.Errorf("INSERT failed at row %d: %v", i, err)
				break
			}
		}

		duration := time.Since(start)
		rate := float64(numRows) / duration.Seconds()
		t.Logf("Inserted %d rows in %v (%.2f rows/sec)", numRows, duration, rate)
	})

	t.Run("SELECT performance", func(t *testing.T) {
		start := time.Now()

		rows, err := db.Query("SELECT * FROM test_perf WHERE value > 5000")
		if err != nil {
			t.Errorf("SELECT query failed: %v", err)
			return
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			count++
			var id, value int
			rows.Scan(&id, &value)
		}

		duration := time.Since(start)
		t.Logf("Selected %d rows in %v", count, duration)
	})

	t.Run("Aggregation performance", func(t *testing.T) {
		start := time.Now()

		var sum int64
		err := db.QueryRow("SELECT SUM(value) FROM test_perf").Scan(&sum)
		if err != nil {
			t.Errorf("SUM query failed: %v", err)
			return
		}

		duration := time.Since(start)
		t.Logf("Computed SUM over 1000 rows in %v (result: %d)", duration, sum)
	})
}