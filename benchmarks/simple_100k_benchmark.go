package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"time"

	_ "github.com/lib/pq"
)

const (
	TOTAL_ROWS = 100000
	BATCH_SIZE = 1000
)

func main() {
	fmt.Println("🚀 100K Row Insertion Benchmark: FastPostgres vs PostgreSQL")
	fmt.Println("============================================================")
	fmt.Println()

	// Test PostgreSQL
	fmt.Println("📊 Testing PostgreSQL...")
	pgTime, pgErr := testDatabase("postgres://postgres@localhost:5432/testdb?sslmode=disable", "PostgreSQL")
	if pgErr != nil {
		log.Printf("PostgreSQL test failed: %v", pgErr)
	}

	// Test FastPostgres
	fmt.Println("📊 Testing FastPostgres...")
	fpTime, fpErr := testDatabase("postgres://postgres@localhost:5433/fastpostgres?sslmode=disable", "FastPostgres")
	if fpErr != nil {
		log.Printf("FastPostgres test failed: %v", fpErr)
	}

	// Compare results
	fmt.Println("🎯 Performance Comparison:")
	fmt.Println("==========================")

	if pgErr == nil && fpErr == nil {
		pgThroughput := float64(TOTAL_ROWS) / pgTime.Seconds()
		fpThroughput := float64(TOTAL_ROWS) / fpTime.Seconds()

		fmt.Printf("PostgreSQL:   %.2fs (%.0f rows/sec)\n", pgTime.Seconds(), pgThroughput)
		fmt.Printf("FastPostgres: %.2fs (%.0f rows/sec)\n", fpTime.Seconds(), fpThroughput)
		fmt.Println()

		if fpThroughput > pgThroughput {
			speedup := fpThroughput / pgThroughput
			improvement := ((fpThroughput - pgThroughput) / pgThroughput) * 100
			fmt.Printf("✅ FastPostgres is %.1f%% faster (%.2fx speedup)\n", improvement, speedup)
		} else {
			speedup := pgThroughput / fpThroughput
			improvement := ((pgThroughput - fpThroughput) / fpThroughput) * 100
			fmt.Printf("📊 PostgreSQL is %.1f%% faster (%.2fx speedup)\n", improvement, speedup)
		}
	}

	fmt.Println()
	fmt.Println("🎉 Benchmark completed!")
}

func testDatabase(dsn, name string) (time.Duration, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return 0, fmt.Errorf("failed to connect: %v", err)
	}
	defer db.Close()

	// Test connection
	if err := db.Ping(); err != nil {
		return 0, fmt.Errorf("failed to ping: %v", err)
	}

	// Setup table
	createTableSQL := `
		DROP TABLE IF EXISTS insertion_benchmark;
		CREATE TABLE insertion_benchmark (
			id INTEGER PRIMARY KEY,
			user_id INTEGER,
			username VARCHAR(50),
			email VARCHAR(100),
			age INTEGER,
			balance INTEGER,
			status VARCHAR(20),
			created_at INTEGER
		);`

	if _, err := db.Exec(createTableSQL); err != nil {
		return 0, fmt.Errorf("failed to create table: %v", err)
	}

	fmt.Printf("  📝 Inserting %d rows in batches of %d...\n", TOTAL_ROWS, BATCH_SIZE)

	start := time.Now()
	inserted := 0

	for batchStart := 1; batchStart <= TOTAL_ROWS; batchStart += BATCH_SIZE {
		batchEnd := batchStart + BATCH_SIZE - 1
		if batchEnd > TOTAL_ROWS {
			batchEnd = TOTAL_ROWS
		}

		if err := insertBatch(db, batchStart, batchEnd); err != nil {
			return 0, fmt.Errorf("failed to insert batch: %v", err)
		}

		inserted = batchEnd
		if inserted%10000 == 0 || inserted == TOTAL_ROWS {
			percent := (inserted * 100) / TOTAL_ROWS
			fmt.Printf("    Progress: %d%% (%d/%d rows)\n", percent, inserted, TOTAL_ROWS)
		}
	}

	duration := time.Since(start)

	// Verify count
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM insertion_benchmark").Scan(&count); err != nil {
		return duration, fmt.Errorf("failed to count rows: %v", err)
	}

	throughput := float64(count) / duration.Seconds()
	fmt.Printf("  ✅ %s: %d rows in %.2fs (%.0f rows/sec)\n", name, count, duration.Seconds(), throughput)
	fmt.Println()

	return duration, nil
}

func insertBatch(db *sql.DB, start, end int) error {
	statuses := []string{"active", "inactive", "pending", "verified"}


	// For batch inserts, we need to construct the query differently
	batchQuery := "INSERT INTO insertion_benchmark (id, user_id, username, email, age, balance, status, created_at) VALUES "
	valuesPart := ""

	batchValues := []interface{}{}
	for i := start; i <= end; i++ {
		if i > start {
			valuesPart += ", "
		}
		valuesPart += fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			(i-start)*8+1, (i-start)*8+2, (i-start)*8+3, (i-start)*8+4,
			(i-start)*8+5, (i-start)*8+6, (i-start)*8+7, (i-start)*8+8)

		batchValues = append(batchValues,
			i,                                    // id
			i,                                    // user_id
			fmt.Sprintf("user_%d", i),            // username
			fmt.Sprintf("user%d@test.com", i),    // email
			18+rand.Intn(70),                     // age
			rand.Intn(100000),                    // balance
			statuses[rand.Intn(len(statuses))],   // status
			1640995200+int64(i),                  // created_at
		)
	}

	fullQuery := batchQuery + valuesPart
	_, err := db.Exec(fullQuery, batchValues...)
	return err
}