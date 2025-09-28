package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	// Clean up data directory
	os.RemoveAll("./data")

	fmt.Println("=== Testing WAL Crash Recovery ===")

	// Phase 1: Start server and insert data
	fmt.Println("\n1. Starting server and inserting data...")
	if err := testInitialData(); err != nil {
		log.Fatalf("Phase 1 failed: %v", err)
	}

	// Phase 2: Simulate crash (server will be stopped)
	fmt.Println("\n2. Simulating crash by stopping server...")
	time.Sleep(2 * time.Second)

	// Phase 3: Restart server and verify data recovery
	fmt.Println("\n3. Restarting server and verifying recovery...")
	if err := testRecovery(); err != nil {
		log.Fatalf("Phase 3 failed: %v", err)
	}

	fmt.Println("\n✅ WAL crash recovery test passed!")
}

func testInitialData() error {
	// Wait for server to start
	time.Sleep(3 * time.Second)

	// Connect to database
	db, err := sql.Open("postgres", "host=localhost port=5433 user=postgres dbname=fastpostgres sslmode=disable")
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer db.Close()

	// Test connection
	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping: %w", err)
	}

	// Insert additional test data
	_, err = db.Exec("INSERT INTO users (id, name, email, age) VALUES (100, 'Test User', 'test@example.com', 99)")
	if err != nil {
		return fmt.Errorf("failed to insert test data: %w", err)
	}

	// Verify data was inserted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM users WHERE id = 100").Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to query test data: %w", err)
	}

	if count != 1 {
		return fmt.Errorf("expected 1 row, got %d", count)
	}

	fmt.Printf("✅ Successfully inserted test data (user id=100)")
	return nil
}

func testRecovery() error {
	// Wait for server to start
	time.Sleep(3 * time.Second)

	// Connect to database
	db, err := sql.Open("postgres", "host=localhost port=5433 user=postgres dbname=fastpostgres sslmode=disable")
	if err != nil {
		return fmt.Errorf("failed to connect after restart: %w", err)
	}
	defer db.Close()

	// Test connection
	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping after restart: %w", err)
	}

	// Verify that our test data survived the crash
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM users WHERE id = 100").Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to query test data after restart: %w", err)
	}

	if count != 1 {
		return fmt.Errorf("expected 1 row after recovery, got %d", count)
	}

	// Verify name was recovered correctly
	var name string
	err = db.QueryRow("SELECT name FROM users WHERE id = 100").Scan(&name)
	if err != nil {
		return fmt.Errorf("failed to query name after restart: %w", err)
	}

	if name != "Test User" {
		return fmt.Errorf("expected 'Test User', got '%s'", name)
	}

	fmt.Printf("✅ Successfully recovered test data after crash (user: %s)", name)
	return nil
}