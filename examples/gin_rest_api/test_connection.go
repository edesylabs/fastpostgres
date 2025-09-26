package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

func main() {
	// Load environment variables from .env file
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using default configuration")
	}

	// Get DATABASE_URL from environment or use default
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		connStr = "postgres://fastpostgres@localhost:5435/fastpostgres?sslmode=disable"
	}

	fmt.Printf("Testing connection with: %s\n", connStr)

	// Test with different connection approaches
	testConnections := []string{
		connStr,
		"host=localhost port=5435 user=fastpostgres dbname=fastpostgres sslmode=disable",
		"postgresql://fastpostgres@localhost:5435/fastpostgres?sslmode=disable",
	}

	for i, testConn := range testConnections {
		fmt.Printf("\n--- Test %d: %s ---\n", i+1, testConn)

		db, err := sql.Open("postgres", testConn)
		if err != nil {
			fmt.Printf("sql.Open failed: %v\n", err)
			continue
		}
		defer db.Close()

		// Set connection parameters that might help
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		db.SetConnMaxLifetime(time.Minute * 5)

		fmt.Println("sql.Open succeeded, testing ping...")

		if err = db.Ping(); err != nil {
			fmt.Printf("db.Ping failed: %v\n", err)
		} else {
			fmt.Println("✓ Connection successful!")

			// Try a simple query
			var count int
			err := db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
			if err != nil {
				fmt.Printf("Query failed: %v\n", err)
			} else {
				fmt.Printf("✓ Query successful! User count: %d\n", count)
			}
			return // Success, exit
		}
	}

	fmt.Println("All connection attempts failed")
}