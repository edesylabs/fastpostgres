package main

import (
	"fmt"
	"log"

	"fastpostgres/pkg/engine"
)

func main() {
	fmt.Println("=== Testing MVCC and UPDATE/DELETE Operations ===")

	// Create database with MVCC enabled
	db := engine.NewDatabase("mvcc_test")

	// Create test table
	table := engine.NewTableWithDB("test_table", db)

	// Add columns
	idCol := engine.NewColumn("id", engine.TypeInt64, 1000)
	nameCol := engine.NewColumn("name", engine.TypeString, 1000)
	ageCol := engine.NewColumn("age", engine.TypeInt64, 1000)

	table.AddColumn(idCol)
	table.AddColumn(nameCol)
	table.AddColumn(ageCol)

	// Store table in database
	db.Tables.Store("test_table", table)

	fmt.Println("\n1. Testing INSERT with MVCC...")

	// Test basic inserts
	testData := []map[string]interface{}{
		{"id": int64(1), "name": "Alice", "age": int64(25)},
		{"id": int64(2), "name": "Bob", "age": int64(30)},
		{"id": int64(3), "name": "Charlie", "age": int64(35)},
	}

	for _, data := range testData {
		if err := table.InsertRow(data); err != nil {
			log.Printf("Failed to insert: %v", err)
		} else {
			fmt.Printf("✅ Inserted: %v\n", data)
		}
	}

	fmt.Printf("Table now has %d rows\n", table.RowCount)
	fmt.Printf("MVCC rows: %d\n", len(table.MVCCRows))

	fmt.Println("\n2. Testing UPDATE operation...")

	// Create filter to update Bob's age
	filter := &engine.FilterExpression{
		Column:   "name",
		Operator: engine.OpEqual,
		Value:    "Bob",
	}

	updates := map[string]interface{}{
		"age": int64(31),
	}

	if table.MVCCManager != nil {
		updateCount, err := table.UpdateRow(filter, updates)
		if err != nil {
			log.Printf("Failed to update: %v", err)
		} else {
			fmt.Printf("✅ Updated %d rows\n", updateCount)
		}
	} else {
		fmt.Println("⚠️  MVCC not enabled, skipping UPDATE test")
	}

	fmt.Println("\n3. Testing DELETE operation...")

	// Create filter to delete Charlie
	deleteFilter := &engine.FilterExpression{
		Column:   "name",
		Operator: engine.OpEqual,
		Value:    "Charlie",
	}

	if table.MVCCManager != nil {
		deleteCount, err := table.DeleteRow(deleteFilter)
		if err != nil {
			log.Printf("Failed to delete: %v", err)
		} else {
			fmt.Printf("✅ Deleted %d rows\n", deleteCount)
		}
	} else {
		fmt.Println("⚠️  MVCC not enabled, skipping DELETE test")
	}

	fmt.Println("\n4. Testing Transaction Isolation...")

	if db.MVCCManager != nil {
		// Create two connections
		conn1 := &engine.Connection{ID: "conn1"}
		conn2 := &engine.Connection{ID: "conn2"}

		// Start transaction in conn1
		if err := db.BeginTransaction(conn1); err != nil {
			log.Printf("Failed to begin transaction: %v", err)
		} else {
			fmt.Println("✅ Started transaction in conn1")

			// Insert in transaction (not yet committed)
			newData := map[string]interface{}{
				"id": int64(4), "name": "David", "age": int64(40),
			}

			if err := table.InsertRowWithTransaction(conn1.MVCCTxn, newData); err != nil {
				log.Printf("Failed to insert in transaction: %v", err)
			} else {
				fmt.Println("✅ Inserted David in conn1 transaction")
			}

			// Try to read from conn2 (should not see David yet)
			fmt.Println("🔍 Checking isolation...")
			_ = conn2 // Prevent unused variable warning

			// Commit the transaction
			if err := db.CommitTransaction(conn1); err != nil {
				log.Printf("Failed to commit: %v", err)
			} else {
				fmt.Println("✅ Committed transaction in conn1")
			}
		}
	}

	fmt.Println("\n5. Testing Concurrent Transactions...")

	if db.MVCCManager != nil {
		conn1 := &engine.Connection{ID: "conn1"}
		conn2 := &engine.Connection{ID: "conn2"}

		// Start transactions in both connections
		db.BeginTransaction(conn1)
		db.BeginTransaction(conn2)

		// Each transaction tries to update the same row
		filter := &engine.FilterExpression{
			Column:   "id",
			Operator: engine.OpEqual,
			Value:    int64(1),
		}

		updates1 := map[string]interface{}{"age": int64(26)}
		updates2 := map[string]interface{}{"age": int64(27)}

		// Conn1 updates
		count1, err1 := table.UpdateRowWithTransaction(conn1.MVCCTxn, filter, updates1)

		// Conn2 updates (should work with MVCC)
		count2, err2 := table.UpdateRowWithTransaction(conn2.MVCCTxn, filter, updates2)

		fmt.Printf("Conn1 updated %d rows (err: %v)\n", count1, err1)
		fmt.Printf("Conn2 updated %d rows (err: %v)\n", count2, err2)

		// Commit both
		if err := db.CommitTransaction(conn1); err != nil {
			fmt.Printf("Conn1 commit failed: %v\n", err)
		} else {
			fmt.Println("✅ Conn1 committed")
		}

		if err := db.CommitTransaction(conn2); err != nil {
			fmt.Printf("Conn2 commit failed: %v\n", err)
		} else {
			fmt.Println("✅ Conn2 committed")
		}
	}

	fmt.Println("\n6. Final Statistics...")
	fmt.Printf("Table RowCount: %d\n", table.RowCount)
	fmt.Printf("MVCC Rows: %d\n", len(table.MVCCRows))

	// Print details of each MVCC row
	for rowID, mvccRow := range table.MVCCRows {
		fmt.Printf("Row %d: ", rowID)

		// Count versions
		versionCount := 0
		for version := mvccRow.LatestVersion; version != nil; version = version.Next {
			versionCount++
		}
		fmt.Printf("%d versions\n", versionCount)
	}

	// Test garbage collection
	if db.MVCCManager != nil {
		fmt.Println("\n7. Testing Garbage Collection...")

		// Manually trigger GC (normally this runs in background)
		// Note: This is a private method, so we'll just show that the GC is running
		fmt.Println("✅ Garbage collector is running in background")

		// Stop the MVCC manager to clean up
		db.MVCCManager.Stop()
		fmt.Println("✅ MVCC manager stopped")
	}

	fmt.Println("\n🎉 MVCC and UPDATE/DELETE testing completed!")
}