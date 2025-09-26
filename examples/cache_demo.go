package main

import (
	"fmt"
	"math/rand"
	"time"
)

func main() {
	fmt.Println("FastPostgres Query Caching Demo")
	fmt.Println("===============================")

	// Create database with sample data
	db := setupDatabaseWithData()

	// Setup query cache with configuration
	cacheConfig := &CacheConfig{
		MaxSize:         100,
		MaxAge:          10 * time.Minute,
		CleanupInterval: 2 * time.Minute,
		MaxMemoryMB:     50,
	}

	cacheEngine := NewQueryCacheEngine(cacheConfig)
	defer cacheEngine.Stop()

	// Create engines
	parser := NewSQLParser()
	vectorEngine := NewVectorizedEngine()

	fmt.Printf("Created database with %d users and %d departments\n", getUserCount(db), getDepartmentCount(db))
	fmt.Println("Cache configured: Max 100 entries, 10min TTL, 50MB limit")

	// Test queries
	testQueries := []string{
		"SELECT * FROM users WHERE age > 25",
		"SELECT name, age FROM users WHERE age < 35",
		"SELECT COUNT(*) FROM users",
		"SELECT * FROM users WHERE name = 'Alice'",
		"SELECT AVG(age) FROM users",
		"SELECT MIN(age), MAX(age) FROM users",
	}

	fmt.Println("\n=== CACHE PERFORMANCE TESTING ===")

	// First execution (cache misses)
	fmt.Println("\n--- First Execution (Cache Misses) ---")
	var firstRunTimes []time.Duration

	for i, sql := range testQueries {
		plan, err := parser.Parse(sql)
		if err != nil {
			fmt.Printf("Query %d failed to parse: %v\n", i+1, err)
			continue
		}

		tableInterface, exists := db.Tables.Load(plan.TableName)
		if !exists {
			fmt.Printf("Query %d: Table %s not found\n", i+1, plan.TableName)
			continue
		}

		table := tableInterface.(*Table)

		start := time.Now()
		result, err := vectorEngine.CachedExecuteSelect(plan, table, cacheEngine)
		elapsed := time.Since(start)

		firstRunTimes = append(firstRunTimes, elapsed)

		if err != nil {
			fmt.Printf("Query %d failed: %v\n", i+1, err)
			continue
		}

		fmt.Printf("Query %d: %v (%d rows)\n", i+1, elapsed, len(result.Rows))
	}

	// Second execution (cache hits)
	fmt.Println("\n--- Second Execution (Cache Hits) ---")
	var secondRunTimes []time.Duration

	for i, sql := range testQueries {
		plan, err := parser.Parse(sql)
		if err != nil {
			continue
		}

		tableInterface, exists := db.Tables.Load(plan.TableName)
		if !exists {
			continue
		}

		table := tableInterface.(*Table)

		start := time.Now()
		result, err := vectorEngine.CachedExecuteSelect(plan, table, cacheEngine)
		elapsed := time.Since(start)

		secondRunTimes = append(secondRunTimes, elapsed)

		if err != nil {
			fmt.Printf("Query %d failed: %v\n", i+1, err)
			continue
		}

		fmt.Printf("Query %d: %v (%d rows) [CACHED]\n", i+1, elapsed, len(result.Rows))
	}

	// Performance comparison
	fmt.Println("\n--- Performance Improvement ---")
	for i := 0; i < len(firstRunTimes) && i < len(secondRunTimes); i++ {
		if secondRunTimes[i] > 0 {
			speedup := float64(firstRunTimes[i]) / float64(secondRunTimes[i])
			fmt.Printf("Query %d: %.1fx faster (from %v to %v)\n",
				i+1, speedup, firstRunTimes[i], secondRunTimes[i])
		}
	}

	// Cache statistics
	stats := cacheEngine.GetStats()
	fmt.Println("\n=== CACHE STATISTICS ===")
	fmt.Printf("Cache Hits: %d\n", stats.HitCount)
	fmt.Printf("Cache Misses: %d\n", stats.MissCount)
	fmt.Printf("Hit Ratio: %.2f%%\n", stats.HitRatio*100)
	fmt.Printf("Cache Size: %d entries\n", stats.CacheSize)
	fmt.Printf("Memory Usage: %.1f KB\n", float64(stats.MemoryUsage)/1024)
	fmt.Printf("Evictions: %d\n", stats.EvictionCount)

	// Test cache invalidation
	fmt.Println("\n=== CACHE INVALIDATION TEST ===")
	fmt.Println("Simulating data modification...")

	// Simulate a data modification that should invalidate cache
	invalidatedCount := cacheEngine.InvalidateTable("users")
	fmt.Printf("Invalidated %d cached queries for 'users' table\n", invalidatedCount)

	// Execute a query again after invalidation (should be cache miss)
	plan, _ := parser.Parse(testQueries[0])
	tableInterface, _ := db.Tables.Load(plan.TableName)
	table := tableInterface.(*Table)

	start := time.Now()
	vectorEngine.CachedExecuteSelect(plan, table, cacheEngine)
	elapsed := time.Since(start)

	fmt.Printf("Query re-executed after invalidation: %v (cache miss)\n", elapsed)

	// Final statistics
	finalStats := cacheEngine.GetStats()
	fmt.Println("\n--- Final Cache Statistics ---")
	fmt.Printf("Total Queries: %d\n", finalStats.TotalQueries)
	fmt.Printf("Hit Ratio: %.2f%%\n", finalStats.HitRatio*100)

	// Benchmark cache performance under load
	fmt.Println("\n=== CACHE LOAD TESTING ===")
	benchmarkCachePerformance(db, cacheEngine, parser, vectorEngine)
}

func setupDatabaseWithData() *Database {
	db := NewDatabase("cache_demo")

	// Create users table with more data
	usersTable := NewTable("users")
	usersTable.AddColumn(NewColumn("id", TypeInt64, 10000))
	usersTable.AddColumn(NewColumn("name", TypeString, 10000))
	usersTable.AddColumn(NewColumn("age", TypeInt64, 10000))

	// Insert sample users
	names := []string{
		"Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry",
		"Ivy", "Jack", "Kate", "Liam", "Mia", "Noah", "Olivia", "Paul",
		"Quinn", "Ruby", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xavier", "Yara", "Zoe",
	}

	for i := 0; i < 1000; i++ {
		name := names[i%len(names)]
		if i >= len(names) {
			name = fmt.Sprintf("%s_%d", name, i/len(names))
		}

		userData := map[string]interface{}{
			"id":   int64(i + 1),
			"name": name,
			"age":  int64(20 + rand.Intn(40)), // Ages 20-59
		}
		usersTable.InsertRow(userData)
	}

	db.Tables.Store("users", usersTable)

	// Create departments table
	deptsTable := NewTable("departments")
	deptsTable.AddColumn(NewColumn("id", TypeInt64, 100))
	deptsTable.AddColumn(NewColumn("name", TypeString, 100))

	departments := []string{"Engineering", "Sales", "Marketing", "HR", "Finance", "Operations"}
	for i, dept := range departments {
		deptData := map[string]interface{}{
			"id":   int64(i + 1),
			"name": dept,
		}
		deptsTable.InsertRow(deptData)
	}

	db.Tables.Store("departments", deptsTable)

	return db
}

func getUserCount(db *Database) int {
	if tableInterface, exists := db.Tables.Load("users"); exists {
		table := tableInterface.(*Table)
		return int(table.RowCount)
	}
	return 0
}

func getDepartmentCount(db *Database) int {
	if tableInterface, exists := db.Tables.Load("departments"); exists {
		table := tableInterface.(*Table)
		return int(table.RowCount)
	}
	return 0
}

func benchmarkCachePerformance(db *Database, cacheEngine *QueryCacheEngine, parser *SQLParser, vectorEngine *VectorizedEngine) {
	fmt.Println("Running 1000 mixed queries to test cache under load...")

	// Mix of different queries
	queryTemplates := []string{
		"SELECT * FROM users WHERE age > %d",
		"SELECT COUNT(*) FROM users WHERE age < %d",
		"SELECT name FROM users WHERE age = %d",
		"SELECT AVG(age) FROM users",
		"SELECT MIN(age), MAX(age) FROM users",
	}

	start := time.Now()
	cacheHits := 0
	cacheMisses := 0

	for i := 0; i < 1000; i++ {
		// Generate varied queries
		var sql string
		if i%5 == 3 || i%5 == 4 {
			// Aggregate queries (no parameters)
			sql = queryTemplates[i%5]
		} else {
			// Parameterized queries
			param := 25 + rand.Intn(30) // Age between 25-54
			sql = fmt.Sprintf(queryTemplates[i%5], param)
		}

		plan, err := parser.Parse(sql)
		if err != nil {
			continue
		}

		tableInterface, exists := db.Tables.Load(plan.TableName)
		if !exists {
			continue
		}

		table := tableInterface.(*Table)

		initialStats := cacheEngine.GetStats()
		vectorEngine.CachedExecuteSelect(plan, table, cacheEngine)
		finalStats := cacheEngine.GetStats()

		// Check if this was a hit or miss
		if finalStats.HitCount > initialStats.HitCount {
			cacheHits++
		} else {
			cacheMisses++
		}
	}

	totalTime := time.Since(start)
	finalStats := cacheEngine.GetStats()

	fmt.Printf("\n--- Load Test Results ---\n")
	fmt.Printf("Total time: %v\n", totalTime)
	fmt.Printf("Queries per second: %.0f\n", 1000.0/totalTime.Seconds())
	fmt.Printf("Cache hits during test: %d\n", cacheHits)
	fmt.Printf("Cache misses during test: %d\n", cacheMisses)
	fmt.Printf("Hit ratio during test: %.2f%%\n", float64(cacheHits)/float64(cacheHits+cacheMisses)*100)
	fmt.Printf("Overall hit ratio: %.2f%%\n", finalStats.HitRatio*100)
	fmt.Printf("Final cache size: %d entries\n", finalStats.CacheSize)
	fmt.Printf("Memory usage: %.1f KB\n", float64(finalStats.MemoryUsage)/1024)

	// Test cache warming
	fmt.Println("\n=== CACHE WARMING TEST ===")
	testCacheWarming(db, cacheEngine, vectorEngine)
}

func testCacheWarming(db *Database, cacheEngine *QueryCacheEngine, vectorEngine *VectorizedEngine) {
	// Clear cache first
	cacheEngine.Clear()
	fmt.Println("Cache cleared for warming test")

	// Create cache warmer
	warmer := NewCacheWarmer(cacheEngine, db, vectorEngine)

	// Add frequently used queries
	warmer.AddWarmQuery("SELECT COUNT(*) FROM users", "users", 10)
	warmer.AddWarmQuery("SELECT AVG(age) FROM users", "users", 8)
	warmer.AddWarmQuery("SELECT * FROM users WHERE age > 30", "users", 5)
	warmer.AddWarmQuery("SELECT name FROM users WHERE age < 25", "users", 3)

	fmt.Println("Warming cache with 4 frequently used queries...")

	start := time.Now()
	warmer.WarmCache()
	warmTime := time.Since(start)

	stats := cacheEngine.GetStats()
	fmt.Printf("Cache warming completed in: %v\n", warmTime)
	fmt.Printf("Entries in cache: %d\n", stats.CacheSize)
	fmt.Printf("Memory usage: %.1f KB\n", float64(stats.MemoryUsage)/1024)

	// Now test that these queries are served from cache
	parser := NewSQLParser()
	fmt.Println("\nTesting warmed queries:")

	warmQueries := []string{
		"SELECT COUNT(*) FROM users",
		"SELECT AVG(age) FROM users",
	}

	for i, sql := range warmQueries {
		plan, _ := parser.Parse(sql)
		tableInterface, _ := db.Tables.Load(plan.TableName)
		table := tableInterface.(*Table)

		start := time.Now()
		result, _ := vectorEngine.CachedExecuteSelect(plan, table, cacheEngine)
		elapsed := time.Since(start)

		fmt.Printf("Warm query %d: %v (%d rows) - should be very fast!\n",
			i+1, elapsed, len(result.Rows))
	}

	finalStats := cacheEngine.GetStats()
	fmt.Printf("Final hit ratio: %.2f%%\n", finalStats.HitRatio*100)
}