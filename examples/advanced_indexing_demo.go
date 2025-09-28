// Advanced Indexing Demo - Showcasing LSM-Tree and Bitmap Indexes
package main

import (
	"fmt"
	"time"

	"fastpostgres/pkg/engine"
	"fastpostgres/pkg/query"
	"fastpostgres/pkg/storage"
)

func main() {
	fmt.Println("=== FastPostgres Advanced Indexing Demo ===")
	fmt.Println()

	// Create index manager and indexed query engine
	indexManager := storage.NewIndexManager()
	queryEngine := query.NewIndexedQueryEngine(indexManager)

	// Create a sample table with user data
	table := engine.NewTable("users")

	// Add columns
	idCol := engine.NewColumn("id", engine.TypeInt64, 10000)
	nameCol := engine.NewColumn("name", engine.TypeString, 10000)
	ageCol := engine.NewColumn("age", engine.TypeInt64, 10000)
	statusCol := engine.NewColumn("status", engine.TypeString, 10000)

	table.AddColumn(idCol)
	table.AddColumn(nameCol)
	table.AddColumn(ageCol)
	table.AddColumn(statusCol)

	fmt.Println("📊 Creating sample dataset...")

	// Insert sample data
	sampleData := []map[string]interface{}{
		{"id": int64(1), "name": "Alice", "age": int64(25), "status": "active"},
		{"id": int64(2), "name": "Bob", "age": int64(30), "status": "active"},
		{"id": int64(3), "name": "Charlie", "age": int64(35), "status": "inactive"},
		{"id": int64(4), "name": "Diana", "age": int64(28), "status": "active"},
		{"id": int64(5), "name": "Eve", "age": int64(22), "status": "pending"},
		{"id": int64(6), "name": "Frank", "age": int64(45), "status": "active"},
		{"id": int64(7), "name": "Grace", "age": int64(33), "status": "inactive"},
		{"id": int64(8), "name": "Henry", "age": int64(29), "status": "active"},
	}

	for _, data := range sampleData {
		table.InsertRow(data)
	}

	fmt.Printf("✅ Inserted %d rows\n\n", len(sampleData))

	// Demo 1: Create LSM-Tree Index for Write-Heavy Workloads
	fmt.Println("🌳 Demo 1: LSM-Tree Index for High-Performance Writes")
	fmt.Println("----------------------------------------------------")

	err := indexManager.CreateIndex("idx_id_lsm", "users", "id", engine.LSMTreeIndex, true)
	if err != nil {
		fmt.Printf("❌ Error creating LSM index: %v\n", err)
	} else {
		fmt.Println("✅ Created LSM-Tree index on 'id' column")

		err = indexManager.BuildIndex(table, "idx_id_lsm")
		if err != nil {
			fmt.Printf("❌ Error building LSM index: %v\n", err)
		} else {
			fmt.Println("✅ Built LSM-Tree index with sample data")
		}
	}

	// Demo 2: Create Bitmap Index for Categorical Data
	fmt.Println("\n🎯 Demo 2: Bitmap Index for Categorical Data")
	fmt.Println("--------------------------------------------")

	err = indexManager.CreateIndex("idx_status_bitmap", "users", "status", engine.RoaringBitmapIndex, false)
	if err != nil {
		fmt.Printf("❌ Error creating bitmap index: %v\n", err)
	} else {
		fmt.Println("✅ Created Roaring Bitmap index on 'status' column")

		err = indexManager.BuildIndex(table, "idx_status_bitmap")
		if err != nil {
			fmt.Printf("❌ Error building bitmap index: %v\n", err)
		} else {
			fmt.Println("✅ Built Bitmap index with sample data")
		}
	}

	// Demo 3: Query Optimization Analysis
	fmt.Println("\n🔍 Demo 3: Query Optimization Analysis")
	fmt.Println("--------------------------------------")

	// Create sample query plans
	queries := []struct {
		description string
		plan        *engine.QueryPlan
	}{
		{
			description: "Point lookup by ID (LSM-Tree optimal)",
			plan: &engine.QueryPlan{
				Type:      engine.QuerySelect,
				TableName: "users",
				Columns:   []string{"*"},
				Filters: []*engine.FilterExpression{
					{
						Column:   "id",
						Operator: engine.OpEqual,
						Value:    int64(5),
					},
				},
			},
		},
		{
			description: "Filter by status (Bitmap index optimal)",
			plan: &engine.QueryPlan{
				Type:      engine.QuerySelect,
				TableName: "users",
				Columns:   []string{"name", "age"},
				Filters: []*engine.FilterExpression{
					{
						Column:   "status",
						Operator: engine.OpEqual,
						Value:    "active",
					},
				},
			},
		},
		{
			description: "Range query by age (B-Tree would be optimal)",
			plan: &engine.QueryPlan{
				Type:      engine.QuerySelect,
				TableName: "users",
				Columns:   []string{"name", "status"},
				Filters: []*engine.FilterExpression{
					{
						Column:   "age",
						Operator: engine.OpGreater,
						Value:    int64(30),
					},
				},
			},
		},
	}

	for i, q := range queries {
		fmt.Printf("\nQuery %d: %s\n", i+1, q.description)

		suggestion, err := queryEngine.OptimizeQuery(q.plan, table)
		if err != nil {
			fmt.Printf("❌ Error analyzing query: %v\n", err)
			continue
		}

		fmt.Printf("  Current Strategy: %s\n", suggestion.CurrentStrategy)

		if len(suggestion.Suggestions) > 0 {
			fmt.Println("  📈 Index Recommendations:")
			for _, sugg := range suggestion.Suggestions {
				fmt.Printf("    - %s on '%s': %.1fx speedup - %s\n",
					getIndexTypeName(sugg.RecommendedType),
					sugg.Column,
					sugg.EstimatedSpeedup,
					sugg.Reason)
			}
		} else {
			fmt.Println("  ✅ Query is already optimized")
		}
	}

	// Demo 4: Index Statistics
	fmt.Println("\n📈 Demo 4: Index Performance Statistics")
	fmt.Println("--------------------------------------")

	stats := indexManager.GetIndexStats()

	fmt.Printf("Traditional Indexes: %v\n", stats["traditional_indexes"])

	if lsmStats, ok := stats["lsm_indexes"].(map[string]interface{}); ok {
		fmt.Printf("LSM-Tree Indexes: %d\n", len(lsmStats))
		for name, info := range lsmStats {
			if infoMap, ok := info.(map[string]interface{}); ok {
				fmt.Printf("  - %s: %v rows\n", name, infoMap["row_count"])
			}
		}
	}

	if bitmapStats, ok := stats["bitmap_indexes"].(map[string]interface{}); ok {
		fmt.Printf("Bitmap Indexes: %d\n", len(bitmapStats))
		for name, stats := range bitmapStats {
			if bitmapInfo, ok := stats.(storage.BitmapIndexStats); ok {
				fmt.Printf("  - %s: %d values, %d rows, %.2f compression ratio\n",
					name, bitmapInfo.Cardinality, bitmapInfo.RowCount, bitmapInfo.CompressionRatio)
			}
		}
	}

	// Demo 5: Performance Comparison Simulation
	fmt.Println("\n⚡ Demo 5: Performance Simulation")
	fmt.Println("---------------------------------")

	fmt.Println("Simulating query performance with different index types:")
	fmt.Println()

	scenarios := []struct {
		scenario string
		description string
		estimatedTime time.Duration
	}{
		{"Sequential Scan", "No index - full table scan", 100 * time.Millisecond},
		{"B-Tree Index", "Balanced tree for range queries", 10 * time.Millisecond},
		{"Hash Index", "O(1) equality lookups", 1 * time.Millisecond},
		{"LSM-Tree Index", "Write-optimized with good read performance", 5 * time.Millisecond},
		{"Bitmap Index", "Compressed bitmaps for categorical data", 2 * time.Millisecond},
	}

	for _, scenario := range scenarios {
		fmt.Printf("%-20s: %s (~%v)\n", scenario.scenario, scenario.description, scenario.estimatedTime)
	}

	fmt.Println()
	fmt.Println("🎉 Advanced Indexing Demo Complete!")
	fmt.Println()
	fmt.Println("Key Benefits Demonstrated:")
	fmt.Println("✅ LSM-Tree indexes optimize for write-heavy workloads")
	fmt.Println("✅ Bitmap indexes provide excellent compression for categorical data")
	fmt.Println("✅ Query optimizer automatically selects the best index")
	fmt.Println("✅ Comprehensive performance statistics and monitoring")
	fmt.Println("✅ Smart recommendations for index creation")

	// Cleanup
	if err := indexManager.Close(); err != nil {
		fmt.Printf("Warning: Error closing index manager: %v\n", err)
	}
}

func getIndexTypeName(indexType engine.IndexType) string {
	switch indexType {
	case engine.BTreeIndex:
		return "B-Tree Index"
	case engine.HashIndex:
		return "Hash Index"
	case engine.BitmapIndex:
		return "Bitmap Index"
	case engine.LSMTreeIndex:
		return "LSM-Tree Index"
	case engine.RoaringBitmapIndex:
		return "Roaring Bitmap Index"
	default:
		return "Unknown Index"
	}
}