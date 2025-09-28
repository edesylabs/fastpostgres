// Package query provides an indexed query execution engine that integrates
// LSM-Tree and Bitmap indexes with the vectorized execution engine.
package query

import (
	"fmt"
	"sort"
	"time"

	"fastpostgres/pkg/engine"
	"fastpostgres/pkg/storage"
)

// IndexedQueryEngine provides index-aware query execution
type IndexedQueryEngine struct {
	vectorizedEngine *VectorizedEngine
	indexManager     *storage.IndexManager
	queryOptimizer   *IndexQueryOptimizer
	stats            *QueryExecutionStats
}

// IndexQueryOptimizer determines the best execution plan using available indexes
type IndexQueryOptimizer struct {
	costModel *IndexCostModel
}

// IndexCostModel estimates the cost of different query execution strategies
type IndexCostModel struct {
	seqScanCostPerRow     float64
	indexLookupCostPerRow float64
	bitmapIndexCost       float64
	lsmIndexCost          float64
}

// QueryExecutionStats tracks performance metrics
type QueryExecutionStats struct {
	IndexScans       int64
	SequentialScans  int64
	LSMIndexUsage    int64
	BitmapIndexUsage int64
	TotalQueries     int64
	AvgResponseTime  time.Duration
}

// IndexScanResult represents the result of an index scan
type IndexScanResult struct {
	IndexName    string
	IndexType    engine.IndexType
	RowIDs       []int
	EstimatedCost float64
	ActualTime   time.Duration
}

// NewIndexedQueryEngine creates a new index-aware query engine
func NewIndexedQueryEngine(indexManager *storage.IndexManager) *IndexedQueryEngine {
	return &IndexedQueryEngine{
		vectorizedEngine: NewVectorizedEngine(),
		indexManager:     indexManager,
		queryOptimizer:   NewIndexQueryOptimizer(),
		stats:            &QueryExecutionStats{},
	}
}

func NewIndexQueryOptimizer() *IndexQueryOptimizer {
	return &IndexQueryOptimizer{
		costModel: &IndexCostModel{
			seqScanCostPerRow:     1.0,
			indexLookupCostPerRow: 0.1,
			bitmapIndexCost:       0.05, // Bitmap indexes are very fast for categorical data
			lsmIndexCost:          0.15, // LSM indexes have higher read cost but excellent write performance
		},
	}
}

// ExecuteQuery is the main entry point for executing queries with index optimization
func (iqe *IndexedQueryEngine) ExecuteQuery(plan *engine.QueryPlan, table *engine.Table) (*engine.QueryResult, error) {
	startTime := time.Now()

	// Analyze query and determine optimal execution strategy
	strategy, err := iqe.analyzeQuery(plan, table)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze query: %w", err)
	}

	var result *engine.QueryResult

	switch strategy.Type {
	case StrategyIndexScan:
		result, err = iqe.executeIndexScan(plan, table, strategy)
	case StrategyBitmapIndexScan:
		result, err = iqe.executeBitmapIndexScan(plan, table, strategy)
	case StrategyLSMIndexScan:
		result, err = iqe.executeLSMIndexScan(plan, table, strategy)
	case StrategySequentialScan:
		result, err = iqe.vectorizedEngine.ExecuteSelect(plan, table)
	default:
		result, err = iqe.vectorizedEngine.ExecuteSelect(plan, table)
	}

	if err != nil {
		return nil, err
	}

	// Update statistics
	iqe.updateStats(strategy, time.Since(startTime))

	return result, nil
}

// ExecutionStrategy represents different query execution approaches
type ExecutionStrategy struct {
	Type          StrategyType
	IndexName     string
	IndexType     engine.IndexType
	EstimatedCost float64
	Filter        *engine.FilterExpression
}

type StrategyType uint8

const (
	StrategySequentialScan StrategyType = iota
	StrategyIndexScan
	StrategyBitmapIndexScan
	StrategyLSMIndexScan
	StrategyCompositeIndexScan
)

// analyzeQuery determines the optimal execution strategy for a query
func (iqe *IndexedQueryEngine) analyzeQuery(plan *engine.QueryPlan, table *engine.Table) (*ExecutionStrategy, error) {
	if len(plan.Filters) == 0 {
		// No filters - use sequential scan
		return &ExecutionStrategy{
			Type: StrategySequentialScan,
			EstimatedCost: float64(table.RowCount) * iqe.queryOptimizer.costModel.seqScanCostPerRow,
		}, nil
	}

	// Find the best index for the query filters
	bestStrategy := &ExecutionStrategy{
		Type: StrategySequentialScan,
		EstimatedCost: float64(table.RowCount) * iqe.queryOptimizer.costModel.seqScanCostPerRow,
	}

	// Check each filter to find applicable indexes
	for _, filter := range plan.Filters {
		strategy := iqe.findBestIndexForFilter(table, filter)
		if strategy != nil && strategy.EstimatedCost < bestStrategy.EstimatedCost {
			bestStrategy = strategy
		}
	}

	return bestStrategy, nil
}

// findBestIndexForFilter finds the best index for a specific filter condition
func (iqe *IndexedQueryEngine) findBestIndexForFilter(table *engine.Table, filter *engine.FilterExpression) *ExecutionStrategy {
	// Get all indexes for this table
	indexStats := iqe.indexManager.GetIndexStats()

	var bestStrategy *ExecutionStrategy
	lowestCost := float64(table.RowCount) * iqe.queryOptimizer.costModel.seqScanCostPerRow

	// Note: In a real implementation, we would check traditional indexes here
	// For now, we focus on LSM and bitmap indexes

	// Check LSM indexes
	if lsmStats, ok := indexStats["lsm_indexes"].(map[string]interface{}); ok {
		for indexName := range lsmStats {
			if iqe.isIndexApplicableToFilter(indexName, filter) {
				cost := iqe.queryOptimizer.costModel.lsmIndexCost * 100 // Estimate
				if cost < lowestCost {
					bestStrategy = &ExecutionStrategy{
						Type:          StrategyLSMIndexScan,
						IndexName:     indexName,
						IndexType:     engine.LSMTreeIndex,
						EstimatedCost: cost,
						Filter:        filter,
					}
					lowestCost = cost
				}
			}
		}
	}

	// Check bitmap indexes
	if bitmapStats, ok := indexStats["bitmap_indexes"].(map[string]interface{}); ok {
		for indexName := range bitmapStats {
			if iqe.isIndexApplicableToFilter(indexName, filter) {
				cost := iqe.queryOptimizer.costModel.bitmapIndexCost * 50 // Estimate
				if cost < lowestCost {
					bestStrategy = &ExecutionStrategy{
						Type:          StrategyBitmapIndexScan,
						IndexName:     indexName,
						IndexType:     engine.RoaringBitmapIndex,
						EstimatedCost: cost,
						Filter:        filter,
					}
					lowestCost = cost
				}
			}
		}
	}

	return bestStrategy
}

// estimateIndexCost estimates the cost of using an index for a query
func (iqe *IndexedQueryEngine) estimateIndexCost(indexType engine.IndexType, filter *engine.FilterExpression, tableRowCount uint64) float64 {
	baseCost := iqe.queryOptimizer.costModel.indexLookupCostPerRow

	switch indexType {
	case engine.BTreeIndex:
		// B-Tree is efficient for range queries and equality
		if filter.Operator == engine.OpEqual {
			return baseCost * 10 // Log(n) lookup
		} else if filter.Operator == engine.OpLess || filter.Operator == engine.OpGreater {
			return baseCost * float64(tableRowCount) * 0.1 // Estimate 10% selectivity
		}
	case engine.HashIndex:
		// Hash index is excellent for equality, poor for ranges
		if filter.Operator == engine.OpEqual {
			return baseCost * 1 // O(1) lookup
		}
		return float64(tableRowCount) * iqe.queryOptimizer.costModel.seqScanCostPerRow // Not applicable
	case engine.BitmapIndex:
		// Traditional bitmap index
		return baseCost * 50
	}

	return float64(tableRowCount) * iqe.queryOptimizer.costModel.seqScanCostPerRow
}

// isIndexApplicableToFilter checks if an index can be used for a filter
func (iqe *IndexedQueryEngine) isIndexApplicableToFilter(indexName string, filter *engine.FilterExpression) bool {
	// This is a simplified check - in a real implementation, we'd examine
	// the index metadata to determine the column it indexes
	return true // Placeholder
}

// executeIndexScan executes a query using a traditional index
func (iqe *IndexedQueryEngine) executeIndexScan(plan *engine.QueryPlan, table *engine.Table, strategy *ExecutionStrategy) (*engine.QueryResult, error) {
	// Convert filter to index query
	indexQuery := iqe.convertFilterToIndexQuery(strategy.Filter)

	// Perform index lookup
	rowIDs, err := iqe.indexManager.QueryIndex(strategy.IndexName, indexQuery)
	if err != nil {
		return nil, fmt.Errorf("index scan failed: %w", err)
	}

	// Fetch rows using the row IDs
	return iqe.fetchRowsByIDs(plan, table, rowIDs)
}

// executeBitmapIndexScan executes a query using a roaring bitmap index
func (iqe *IndexedQueryEngine) executeBitmapIndexScan(plan *engine.QueryPlan, table *engine.Table, strategy *ExecutionStrategy) (*engine.QueryResult, error) {
	indexQuery := iqe.convertFilterToIndexQuery(strategy.Filter)

	rowIDs, err := iqe.indexManager.QueryIndex(strategy.IndexName, indexQuery)
	if err != nil {
		return nil, fmt.Errorf("bitmap index scan failed: %w", err)
	}

	iqe.stats.BitmapIndexUsage++
	return iqe.fetchRowsByIDs(plan, table, rowIDs)
}

// executeLSMIndexScan executes a query using an LSM-Tree index
func (iqe *IndexedQueryEngine) executeLSMIndexScan(plan *engine.QueryPlan, table *engine.Table, strategy *ExecutionStrategy) (*engine.QueryResult, error) {
	indexQuery := iqe.convertFilterToIndexQuery(strategy.Filter)

	rowIDs, err := iqe.indexManager.QueryIndex(strategy.IndexName, indexQuery)
	if err != nil {
		return nil, fmt.Errorf("LSM index scan failed: %w", err)
	}

	iqe.stats.LSMIndexUsage++
	return iqe.fetchRowsByIDs(plan, table, rowIDs)
}

// convertFilterToIndexQuery converts a filter expression to an index query
func (iqe *IndexedQueryEngine) convertFilterToIndexQuery(filter *engine.FilterExpression) storage.IndexQuery {
	switch filter.Operator {
	case engine.OpEqual:
		return storage.IndexQuery{
			Type:  storage.QueryEqual,
			Value: filter.Value,
		}
	case engine.OpLess, engine.OpLessEqual, engine.OpGreater, engine.OpGreaterEqual:
		// For range queries, we need to determine the range
		// This is simplified - real implementation would handle more complex cases
		return storage.IndexQuery{
			Type:     storage.QueryRange,
			MinValue: filter.Value,
			MaxValue: filter.Value,
		}
	case engine.OpIn:
		// Assume filter.Value is a slice for IN queries
		if values, ok := filter.Value.([]interface{}); ok {
			return storage.IndexQuery{
				Type:   storage.QueryIn,
				Values: values,
			}
		}
	}

	// Default to equality
	return storage.IndexQuery{
		Type:  storage.QueryEqual,
		Value: filter.Value,
	}
}

// fetchRowsByIDs retrieves rows from the table using the provided row IDs
func (iqe *IndexedQueryEngine) fetchRowsByIDs(plan *engine.QueryPlan, table *engine.Table, rowIDs []int) (*engine.QueryResult, error) {
	if len(rowIDs) == 0 {
		return &engine.QueryResult{
			Columns: plan.Columns,
			Rows:    make([][]interface{}, 0),
		}, nil
	}

	// Sort row IDs for better cache locality
	sort.Ints(rowIDs)

	// Determine which columns to fetch
	var selectedCols []*engine.Column
	var columnNames []string

	if len(plan.Columns) == 1 && plan.Columns[0] == "*" {
		selectedCols = table.Columns
		columnNames = make([]string, len(table.Columns))
		for i, col := range table.Columns {
			columnNames[i] = col.Name
		}
	} else {
		selectedCols = make([]*engine.Column, len(plan.Columns))
		columnNames = plan.Columns

		for i, colName := range plan.Columns {
			col := table.GetColumn(colName)
			if col == nil {
				return nil, fmt.Errorf("column %s not found", colName)
			}
			selectedCols[i] = col
		}
	}

	// Fetch data for selected row IDs
	rows := make([][]interface{}, 0, len(rowIDs))
	types := make([]engine.DataType, len(selectedCols))

	for i, col := range selectedCols {
		types[i] = col.Type
	}

	for _, rowID := range rowIDs {
		if uint64(rowID) >= table.RowCount {
			continue // Skip invalid row IDs
		}

		row := make([]interface{}, len(selectedCols))
		for j, col := range selectedCols {
			switch col.Type {
			case engine.TypeInt64:
				if value, ok := col.GetInt64(uint64(rowID)); ok {
					row[j] = value
				} else {
					row[j] = nil
				}
			case engine.TypeString:
				if value, ok := col.GetString(uint64(rowID)); ok {
					row[j] = value
				} else {
					row[j] = nil
				}
			default:
				row[j] = nil
			}
		}
		rows = append(rows, row)
	}

	return &engine.QueryResult{
		Columns: columnNames,
		Types:   types,
		Rows:    rows,
	}, nil
}

// updateStats updates query execution statistics
func (iqe *IndexedQueryEngine) updateStats(strategy *ExecutionStrategy, executionTime time.Duration) {
	iqe.stats.TotalQueries++

	switch strategy.Type {
	case StrategyIndexScan:
		iqe.stats.IndexScans++
	case StrategySequentialScan:
		iqe.stats.SequentialScans++
	case StrategyBitmapIndexScan:
		iqe.stats.BitmapIndexUsage++
	case StrategyLSMIndexScan:
		iqe.stats.LSMIndexUsage++
	}

	// Update average response time
	totalTime := time.Duration(iqe.stats.TotalQueries-1) * iqe.stats.AvgResponseTime + executionTime
	iqe.stats.AvgResponseTime = totalTime / time.Duration(iqe.stats.TotalQueries)
}

// GetStats returns current query execution statistics
func (iqe *IndexedQueryEngine) GetStats() *QueryExecutionStats {
	return iqe.stats
}

// GetIndexManager returns the index manager for testing and external access
func (iqe *IndexedQueryEngine) GetIndexManager() *storage.IndexManager {
	return iqe.indexManager
}


// OptimizeQuery suggests the best indexes for a given query pattern
func (iqe *IndexedQueryEngine) OptimizeQuery(plan *engine.QueryPlan, table *engine.Table) (*QueryOptimizationSuggestion, error) {
	suggestion := &QueryOptimizationSuggestion{
		CurrentStrategy: "Sequential Scan",
		Suggestions:     make([]IndexSuggestion, 0),
	}

	// Analyze filters to suggest appropriate indexes
	for _, filter := range plan.Filters {
		indexSugg := iqe.suggestIndexForFilter(filter, table)
		if indexSugg != nil {
			suggestion.Suggestions = append(suggestion.Suggestions, *indexSugg)
		}
	}

	return suggestion, nil
}

// QueryOptimizationSuggestion provides recommendations for query optimization
type QueryOptimizationSuggestion struct {
	CurrentStrategy string
	Suggestions     []IndexSuggestion
}

// IndexSuggestion recommends creating a specific type of index
type IndexSuggestion struct {
	Column            string
	RecommendedType   engine.IndexType
	EstimatedSpeedup  float64
	Reason            string
}

// suggestIndexForFilter suggests the best index type for a filter condition
func (iqe *IndexedQueryEngine) suggestIndexForFilter(filter *engine.FilterExpression, table *engine.Table) *IndexSuggestion {
	column := table.GetColumn(filter.Column)
	if column == nil {
		return nil
	}

	// Determine the best index type based on data characteristics and query pattern
	switch filter.Operator {
	case engine.OpEqual:
		// For equality queries, suggest hash index for high-cardinality data,
		// bitmap index for low-cardinality categorical data
		if iqe.isLowCardinality(column) {
			return &IndexSuggestion{
				Column:           filter.Column,
				RecommendedType:  engine.RoaringBitmapIndex,
				EstimatedSpeedup: 10.0,
				Reason:           "Low-cardinality categorical data benefits from bitmap indexes",
			}
		} else {
			return &IndexSuggestion{
				Column:           filter.Column,
				RecommendedType:  engine.HashIndex,
				EstimatedSpeedup: 100.0,
				Reason:           "Hash index provides O(1) lookup for equality queries",
			}
		}
	case engine.OpLess, engine.OpLessEqual, engine.OpGreater, engine.OpGreaterEqual:
		// For range queries, suggest B-Tree or LSM-Tree
		if iqe.isWriteHeavy(table) {
			return &IndexSuggestion{
				Column:           filter.Column,
				RecommendedType:  engine.LSMTreeIndex,
				EstimatedSpeedup: 5.0,
				Reason:           "LSM-Tree index optimizes for write-heavy workloads while maintaining good range query performance",
			}
		} else {
			return &IndexSuggestion{
				Column:           filter.Column,
				RecommendedType:  engine.BTreeIndex,
				EstimatedSpeedup: 20.0,
				Reason:           "B-Tree index provides efficient range scans",
			}
		}
	}

	return nil
}

// isLowCardinality estimates if a column has low cardinality (suitable for bitmap index)
func (iqe *IndexedQueryEngine) isLowCardinality(column *engine.Column) bool {
	// Simplified heuristic - in practice, would analyze data distribution
	return column.Length < 10000 || // Small datasets are likely low cardinality
		   column.Type == engine.TypeBool // Boolean columns are always low cardinality
}

// isWriteHeavy estimates if a table has write-heavy workload (suitable for LSM-Tree)
func (iqe *IndexedQueryEngine) isWriteHeavy(table *engine.Table) bool {
	// Simplified heuristic - in practice, would analyze query patterns
	// For now, assume tables with frequent inserts benefit from LSM-Tree
	return true // Placeholder
}