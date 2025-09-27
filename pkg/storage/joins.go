// Package storage provides high-performance join execution capabilities.
// It implements hash joins, sort-merge joins, and nested loop joins with parallel execution support.
package storage

import (
	"fmt"
	"sort"
	"sync"

	"fastpostgres/pkg/engine"
)

// JoinEngine executes various types of join operations efficiently.
// It supports hash joins, sort-merge joins, and nested loop joins.
type JoinEngine struct {
	hashTableSize int
	useParallel   bool
	workerCount   int
}

// HashTable stores build-side data for hash join operations.
type HashTable struct {
	buckets []HashJoinBucket
	mask    int
	size    int
}

// HashJoinBucket represents a single hash table bucket.
type HashJoinBucket struct {
	entries []HashJoinEntry
	mu      sync.RWMutex
}

// HashJoinEntry stores a join key and its associated row IDs.
type HashJoinEntry struct {
	key    interface{}
	rowIds []int
	rows   [][]interface{}
}

// JoinResult contains the output of a join operation with statistics.
type JoinResult struct {
	Columns []string
	Types   []engine.DataType
	Rows    [][]interface{}
	Stats   JoinStats
}

// JoinStats tracks performance metrics for join operations.
type JoinStats struct {
	BuildTime    int64 // nanoseconds
	ProbeTime    int64 // nanoseconds
	RowsMatched  int
	HashCollisions int
}

// SortedTable represents a table sorted by the join column.
type SortedTable struct {
	columns    []string
	types      []engine.DataType
	rows       [][]interface{}
	sortColumn int
	sorted     bool
}

// NewJoinEngine creates a new join execution engine.
func NewJoinEngine() *JoinEngine {
	return &JoinEngine{
		hashTableSize: 65536,
		useParallel:   true,
		workerCount:   4,
	}
}

// ExecuteHashJoin performs a hash join between two tables.
// Hash joins are optimal for equi-joins with large datasets.
func (je *JoinEngine) ExecuteHashJoin(leftTable, rightTable *engine.Table, joinExpr *engine.JoinExpression) (*JoinResult, error) {
	// Find join columns
	leftCol := leftTable.GetColumn(joinExpr.LeftCol)
	rightCol := rightTable.GetColumn(joinExpr.RightCol)

	if leftCol == nil {
		return nil, fmt.Errorf("left join column %s not found", joinExpr.LeftCol)
	}
	if rightCol == nil {
		return nil, fmt.Errorf("right join column %s not found", joinExpr.RightCol)
	}

	// Choose build and probe sides (smaller table as build side)
	var buildTable, probeTable *engine.Table
	var buildCol, probeCol *engine.Column
	var buildOnLeft bool

	if leftTable.RowCount <= rightTable.RowCount {
		buildTable, probeTable = leftTable, rightTable
		buildCol, probeCol = leftCol, rightCol
		buildOnLeft = true
	} else {
		buildTable, probeTable = rightTable, leftTable
		buildCol, probeCol = rightCol, leftCol
		buildOnLeft = false
	}

	// Build phase: create hash table from smaller table
	hashTable := je.buildHashTable(buildTable, buildCol, buildOnLeft)

	// Probe phase: probe larger table against hash table
	result := je.probeHashTable(hashTable, probeTable, probeCol, buildTable, !buildOnLeft, joinExpr.Type)

	return result, nil
}

func (je *JoinEngine) buildHashTable(table *engine.Table, joinCol *engine.Column, isLeft bool) *HashTable {
	ht := &HashTable{
		buckets: make([]HashJoinBucket, je.hashTableSize),
		mask:    je.hashTableSize - 1,
		size:    je.hashTableSize,
	}

	// Initialize buckets
	for i := range ht.buckets {
		ht.buckets[i].entries = make([]HashJoinEntry, 0)
	}

	// Build hash table
	rowCount := int(table.RowCount)
	for i := 0; i < rowCount; i++ {
		// Get join key
		var key interface{}
		var valid bool

		switch joinCol.Type {
		case engine.TypeInt64:
			key, valid = joinCol.GetInt64(uint64(i))
		case engine.TypeString:
			key, valid = joinCol.GetString(uint64(i))
		default:
			continue
		}

		if !valid {
			continue
		}

		// Get complete row
		row := make([]interface{}, len(table.Columns))
		for j, col := range table.Columns {
			switch col.Type {
			case engine.TypeInt64:
				if val, ok := col.GetInt64(uint64(i)); ok {
					row[j] = val
				} else {
					row[j] = nil
				}
			case engine.TypeString:
				if val, ok := col.GetString(uint64(i)); ok {
					row[j] = val
				} else {
					row[j] = nil
				}
			default:
				row[j] = nil
			}
		}

		// Hash and insert
		bucketIdx := je.hashValue(key) & ht.mask
		bucket := &ht.buckets[bucketIdx]

		bucket.mu.Lock()

		// Look for existing key
		found := false
		for k := range bucket.entries {
			if compareValues(bucket.entries[k].key, key) == 0 {
				bucket.entries[k].rowIds = append(bucket.entries[k].rowIds, i)
				bucket.entries[k].rows = append(bucket.entries[k].rows, row)
				found = true
				break
			}
		}

		if !found {
			bucket.entries = append(bucket.entries, HashJoinEntry{
				key:    key,
				rowIds: []int{i},
				rows:   [][]interface{}{row},
			})
		}

		bucket.mu.Unlock()
	}

	return ht
}

func (je *JoinEngine) probeHashTable(ht *HashTable, probeTable *engine.Table, probeCol *engine.Column, buildTable *engine.Table, buildOnRight bool, joinType engine.JoinType) *JoinResult {
	// Prepare result structure
	var resultColumns []string
	var resultTypes []engine.DataType

	// Add columns from both tables
	if buildOnRight {
		// Probe table columns first
		for _, col := range probeTable.Columns {
			resultColumns = append(resultColumns, probeTable.Name+"."+col.Name)
			resultTypes = append(resultTypes, col.Type)
		}
		// Build table columns second
		for _, col := range buildTable.Columns {
			resultColumns = append(resultColumns, buildTable.Name+"."+col.Name)
			resultTypes = append(resultTypes, col.Type)
		}
	} else {
		// Build table columns first
		for _, col := range buildTable.Columns {
			resultColumns = append(resultColumns, buildTable.Name+"."+col.Name)
			resultTypes = append(resultTypes, col.Type)
		}
		// Probe table columns second
		for _, col := range probeTable.Columns {
			resultColumns = append(resultColumns, probeTable.Name+"."+col.Name)
			resultTypes = append(resultTypes, col.Type)
		}
	}

	result := &JoinResult{
		Columns: resultColumns,
		Types:   resultTypes,
		Rows:    make([][]interface{}, 0),
	}

	// Probe phase
	rowCount := int(probeTable.RowCount)
	for i := 0; i < rowCount; i++ {
		// Get probe key
		var probeKey interface{}
		var valid bool

		switch probeCol.Type {
		case engine.TypeInt64:
			probeKey, valid = probeCol.GetInt64(uint64(i))
		case engine.TypeString:
			probeKey, valid = probeCol.GetString(uint64(i))
		default:
			continue
		}

		if !valid {
			continue
		}

		// Get probe row
		probeRow := make([]interface{}, len(probeTable.Columns))
		for j, col := range probeTable.Columns {
			switch col.Type {
			case engine.TypeInt64:
				if val, ok := col.GetInt64(uint64(i)); ok {
					probeRow[j] = val
				} else {
					probeRow[j] = nil
				}
			case engine.TypeString:
				if val, ok := col.GetString(uint64(i)); ok {
					probeRow[j] = val
				} else {
					probeRow[j] = nil
				}
			default:
				probeRow[j] = nil
			}
		}

		// Probe hash table
		bucketIdx := je.hashValue(probeKey) & ht.mask
		bucket := &ht.buckets[bucketIdx]

		bucket.mu.RLock()
		for _, entry := range bucket.entries {
			if compareValues(entry.key, probeKey) == 0 {
				// Found match, create joined rows
				for _, buildRow := range entry.rows {
					var joinedRow []interface{}

					if buildOnRight {
						joinedRow = append(joinedRow, probeRow...)
						joinedRow = append(joinedRow, buildRow...)
					} else {
						joinedRow = append(joinedRow, buildRow...)
						joinedRow = append(joinedRow, probeRow...)
					}

					result.Rows = append(result.Rows, joinedRow)
					result.Stats.RowsMatched++
				}
			}
		}
		bucket.mu.RUnlock()
	}

	return result
}

// ExecuteSortMergeJoin performs a sort-merge join between two tables.
// Sort-merge joins are optimal for pre-sorted data or range queries.
func (je *JoinEngine) ExecuteSortMergeJoin(leftTable, rightTable *engine.Table, joinExpr *engine.JoinExpression) (*JoinResult, error) {
	// Get join columns
	leftCol := leftTable.GetColumn(joinExpr.LeftCol)
	rightCol := rightTable.GetColumn(joinExpr.RightCol)

	if leftCol == nil {
		return nil, fmt.Errorf("left join column %s not found", joinExpr.LeftCol)
	}
	if rightCol == nil {
		return nil, fmt.Errorf("right join column %s not found", joinExpr.RightCol)
	}

	// Sort both tables by join column
	leftSorted := je.sortTable(leftTable, joinExpr.LeftCol)
	rightSorted := je.sortTable(rightTable, joinExpr.RightCol)

	// Merge phase
	result := je.mergeJoin(leftSorted, rightSorted, joinExpr)

	return result, nil
}

func (je *JoinEngine) sortTable(table *engine.Table, sortColumn string) *SortedTable {
	// Find sort column index
	var sortColIdx int = -1
	for i, col := range table.Columns {
		if col.Name == sortColumn {
			sortColIdx = i
			break
		}
	}

	if sortColIdx == -1 {
		return nil
	}

	// Extract all rows
	rowCount := int(table.RowCount)
	sortedTable := &SortedTable{
		columns:    make([]string, len(table.Columns)),
		types:      make([]engine.DataType, len(table.Columns)),
		rows:       make([][]interface{}, rowCount),
		sortColumn: sortColIdx,
	}

	// Copy column metadata
	for i, col := range table.Columns {
		sortedTable.columns[i] = col.Name
		sortedTable.types[i] = col.Type
	}

	// Extract rows
	for i := 0; i < rowCount; i++ {
		row := make([]interface{}, len(table.Columns))
		for j, col := range table.Columns {
			switch col.Type {
			case engine.TypeInt64:
				if val, ok := col.GetInt64(uint64(i)); ok {
					row[j] = val
				} else {
					row[j] = nil
				}
			case engine.TypeString:
				if val, ok := col.GetString(uint64(i)); ok {
					row[j] = val
				} else {
					row[j] = nil
				}
			default:
				row[j] = nil
			}
		}
		sortedTable.rows[i] = row
	}

	// Sort by join column
	sort.Slice(sortedTable.rows, func(i, j int) bool {
		valI := sortedTable.rows[i][sortColIdx]
		valJ := sortedTable.rows[j][sortColIdx]
		return compareValues(valI, valJ) < 0
	})

	sortedTable.sorted = true
	return sortedTable
}

func (je *JoinEngine) mergeJoin(left, right *SortedTable, joinExpr *engine.JoinExpression) *JoinResult {
	// Prepare result
	var resultColumns []string
	var resultTypes []engine.DataType

	// Add columns from both tables
	for _, col := range left.columns {
		resultColumns = append(resultColumns, "left."+col)
	}
	for _, col := range right.columns {
		resultColumns = append(resultColumns, "right."+col)
	}

	resultTypes = append(resultTypes, left.types...)
	resultTypes = append(resultTypes, right.types...)

	result := &JoinResult{
		Columns: resultColumns,
		Types:   resultTypes,
		Rows:    make([][]interface{}, 0),
	}

	// Merge join algorithm
	leftIdx, rightIdx := 0, 0

	for leftIdx < len(left.rows) && rightIdx < len(right.rows) {
		leftKey := left.rows[leftIdx][left.sortColumn]
		rightKey := right.rows[rightIdx][right.sortColumn]

		cmp := compareValues(leftKey, rightKey)

		if cmp == 0 {
			// Keys match - join all combinations
			leftStart := leftIdx
			rightStart := rightIdx

			// Find all left rows with same key
			for leftIdx < len(left.rows) && compareValues(left.rows[leftIdx][left.sortColumn], leftKey) == 0 {
				leftIdx++
			}

			// Find all right rows with same key
			for rightIdx < len(right.rows) && compareValues(right.rows[rightIdx][right.sortColumn], rightKey) == 0 {
				rightIdx++
			}

			// Create cartesian product for matching keys
			for i := leftStart; i < leftIdx; i++ {
				for j := rightStart; j < rightIdx; j++ {
					joinedRow := append(left.rows[i], right.rows[j]...)
					result.Rows = append(result.Rows, joinedRow)
					result.Stats.RowsMatched++
				}
			}
		} else if cmp < 0 {
			leftIdx++
		} else {
			rightIdx++
		}
	}

	return result
}

// ExecuteNestedLoopJoin performs a nested loop join between two tables.
// Nested loop joins handle any join condition but have O(n*m) complexity.
func (je *JoinEngine) ExecuteNestedLoopJoin(leftTable, rightTable *engine.Table, joinExpr *engine.JoinExpression) (*JoinResult, error) {
	// Prepare result structure
	var resultColumns []string
	var resultTypes []engine.DataType

	// Add columns from both tables
	for _, col := range leftTable.Columns {
		resultColumns = append(resultColumns, "left."+col.Name)
		resultTypes = append(resultTypes, col.Type)
	}
	for _, col := range rightTable.Columns {
		resultColumns = append(resultColumns, "right."+col.Name)
		resultTypes = append(resultTypes, col.Type)
	}

	result := &JoinResult{
		Columns: resultColumns,
		Types:   resultTypes,
		Rows:    make([][]interface{}, 0),
	}

	// Nested loop join - O(n*m) but handles any join condition
	leftRowCount := int(leftTable.RowCount)
	rightRowCount := int(rightTable.RowCount)

	for i := 0; i < leftRowCount; i++ {
		// Get left row
		leftRow := make([]interface{}, len(leftTable.Columns))
		for j, col := range leftTable.Columns {
			switch col.Type {
			case engine.TypeInt64:
				if val, ok := col.GetInt64(uint64(i)); ok {
					leftRow[j] = val
				} else {
					leftRow[j] = nil
				}
			case engine.TypeString:
				if val, ok := col.GetString(uint64(i)); ok {
					leftRow[j] = val
				} else {
					leftRow[j] = nil
				}
			default:
				leftRow[j] = nil
			}
		}

		for k := 0; k < rightRowCount; k++ {
			// Get right row
			rightRow := make([]interface{}, len(rightTable.Columns))
			for j, col := range rightTable.Columns {
				switch col.Type {
				case engine.TypeInt64:
					if val, ok := col.GetInt64(uint64(k)); ok {
						rightRow[j] = val
					} else {
						rightRow[j] = nil
					}
				case engine.TypeString:
					if val, ok := col.GetString(uint64(k)); ok {
						rightRow[j] = val
					} else {
						rightRow[j] = nil
					}
				default:
					rightRow[j] = nil
				}
			}

			// Evaluate join condition
			if je.evaluateJoinCondition(leftRow, rightRow, leftTable, rightTable, joinExpr) {
				joinedRow := append(leftRow, rightRow...)
				result.Rows = append(result.Rows, joinedRow)
				result.Stats.RowsMatched++
			}
		}
	}

	return result, nil
}

func (je *JoinEngine) evaluateJoinCondition(leftRow, rightRow []interface{}, leftTable, rightTable *engine.Table, joinExpr *engine.JoinExpression) bool {
	// Find column indices
	var leftColIdx, rightColIdx int = -1, -1

	for i, col := range leftTable.Columns {
		if col.Name == joinExpr.LeftCol {
			leftColIdx = i
			break
		}
	}

	for i, col := range rightTable.Columns {
		if col.Name == joinExpr.RightCol {
			rightColIdx = i
			break
		}
	}

	if leftColIdx == -1 || rightColIdx == -1 {
		return false
	}

	// Simple equality check for now - can be extended for other operators
	leftVal := leftRow[leftColIdx]
	rightVal := rightRow[rightColIdx]

	return compareValues(leftVal, rightVal) == 0
}

func (je *JoinEngine) hashValue(key interface{}) int {
	switch v := key.(type) {
	case int64:
		// Multiply by large prime and ensure positive
		hash := uint64(v) * 2654435761
		return int(hash & 0x7FFFFFFF)
	case string:
		hash := uint64(0)
		for _, c := range v {
			hash = hash*31 + uint64(c)
		}
		return int(hash & 0x7FFFFFFF)
	default:
		return 0
	}
}

// Note: VectorizedEngine methods for join queries have been moved to pkg/query
// to avoid circular import dependencies.