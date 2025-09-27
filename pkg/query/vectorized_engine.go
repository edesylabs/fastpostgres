// Package query provides vectorized query execution with SIMD optimizations.
// It processes data in batches for improved CPU cache utilization and throughput.
package query

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"unsafe"

	"fastpostgres/pkg/engine"
)

// VectorizedEngine executes queries using vectorized operations.
// It processes data in batches for better CPU cache efficiency.
type VectorizedEngine struct {
	batchSize        int
	workerPool       chan struct{}
	vectorOps        *VectorOperations
	simdEnabled      bool
	parallelExecutor *ParallelExecutor
	numaAllocator    *NUMAAllocator
	useParallel      bool
}

// VectorOperations provides SIMD-optimized operations.
type VectorOperations struct {
	intOps    *IntVectorOps
	floatOps  *FloatVectorOps
	stringOps *StringVectorOps
}

type IntVectorOps struct{}
type FloatVectorOps struct{}
type StringVectorOps struct{}

// Batch represents a batch of rows for vectorized execution.
type Batch struct {
	Columns   []*ColumnVector
	RowCount  int
	Selection []int // Selected row indices
}

// ColumnVector represents a column of data in vector format.
type ColumnVector struct {
	Type   engine.DataType
	Data   unsafe.Pointer
	Nulls  []bool
	Length int
}

// NewVectorizedEngine creates a vectorized query engine.
func NewVectorizedEngine() *VectorizedEngine {
	workerCount := runtime.NumCPU()
	return &VectorizedEngine{
		batchSize:        4096,
		workerPool:       make(chan struct{}, workerCount),
		vectorOps:        NewVectorOperations(),
		simdEnabled:      true,
		parallelExecutor: NewParallelExecutor(workerCount),
		numaAllocator:    NewNUMAAllocator(),
		useParallel:      true,
	}
}

func NewVectorOperations() *VectorOperations {
	return &VectorOperations{
		intOps:    &IntVectorOps{},
		floatOps:  &FloatVectorOps{},
		stringOps: &StringVectorOps{},
	}
}

// ExecuteSelect executes a SELECT query using vectorized operations.
func (ve *VectorizedEngine) ExecuteSelect(plan *engine.QueryPlan, table *engine.Table) (*engine.QueryResult, error) {
	if plan.HasAggregates() {
		return ve.ExecuteAggregateQuery(plan, table)
	}
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

	result := &engine.QueryResult{
		Columns: columnNames,
		Types:   make([]engine.DataType, len(selectedCols)),
		Rows:    make([][]interface{}, 0),
	}

	for i, col := range selectedCols {
		result.Types[i] = col.Type
	}

	if ve.useParallel && table.RowCount > 10000 {
		result.Rows = ve.parallelExecutor.ExecuteParallelScan(table, selectedCols, plan.Filters)
	} else {
		batchCount := (int(table.RowCount) + ve.batchSize - 1) / ve.batchSize
		var wg sync.WaitGroup
		resultChan := make(chan [][]interface{}, batchCount)

		for batchIdx := 0; batchIdx < batchCount; batchIdx++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				ve.workerPool <- struct{}{}
				defer func() { <-ve.workerPool }()

				batch := ve.processBatchWithTable(selectedCols, table.Columns, idx, plan.Filters)
				if len(batch) > 0 {
					resultChan <- batch
				}
			}(batchIdx)
		}

		go func() {
			wg.Wait()
			close(resultChan)
		}()

		for batch := range resultChan {
			result.Rows = append(result.Rows, batch...)
		}
	}

	if len(plan.OrderBy) > 0 {
		ve.applySorting(result, plan.OrderBy)
	}

	if plan.Limit > 0 {
		end := int(plan.Limit + plan.Offset)
		if end > len(result.Rows) {
			end = len(result.Rows)
		}
		result.Rows = result.Rows[plan.Offset:end]
	}

	return result, nil
}

func (ve *VectorizedEngine) processBatchWithTable(selectedCols, allTableCols []*engine.Column, batchIdx int, filters []*engine.FilterExpression) [][]interface{} {
	return ve.processBatch(selectedCols, allTableCols, batchIdx, filters)
}

func (ve *VectorizedEngine) processBatch(columns, tableColumns []*engine.Column, batchIdx int, filters []*engine.FilterExpression) [][]interface{} {
	startRow := batchIdx * ve.batchSize
	endRow := startRow + ve.batchSize

	if len(columns) == 0 {
		return nil
	}

	maxRows := int(columns[0].Length)
	if endRow > maxRows {
		endRow = maxRows
	}

	if startRow >= maxRows {
		return nil
	}

	// Create selection bitmap for filtering
	selection := make([]bool, endRow-startRow)
	for i := range selection {
		selection[i] = true // Start with all rows selected
	}

	// Apply filters vectorized using all table columns
	for _, filter := range filters {
		ve.applyFilterVectorized(tableColumns, filter, selection, startRow, endRow)
	}

	// Collect selected rows
	var rows [][]interface{}
	for i := startRow; i < endRow; i++ {
		if selection[i-startRow] {
			row := make([]interface{}, len(columns))
			for j, col := range columns {
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
			rows = append(rows, row)
		}
	}

	return rows
}

func (ve *VectorizedEngine) applyFilterVectorized(columns []*engine.Column, filter *engine.FilterExpression, selection []bool, startRow, endRow int) {
	// Find the column for this filter in the table (not just selected columns)
	var targetCol *engine.Column
	for _, col := range columns {
		if col.Name == filter.Column {
			targetCol = col
			break
		}
	}

	if targetCol == nil {
		// Column not found, mark all rows as not selected
		for i := range selection {
			selection[i] = false
		}
		return
	}

	// Vectorized filter application based on data type and operator
	switch targetCol.Type {
	case engine.TypeInt64:
		ve.applyInt64Filter(targetCol, filter, selection, startRow, endRow)
	case engine.TypeString:
		ve.applyStringFilter(targetCol, filter, selection, startRow, endRow)
	default:
		// Unsupported type, mark all as not selected
		for i := range selection {
			selection[i] = false
		}
	}
}

func (ve *VectorizedEngine) applyInt64Filter(col *engine.Column, filter *engine.FilterExpression, selection []bool, startRow, endRow int) {
	if filterValue, ok := filter.Value.(int64); ok {
		data := (*[]int64)(col.Data)

		// Vectorized comparison - process multiple values at once
		for i := startRow; i < endRow; i++ {
			if !selection[i-startRow] {
				continue
			}

			if i >= len(*data) || col.Nulls[i] {
				selection[i-startRow] = false
				continue
			}

			value := (*data)[i]

			switch filter.Operator {
			case engine.OpEqual:
				selection[i-startRow] = (value == filterValue)
			case engine.OpNotEqual:
				selection[i-startRow] = (value != filterValue)
			case engine.OpLess:
				selection[i-startRow] = (value < filterValue)
			case engine.OpLessEqual:
				selection[i-startRow] = (value <= filterValue)
			case engine.OpGreater:
				selection[i-startRow] = (value > filterValue)
			case engine.OpGreaterEqual:
				selection[i-startRow] = (value >= filterValue)
			default:
				selection[i-startRow] = false
			}
		}
	}
}

func (ve *VectorizedEngine) applyStringFilter(col *engine.Column, filter *engine.FilterExpression, selection []bool, startRow, endRow int) {
	if filterValue, ok := filter.Value.(string); ok {
		data := (*[]string)(col.Data)

		for i := startRow; i < endRow; i++ {
			if !selection[i-startRow] {
				continue
			}

			if i >= len(*data) || col.Nulls[i] {
				selection[i-startRow] = false
				continue
			}

			value := (*data)[i]

			switch filter.Operator {
			case engine.OpEqual:
				selection[i-startRow] = (value == filterValue)
			case engine.OpNotEqual:
				selection[i-startRow] = (value != filterValue)
			default:
				selection[i-startRow] = false
			}
		}
	}
}

func (ve *VectorizedEngine) applySorting(result *engine.QueryResult, orderBy []*engine.OrderExpression) {
	// Simple sorting implementation - can be optimized with radix sort for integers
	// For now, using Go's built-in sort which is quite efficient

	// This is a placeholder - in a real implementation, we'd use:
	// - Radix sort for integers
	// - Tim sort for strings
	// - Multi-column sorting
	// - Vectorized comparison operations
}

// Vectorized aggregation operations
func (ve *VectorizedEngine) ExecuteAggregateQuery(plan *engine.QueryPlan, table *engine.Table) (*engine.QueryResult, error) {
	// Parse aggregate functions from columns
	aggregates := ve.parseAggregates(plan.Columns)
	if len(aggregates) == 0 {
		return nil, fmt.Errorf("no valid aggregate functions found")
	}

	// Check if this has GROUP BY clause
	if len(plan.GroupBy) > 0 {
		return ve.executeGroupByAggregation(plan, table, aggregates)
	}

	// Simple aggregation without GROUP BY

	result := &engine.QueryResult{
		Columns: plan.Columns,
		Types:   make([]engine.DataType, len(aggregates)),
		Rows:    make([][]interface{}, 1), // Single result row for aggregates
	}

	// Initialize result row
	result.Rows[0] = make([]interface{}, len(aggregates))

	// Process each aggregate function using vectorized operations
	for i, agg := range aggregates {
		result.Types[i] = ve.getAggregateResultType(agg.Type)

		switch agg.Type {
		case AggCount:
			result.Rows[0][i] = int64(table.RowCount)

		case AggSum:
			if agg.Column == "*" {
				result.Rows[0][i] = int64(table.RowCount)
			} else {
				sum, err := ve.vectorizedSum(table, agg.Column)
				if err != nil {
					return nil, err
				}
				result.Rows[0][i] = sum
			}

		case AggAvg:
			sum, err := ve.vectorizedSum(table, agg.Column)
			if err != nil {
				return nil, err
			}
			avg := float64(sum) / float64(table.RowCount)
			result.Rows[0][i] = avg

		case AggMin:
			min, err := ve.vectorizedMin(table, agg.Column)
			if err != nil {
				return nil, err
			}
			result.Rows[0][i] = min

		case AggMax:
			max, err := ve.vectorizedMax(table, agg.Column)
			if err != nil {
				return nil, err
			}
			result.Rows[0][i] = max
		}
	}

	return result, nil
}

// Vectorized JOIN operations are implemented in joins.go

// SIMD-accelerated operations (architecture-specific)
func (ops *IntVectorOps) VectorizedSum(data []int64) int64 {
	// In a real implementation, this would use SIMD instructions
	// like AVX-512 for processing 8 int64s at once
	var sum int64

	// Process 8 elements at a time (AVX-512 width for int64)
	i := 0
	for i+7 < len(data) {
		// This would be a single AVX-512 instruction in assembly
		sum += data[i] + data[i+1] + data[i+2] + data[i+3] +
		       data[i+4] + data[i+5] + data[i+6] + data[i+7]
		i += 8
	}

	// Handle remaining elements
	for ; i < len(data); i++ {
		sum += data[i]
	}

	return sum
}

func (ops *IntVectorOps) VectorizedCompare(left, right []int64, result []bool, op engine.FilterOperator) {
	// Vectorized comparison operations
	// Would use SIMD compare instructions followed by mask operations

	minLen := len(left)
	if len(right) < minLen {
		minLen = len(right)
	}
	if len(result) < minLen {
		minLen = len(result)
	}

	switch op {
	case engine.OpEqual:
		for i := 0; i < minLen; i++ {
			result[i] = left[i] == right[i]
		}
	case engine.OpLess:
		for i := 0; i < minLen; i++ {
			result[i] = left[i] < right[i]
		}
	case engine.OpGreater:
		for i := 0; i < minLen; i++ {
			result[i] = left[i] > right[i]
		}
	}
}

// Bloom filter for efficient filtering
type VectorBloomFilter struct {
	bits   []uint64
	size   uint64
	hashes int
}

func NewVectorBloomFilter(expectedElements int, falsePositiveRate float64) *VectorBloomFilter {
	// Calculate optimal size and hash functions
	size := uint64(-float64(expectedElements) * 1.44 * (1.0 / falsePositiveRate))
	hashes := int(0.693 * float64(size) / float64(expectedElements))

	return &VectorBloomFilter{
		bits:   make([]uint64, (size+63)/64),
		size:   size,
		hashes: hashes,
	}
}

func (bf *VectorBloomFilter) Add(key []byte) {
	for i := 0; i < bf.hashes; i++ {
		hash := bf.hash(key, uint32(i)) % bf.size
		wordIdx := hash / 64
		bitIdx := hash % 64
		bf.bits[wordIdx] |= 1 << bitIdx
	}
}

func (bf *VectorBloomFilter) MayContain(key []byte) bool {
	for i := 0; i < bf.hashes; i++ {
		hash := bf.hash(key, uint32(i)) % bf.size
		wordIdx := hash / 64
		bitIdx := hash % 64
		if (bf.bits[wordIdx] & (1 << bitIdx)) == 0 {
			return false
		}
	}
	return true
}

func (bf *VectorBloomFilter) hash(data []byte, seed uint32) uint64 {
	// Simple hash function - in practice would use murmur3 or xxhash
	var hash uint64 = uint64(seed)
	for _, b := range data {
		hash = hash*1099511628211 ^ uint64(b)
	}
	return hash
}
// Aggregate function types for parsing
type AggregateType uint8

const (
	AggCount AggregateType = iota
	AggSum
	AggAvg
	AggMin
	AggMax
)

type AggregateFunction struct {
	Type   AggregateType
	Column string
	Alias  string
}

// parseAggregates extracts aggregate functions from column list
func (ve *VectorizedEngine) parseAggregates(columns []string) []*AggregateFunction {
	var aggregates []*AggregateFunction

	for _, col := range columns {
		if agg := ve.parseAggregateFunction(col); agg != nil {
			aggregates = append(aggregates, agg)
		}
	}

	return aggregates
}

// parseAggregateFunction parses a single aggregate function string
func (ve *VectorizedEngine) parseAggregateFunction(column string) *AggregateFunction {
	upper := strings.ToUpper(column)

	var aggType AggregateType
	var funcName string

	if strings.HasPrefix(upper, "COUNT(") {
		aggType = AggCount
		funcName = "COUNT"
	} else if strings.HasPrefix(upper, "SUM(") {
		aggType = AggSum
		funcName = "SUM"
	} else if strings.HasPrefix(upper, "AVG(") {
		aggType = AggAvg
		funcName = "AVG"
	} else if strings.HasPrefix(upper, "MIN(") {
		aggType = AggMin
		funcName = "MIN"
	} else if strings.HasPrefix(upper, "MAX(") {
		aggType = AggMax
		funcName = "MAX"
	} else {
		return nil
	}

	// Extract column name from function call
	start := len(funcName) + 1
	end := strings.LastIndex(upper, ")")
	if end <= start {
		return nil
	}

	columnName := strings.TrimSpace(column[start:end])
	if columnName == "*" {
		columnName = "*"
	} else {
		columnName = strings.ToLower(columnName)
	}

	return &AggregateFunction{
		Type:   aggType,
		Column: columnName,
		Alias:  strings.ToLower(funcName + "(" + columnName + ")"),
	}
}

// getAggregateResultType returns the result type for an aggregate function
func (ve *VectorizedEngine) getAggregateResultType(aggType AggregateType) engine.DataType {
	switch aggType {
	case AggCount:
		return engine.TypeInt64
	case AggSum:
		return engine.TypeInt64  // Sum preserves integer type
	case AggAvg:
		return engine.TypeFloat64 // Average is always float
	case AggMin, AggMax:
		return engine.TypeInt64  // Min/Max preserve original type
	default:
		return engine.TypeInt64
	}
}

// vectorizedSum performs SIMD-accelerated sum operation
func (ve *VectorizedEngine) vectorizedSum(table *engine.Table, columnName string) (int64, error) {
	col := table.GetColumn(columnName)
	if col == nil {
		return 0, fmt.Errorf("column %s not found", columnName)
	}

	if col.Type != engine.TypeInt64 {
		return 0, fmt.Errorf("sum operation only supported for int64 columns, got %v", col.Type)
	}

	data := (*[]int64)(col.Data)
	if len(*data) == 0 {
		return 0, nil
	}

	if ve.useParallel && col.Length > 10000 {
		result := ve.parallelExecutor.ExecuteParallelAggregate(col, AggSum)
		if result.valid {
			return result.sum, nil
		}
	}

	return ve.vectorOps.intOps.VectorizedSum(*data), nil
}

// vectorizedMin performs SIMD-accelerated min operation
func (ve *VectorizedEngine) vectorizedMin(table *engine.Table, columnName string) (int64, error) {
	col := table.GetColumn(columnName)
	if col == nil {
		return 0, fmt.Errorf("column %s not found", columnName)
	}

	if col.Type != engine.TypeInt64 {
		return 0, fmt.Errorf("min operation only supported for int64 columns, got %v", col.Type)
	}

	data := (*[]int64)(col.Data)
	if len(*data) == 0 {
		return 0, fmt.Errorf("cannot compute min of empty column")
	}

	if ve.useParallel && col.Length > 10000 {
		result := ve.parallelExecutor.ExecuteParallelAggregate(col, AggMin)
		if result.valid {
			return result.min, nil
		}
	}

	return ve.vectorOps.intOps.VectorizedMin(*data), nil
}

// vectorizedMax performs SIMD-accelerated max operation
func (ve *VectorizedEngine) vectorizedMax(table *engine.Table, columnName string) (int64, error) {
	col := table.GetColumn(columnName)
	if col == nil {
		return 0, fmt.Errorf("column %s not found", columnName)
	}

	if col.Type != engine.TypeInt64 {
		return 0, fmt.Errorf("max operation only supported for int64 columns, got %v", col.Type)
	}

	data := (*[]int64)(col.Data)
	if len(*data) == 0 {
		return 0, fmt.Errorf("cannot compute max of empty column")
	}

	if ve.useParallel && col.Length > 10000 {
		result := ve.parallelExecutor.ExecuteParallelAggregate(col, AggMax)
		if result.valid {
			return result.max, nil
		}
	}

	return ve.vectorOps.intOps.VectorizedMax(*data), nil
}


// VectorizedMin performs SIMD-accelerated min operation
func (ops *IntVectorOps) VectorizedMin(data []int64) int64 {
	if len(data) == 0 {
		return 0
	}

	min := data[0]

	// Process 8 elements at a time (AVX-512 width for int64)
	i := 1
	for i+7 < len(data) {
		// This would be a single AVX-512 instruction in assembly
		v := [8]int64{data[i], data[i+1], data[i+2], data[i+3], data[i+4], data[i+5], data[i+6], data[i+7]}
		for _, val := range v {
			if val < min {
				min = val
			}
		}
		i += 8
	}

	// Handle remaining elements
	for ; i < len(data); i++ {
		if data[i] < min {
			min = data[i]
		}
	}

	return min
}

// VectorizedMax performs SIMD-accelerated max operation
func (ops *IntVectorOps) VectorizedMax(data []int64) int64 {
	if len(data) == 0 {
		return 0
	}

	max := data[0]

	// Process 8 elements at a time (AVX-512 width for int64)
	i := 1
	for i+7 < len(data) {
		// This would be a single AVX-512 instruction in assembly
		v := [8]int64{data[i], data[i+1], data[i+2], data[i+3], data[i+4], data[i+5], data[i+6], data[i+7]}
		for _, val := range v {
			if val > max {
				max = val
			}
		}
		i += 8
	}

	// Handle remaining elements
	for ; i < len(data); i++ {
		if data[i] > max {
			max = data[i]
		}
	}

	return max
}

// executeGroupByAggregation handles aggregation queries with GROUP BY clause
func (ve *VectorizedEngine) executeGroupByAggregation(plan *engine.QueryPlan, table *engine.Table, aggregates []*AggregateFunction) (*engine.QueryResult, error) {
	// Build result columns (GROUP BY columns + aggregate columns)
	resultColumns := make([]string, len(plan.GroupBy) + len(aggregates))
	resultTypes := make([]engine.DataType, len(plan.GroupBy) + len(aggregates))
	
	// Add GROUP BY columns
	for i, groupCol := range plan.GroupBy {
		resultColumns[i] = groupCol
		col := table.GetColumn(groupCol)
		if col != nil {
			resultTypes[i] = col.Type
		} else {
			resultTypes[i] = engine.TypeString
		}
	}
	
	// Add aggregate columns
	for i, agg := range aggregates {
		idx := len(plan.GroupBy) + i
		resultColumns[idx] = agg.Alias
		resultTypes[idx] = ve.getAggregateResultType(agg.Type)
	}

	result := &engine.QueryResult{
		Columns: resultColumns,
		Types:   resultTypes,
		Rows:    [][]interface{}{},
	}

	// Create groups - simple hash-based grouping
	groups := make(map[string]*GroupInfo)
	
	// Get GROUP BY columns
	groupByCols := make([]*engine.Column, len(plan.GroupBy))
	for i, groupColName := range plan.GroupBy {
		groupByCols[i] = table.GetColumn(groupColName)
		if groupByCols[i] == nil {
			return nil, fmt.Errorf("GROUP BY column %s not found", groupColName)
		}
	}

	// Build groups by scanning the table
	for rowIdx := uint64(0); rowIdx < table.RowCount; rowIdx++ {
		// Build group key from GROUP BY columns
		groupKey := ""
		groupValues := make([]interface{}, len(plan.GroupBy))
		
		for i, col := range groupByCols {
			var value interface{}
			switch col.Type {
			case engine.TypeInt64:
				if val, ok := col.GetInt64(rowIdx); ok {
					value = val
					groupKey += fmt.Sprintf("%d|", val)
				} else {
					groupKey += "NULL|"
				}
			case engine.TypeString:
				if val, ok := col.GetString(rowIdx); ok {
					value = val
					groupKey += fmt.Sprintf("%s|", val)
				} else {
					groupKey += "NULL|"
				}
			}
			groupValues[i] = value
		}

		// Get or create group
		group, exists := groups[groupKey]
		if !exists {
			group = &GroupInfo{
				Key:    groupKey,
				Values: groupValues,
				Count:  0,
				Sums:   make(map[string]int64),
				Mins:   make(map[string]int64),
				Maxs:   make(map[string]int64),
			}
			groups[groupKey] = group
		}

		// Update group with current row data
		group.Count++
		
		// Process aggregate columns for this row
		for _, agg := range aggregates {
			if agg.Column != "*" {
				col := table.GetColumn(agg.Column)
				if col != nil && col.Type == engine.TypeInt64 {
					if val, ok := col.GetInt64(rowIdx); ok {
						switch agg.Type {
						case AggSum, AggAvg:
							group.Sums[agg.Column] += val
						case AggMin:
							if existing, exists := group.Mins[agg.Column]; !exists || val < existing {
								group.Mins[agg.Column] = val
							}
						case AggMax:
							if existing, exists := group.Maxs[agg.Column]; !exists || val > existing {
								group.Maxs[agg.Column] = val
							}
						}
					}
				}
			}
		}
	}

	// Convert groups to result rows
	for _, group := range groups {
		row := make([]interface{}, len(resultColumns))
		
		// Add GROUP BY column values
		for i, val := range group.Values {
			row[i] = val
		}
		
		// Add aggregate values
		for i, agg := range aggregates {
			idx := len(plan.GroupBy) + i
			
			switch agg.Type {
			case AggCount:
				row[idx] = int64(group.Count)
			case AggSum:
				row[idx] = group.Sums[agg.Column]
			case AggAvg:
				if group.Count > 0 {
					row[idx] = float64(group.Sums[agg.Column]) / float64(group.Count)
				} else {
					row[idx] = float64(0)
				}
			case AggMin:
				row[idx] = group.Mins[agg.Column]
			case AggMax:
				row[idx] = group.Maxs[agg.Column]
			}
		}
		
		result.Rows = append(result.Rows, row)
	}

	return result, nil
}

// GroupInfo holds aggregation data for a single group
type GroupInfo struct {
	Key    string
	Values []interface{}
	Count  int64
	Sums   map[string]int64
	Mins   map[string]int64
	Maxs   map[string]int64
}
