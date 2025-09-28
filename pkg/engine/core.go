// Package engine provides the core database engine components.
// It defines the fundamental data structures for tables, columns, indexes, and queries.
package engine

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// DataType identifies the type of data stored in a column.
type DataType uint8

const (
	TypeInt32 DataType = iota
	TypeInt64
	TypeFloat32
	TypeFloat64
	TypeString
	TypeBool
	TypeTimestamp
	TypeDecimal
)

// Column represents a single column in columnar storage format.
type Column struct {
	Name     string
	Type     DataType
	Data     unsafe.Pointer
	Nulls    []bool
	Length   uint64
	Capacity uint64
	mu       sync.RWMutex
}

// Table represents a table with columnar storage and MVCC support.
type Table struct {
	Name      string
	Columns   []*Column
	RowCount  uint64
	Indexes   map[string]*Index
	Database  *Database  // Reference to parent database for WAL access

	// MVCC Support
	MVCCRows     map[uint64]*MVCCRow // Row ID -> MVCC Row
	NextRowID    uint64              // Next available row ID
	MVCCManager  *MVCCManager        // MVCC transaction manager

	mu        sync.RWMutex
}

// Index represents an index structure on a column.
type Index struct {
	Name       string
	Column     string
	Type       IndexType
	Data       unsafe.Pointer
	Sorted     bool
	mu         sync.RWMutex
}

// IndexType identifies the index implementation type.
type IndexType uint8

const (
	BTreeIndex IndexType = iota
	HashIndex
	BitmapIndex
	BloomFilter
)

// Database is the main database engine managing tables and connections.
type Database struct {
	Name              string
	Tables            sync.Map // map[string]*Table
	Connections       sync.Map // map[string]*Connection
	IndexManager      interface{} // *storage.IndexManager - interface to avoid circular import
	QueryCache        *QueryCache
	BufferPool        *BufferPool
	TransactionMgr    *TransactionManager
	MVCCManager       *MVCCManager // MVCC transaction manager
	Stats             *Statistics
	WAL               interface{} // *storage.WAL
	DiskStorage       interface{} // *storage.DiskStorage
	CheckpointManager interface{} // *storage.CheckpointManager
}

// QueryPlan represents a parsed and optimized query execution plan.
type QueryPlan struct {
	Type       QueryType
	TableName  string
	Columns    []string
	Filters    []*FilterExpression
	Joins      []*JoinExpression
	OrderBy    []*OrderExpression
	GroupBy    []string
	Having     []*FilterExpression
	Limit      int64
	Offset     int64
	Optimized  bool
}

// QueryType identifies the type of SQL query.
type QueryType uint8

const (
	QuerySelect QueryType = iota
	QueryInsert
	QueryUpdate
	QueryDelete
	QueryCreateTable
	QueryCreateIndex
)

type FilterExpression struct {
	Column   string
	Operator FilterOperator
	Value    interface{}
	Children []*FilterExpression
}

type FilterOperator uint8

const (
	OpEqual FilterOperator = iota
	OpNotEqual
	OpLess
	OpLessEqual
	OpGreater
	OpGreaterEqual
	OpLike
	OpIn
	OpBetween
	OpAnd
	OpOr
	OpNot
)

type JoinExpression struct {
	Type      JoinType
	Table     string
	LeftCol   string
	RightCol  string
	Condition *FilterExpression
}

type JoinType uint8

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
	FullJoin
)

type OrderExpression struct {
	Column string
	Desc   bool
}

// Query cache for performance
type QueryCache struct {
	cache  sync.Map // map[string]*CachedQuery
	engine interface{} // *query.QueryCacheEngine - interface to avoid circular import
	stats struct {
		hits   uint64
		misses uint64
	}
}

type CachedQuery struct {
	Plan      *QueryPlan
	Result    *QueryResult
	Timestamp time.Time
	HitCount  uint64
}

// Buffer pool for memory management
type BufferPool struct {
	pages    chan []byte
	pageSize int
	maxPages int
}

// Transaction management
type TransactionManager struct {
	transactions sync.Map // map[string]*Transaction
	isolation    IsolationLevel
}

type IsolationLevel uint8

const (
	ReadUncommitted IsolationLevel = iota
	ReadCommitted
	RepeatableRead
	Serializable
)

type Transaction struct {
	ID        string
	StartTime time.Time
	State     TransactionState
	ReadSet   map[string]uint64
	WriteSet  map[string]*WriteOp
	mu        sync.Mutex
}

type TransactionState uint8

const (
	TxActive TransactionState = iota
	TxCommitted
	TxAborted
)

type WriteOp struct {
	Table string
	Row   uint64
	Type  WriteOpType
	Data  interface{}
}

type WriteOpType uint8

const (
	WriteInsert WriteOpType = iota
	WriteUpdate
	WriteDelete
)

// Statistics collection
type Statistics struct {
	QueriesExecuted uint64
	RowsScanned     uint64
	RowsReturned    uint64
	IndexHits       uint64
	CacheHits       uint64
	DiskReads       uint64
	DiskWrites      uint64
	mu              sync.RWMutex
}

// Query result
type QueryResult struct {
	Columns []string
	Types   []DataType
	Rows    [][]interface{}
	Error   error
	Stats   QueryStats
}

type QueryStats struct {
	ExecutionTime time.Duration
	RowsAffected  int64
	IndexScans    int
	SeqScans      int
}

// Connection represents a client connection
type Connection struct {
	ID          string
	RemoteAddr  string
	Database    string
	Transaction *Transaction
	MVCCTxn     *MVCCTransaction // MVCC transaction for this connection
	LastActivity time.Time
}

// NewDatabase creates a new database instance
func NewDatabase(name string) *Database {
	return &Database{
		Name:           name,
		IndexManager:   nil, // Set by storage package to avoid circular import
		QueryCache:     NewQueryCache(1000),
		BufferPool:     NewBufferPool(4096, 10000),
		TransactionMgr: NewTransactionManager(),
		MVCCManager:    NewMVCCManager(),
		Stats:          &Statistics{},
	}
}

func NewQueryCache(maxSize int) *QueryCache {
	return &QueryCache{}
}

func NewBufferPool(pageSize, maxPages int) *BufferPool {
	pool := &BufferPool{
		pages:    make(chan []byte, maxPages),
		pageSize: pageSize,
		maxPages: maxPages,
	}

	// Pre-allocate some pages
	for i := 0; i < maxPages/2; i++ {
		pool.pages <- make([]byte, pageSize)
	}

	return pool
}

func NewTransactionManager() *TransactionManager {
	return &TransactionManager{
		isolation: ReadCommitted,
	}
}

func (bp *BufferPool) GetPage() []byte {
	select {
	case page := <-bp.pages:
		return page
	default:
		return make([]byte, bp.pageSize)
	}
}

func (bp *BufferPool) PutPage(page []byte) {
	if len(page) != bp.pageSize {
		return
	}

	select {
	case bp.pages <- page:
	default:
		// Pool is full, let GC handle it
	}
}

// Column operations
func NewColumn(name string, dataType DataType, capacity uint64) *Column {
	var data unsafe.Pointer

	switch dataType {
	case TypeInt32:
		slice := make([]int32, 0, capacity)
		data = unsafe.Pointer(&slice)
	case TypeInt64:
		slice := make([]int64, 0, capacity)
		data = unsafe.Pointer(&slice)
	case TypeFloat64:
		slice := make([]float64, 0, capacity)
		data = unsafe.Pointer(&slice)
	case TypeString:
		slice := make([]string, 0, capacity)
		data = unsafe.Pointer(&slice)
	case TypeBool:
		slice := make([]bool, 0, capacity)
		data = unsafe.Pointer(&slice)
	default:
		slice := make([]interface{}, 0, capacity)
		data = unsafe.Pointer(&slice)
	}

	return &Column{
		Name:     name,
		Type:     dataType,
		Data:     data,
		Nulls:    make([]bool, 0, capacity),
		Capacity: capacity,
	}
}

func (c *Column) AppendInt64(value int64, isNull bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.Type != TypeInt64 {
		return
	}

	slice := (*[]int64)(c.Data)
	*slice = append(*slice, value)
	c.Nulls = append(c.Nulls, isNull)
	atomic.AddUint64(&c.Length, 1)
}

func (c *Column) AppendString(value string, isNull bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.Type != TypeString {
		return
	}

	slice := (*[]string)(c.Data)
	*slice = append(*slice, value)
	c.Nulls = append(c.Nulls, isNull)
	atomic.AddUint64(&c.Length, 1)
}

func (c *Column) GetInt64(index uint64) (int64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.Type != TypeInt64 || index >= c.Length {
		return 0, false
	}

	slice := (*[]int64)(c.Data)
	return (*slice)[index], !c.Nulls[index]
}

func (c *Column) GetString(index uint64) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.Type != TypeString || index >= c.Length {
		return "", false
	}

	slice := (*[]string)(c.Data)
	return (*slice)[index], !c.Nulls[index]
}

// Table operations
func NewTable(name string) *Table {
	return &Table{
		Name:     name,
		Columns:  make([]*Column, 0),
		Indexes:  make(map[string]*Index),
		Database: nil, // Set when table is added to database
	}
}

// NewTableWithDB creates a new table with database reference
func NewTableWithDB(name string, db *Database) *Table {
	var mvccMgr *MVCCManager
	if db != nil {
		mvccMgr = db.MVCCManager
	}

	return &Table{
		Name:        name,
		Columns:     make([]*Column, 0),
		Indexes:     make(map[string]*Index),
		Database:    db,
		MVCCRows:    make(map[uint64]*MVCCRow),
		NextRowID:   1,
		MVCCManager: mvccMgr,
	}
}

func (t *Table) AddColumn(column *Column) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.Columns = append(t.Columns, column)
}

func (t *Table) GetColumn(name string) *Column {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for _, col := range t.Columns {
		if col.Name == name {
			return col
		}
	}
	return nil
}

// InsertRow inserts a new row with MVCC support
func (t *Table) InsertRow(values map[string]interface{}) error {
	return t.InsertRowWithTransaction(nil, values)
}

// InsertRowWithTransaction inserts a new row within a transaction
func (t *Table) InsertRowWithTransaction(txn *MVCCTransaction, values map[string]interface{}) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Write WAL record before applying changes
	if t.Database != nil && t.Database.WAL != nil {
		if err := t.writeInsertWAL(values); err != nil {
			return fmt.Errorf("failed to write WAL record: %w", err)
		}
	}

	// Get next row ID
	rowID := atomic.AddUint64(&t.NextRowID, 1)

	// For non-MVCC mode (backward compatibility)
	if t.MVCCManager == nil {
		return t.insertRowLegacy(values)
	}

	// MVCC mode: Create new MVCC row
	mvccRow := &MVCCRow{
		RowID: rowID,
	}

	// Use provided transaction or create a default one
	var currentTxn *MVCCTransaction
	if txn != nil {
		currentTxn = txn
	} else {
		// Auto-commit transaction for backward compatibility
		currentTxn = t.MVCCManager.BeginTransaction()
		defer func() {
			if err := t.MVCCManager.CommitTransaction(currentTxn); err != nil {
				t.MVCCManager.AbortTransaction(currentTxn)
			}
		}()
	}

	// Create row version
	if err := t.MVCCManager.WriteRow(currentTxn, mvccRow, values); err != nil {
		return fmt.Errorf("failed to write MVCC row: %w", err)
	}

	// Store in table
	t.MVCCRows[rowID] = mvccRow

	// Also update columnar storage for compatibility
	t.insertRowLegacy(values)

	atomic.AddUint64(&t.RowCount, 1)
	return nil
}

// insertRowLegacy handles the original columnar storage format
func (t *Table) insertRowLegacy(values map[string]interface{}) error {
	for _, col := range t.Columns {
		value, exists := values[col.Name]
		isNull := !exists || value == nil

		switch col.Type {
		case TypeInt64:
			if !isNull {
				if v, ok := value.(int64); ok {
					col.AppendInt64(v, false)
				} else {
					col.AppendInt64(0, true)
				}
			} else {
				col.AppendInt64(0, true)
			}
		case TypeString:
			if !isNull {
				if v, ok := value.(string); ok {
					col.AppendString(v, false)
				} else {
					col.AppendString("", true)
				}
			} else {
				col.AppendString("", true)
			}
		}
	}
	return nil
}

// writeInsertWAL writes an INSERT operation to WAL
func (t *Table) writeInsertWAL(values map[string]interface{}) error {
	// Convert values to JSON for storage
	data, err := json.Marshal(values)
	if err != nil {
		return fmt.Errorf("failed to serialize row data: %w", err)
	}

	// Use reflection to call WriteRecord on the storage.WAL interface
	walInterface := t.Database.WAL
	if walInterface == nil {
		return nil // WAL not initialized
	}

	// Create storage.WALRecord using reflection to avoid circular imports
	record := map[string]interface{}{
		"Type":      uint8(1), // WALRecordInsert = 1 (from storage package)
		"TableName": t.Name,
		"Data":      data,
	}

	// Call WriteRecord via reflection
	return t.callWALWriteRecord(walInterface, record)
}

// callWALWriteRecord calls WriteRecord on WAL using reflection
func (t *Table) callWALWriteRecord(walInterface interface{}, record map[string]interface{}) error {
	// Get the WriteRecord method
	val := reflect.ValueOf(walInterface)
	method := val.MethodByName("WriteRecord")
	if !method.IsValid() {
		return fmt.Errorf("WriteRecord method not found on WAL interface")
	}

	// Create a WALRecord-like struct using reflection
	walRecordType := method.Type().In(0).Elem() // Get the type of the parameter (dereferenced)

	// Create new instance
	walRecord := reflect.New(walRecordType).Elem()

	// Set fields with proper type conversion
	for fieldName, value := range record {
		field := walRecord.FieldByName(fieldName)
		if field.IsValid() && field.CanSet() {
			if fieldName == "Type" {
				// For Type field, we need to convert to the proper enum type
				typeField := field.Type()
				typeValue := reflect.ValueOf(value).Convert(typeField)
				field.Set(typeValue)
			} else {
				field.Set(reflect.ValueOf(value))
			}
		}
	}

	// Call WriteRecord
	results := method.Call([]reflect.Value{walRecord.Addr()})

	// Check for error
	if len(results) > 0 && !results[0].IsNil() {
		return results[0].Interface().(error)
	}

	return nil
}

// WriteCreateTableWAL writes a CREATE TABLE operation to WAL
func (t *Table) WriteCreateTableWAL() error {
	if t.Database == nil || t.Database.WAL == nil {
		return nil // WAL not enabled
	}

	// Create table schema for WAL
	schema := TableSchema{
		Name:    t.Name,
		Columns: make([]ColumnDef, len(t.Columns)),
	}

	for i, col := range t.Columns {
		schema.Columns[i] = ColumnDef{
			Name: col.Name,
			Type: col.Type,
		}
	}

	// Serialize schema
	data, err := json.Marshal(schema)
	if err != nil {
		return fmt.Errorf("failed to serialize table schema: %w", err)
	}

	// Create storage.WALRecord using reflection
	record := map[string]interface{}{
		"Type":      uint8(4), // WALRecordCreateTable = 4 (from storage package)
		"TableName": t.Name,
		"Data":      data,
	}

	// Call WriteRecord via reflection
	return t.callWALWriteRecord(t.Database.WAL, record)
}

// WALWriter interface for writing WAL records
type WALWriter interface {
	WriteRecord(record *WALRecord) error
}

// WALRecord represents a WAL record (mirrored from storage package)
type WALRecord struct {
	LSN       uint64
	Type      uint8
	TableName string
	Data      []byte
	Timestamp time.Time
	CRC       uint32
}

// TableSchema represents table structure for WAL
type TableSchema struct {
	Name    string
	Columns []ColumnDef
}

// ColumnDef represents a column definition
type ColumnDef struct {
	Name string
	Type DataType
}

// UpdateRow updates existing rows that match the filter with MVCC support
func (t *Table) UpdateRow(filter *FilterExpression, updates map[string]interface{}) (int64, error) {
	return t.UpdateRowWithTransaction(nil, filter, updates)
}

// UpdateRowWithTransaction updates rows within a transaction
func (t *Table) UpdateRowWithTransaction(txn *MVCCTransaction, filter *FilterExpression, updates map[string]interface{}) (int64, error) {
	if t.MVCCManager == nil {
		return 0, fmt.Errorf("MVCC not enabled for this table")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Use provided transaction or create a default one
	var currentTxn *MVCCTransaction
	if txn != nil {
		currentTxn = txn
	} else {
		// Auto-commit transaction
		currentTxn = t.MVCCManager.BeginTransaction()
		defer func() {
			if err := t.MVCCManager.CommitTransaction(currentTxn); err != nil {
				t.MVCCManager.AbortTransaction(currentTxn)
			}
		}()
	}

	updatedCount := int64(0)

	// Iterate through all MVCC rows
	for _, mvccRow := range t.MVCCRows {
		// Read current version
		currentData, exists := t.MVCCManager.ReadRow(currentTxn, mvccRow)
		if !exists {
			continue // Row not visible or deleted
		}

		// Check if row matches filter
		if filter != nil && !t.evaluateFilter(currentData, filter) {
			continue
		}

		// Write WAL record for UPDATE
		if t.Database != nil && t.Database.WAL != nil {
			updateData := map[string]interface{}{
				"row_id":  mvccRow.RowID,
				"old_data": currentData,
				"new_data": updates,
			}
			if err := t.writeUpdateWAL(updateData); err != nil {
				return 0, fmt.Errorf("failed to write UPDATE WAL record: %w", err)
			}
		}

		// Create new data by merging updates
		newData := make(map[string]interface{})
		for k, v := range currentData {
			newData[k] = v
		}
		for k, v := range updates {
			newData[k] = v
		}

		// Create new version
		if err := t.MVCCManager.WriteRow(currentTxn, mvccRow, newData); err != nil {
			return 0, fmt.Errorf("failed to create new version: %w", err)
		}

		updatedCount++
	}

	return updatedCount, nil
}

// DeleteRow deletes rows that match the filter with MVCC support
func (t *Table) DeleteRow(filter *FilterExpression) (int64, error) {
	return t.DeleteRowWithTransaction(nil, filter)
}

// DeleteRowWithTransaction deletes rows within a transaction
func (t *Table) DeleteRowWithTransaction(txn *MVCCTransaction, filter *FilterExpression) (int64, error) {
	if t.MVCCManager == nil {
		return 0, fmt.Errorf("MVCC not enabled for this table")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Use provided transaction or create a default one
	var currentTxn *MVCCTransaction
	if txn != nil {
		currentTxn = txn
	} else {
		// Auto-commit transaction
		currentTxn = t.MVCCManager.BeginTransaction()
		defer func() {
			if err := t.MVCCManager.CommitTransaction(currentTxn); err != nil {
				t.MVCCManager.AbortTransaction(currentTxn)
			}
		}()
	}

	deletedCount := int64(0)

	// Iterate through all MVCC rows
	for _, mvccRow := range t.MVCCRows {
		// Read current version
		currentData, exists := t.MVCCManager.ReadRow(currentTxn, mvccRow)
		if !exists {
			continue // Row not visible or already deleted
		}

		// Check if row matches filter
		if filter != nil && !t.evaluateFilter(currentData, filter) {
			continue
		}

		// Write WAL record for DELETE
		if t.Database != nil && t.Database.WAL != nil {
			deleteData := map[string]interface{}{
				"row_id": mvccRow.RowID,
				"data":   currentData,
			}
			if err := t.writeDeleteWAL(deleteData); err != nil {
				return 0, fmt.Errorf("failed to write DELETE WAL record: %w", err)
			}
		}

		// Mark row as deleted
		if err := t.MVCCManager.DeleteRow(currentTxn, mvccRow); err != nil {
			return 0, fmt.Errorf("failed to delete row: %w", err)
		}

		deletedCount++
	}

	return deletedCount, nil
}

// evaluateFilter checks if a row matches a filter condition
func (t *Table) evaluateFilter(data map[string]interface{}, filter *FilterExpression) bool {
	value, exists := data[filter.Column]
	if !exists {
		return false
	}

	switch filter.Operator {
	case OpEqual:
		return value == filter.Value
	case OpNotEqual:
		return value != filter.Value
	case OpLess:
		if v1, ok := value.(int64); ok {
			if v2, ok := filter.Value.(int64); ok {
				return v1 < v2
			}
		}
	case OpLessEqual:
		if v1, ok := value.(int64); ok {
			if v2, ok := filter.Value.(int64); ok {
				return v1 <= v2
			}
		}
	case OpGreater:
		if v1, ok := value.(int64); ok {
			if v2, ok := filter.Value.(int64); ok {
				return v1 > v2
			}
		}
	case OpGreaterEqual:
		if v1, ok := value.(int64); ok {
			if v2, ok := filter.Value.(int64); ok {
				return v1 >= v2
			}
		}
	}
	return false
}

// writeUpdateWAL writes an UPDATE operation to WAL
func (t *Table) writeUpdateWAL(updateData map[string]interface{}) error {
	data, err := json.Marshal(updateData)
	if err != nil {
		return fmt.Errorf("failed to serialize update data: %w", err)
	}

	record := map[string]interface{}{
		"Type":      uint8(1), // WALRecordUpdate = 1
		"TableName": t.Name,
		"Data":      data,
	}

	return t.callWALWriteRecord(t.Database.WAL, record)
}

// writeDeleteWAL writes a DELETE operation to WAL
func (t *Table) writeDeleteWAL(deleteData map[string]interface{}) error {
	data, err := json.Marshal(deleteData)
	if err != nil {
		return fmt.Errorf("failed to serialize delete data: %w", err)
	}

	record := map[string]interface{}{
		"Type":      uint8(2), // WALRecordDelete = 2
		"TableName": t.Name,
		"Data":      data,
	}

	return t.callWALWriteRecord(t.Database.WAL, record)
}

// Transaction Control Methods

// BeginTransaction starts a new MVCC transaction for a connection
func (db *Database) BeginTransaction(conn *Connection) error {
	if conn.MVCCTxn != nil && conn.MVCCTxn.State == TxActive {
		return fmt.Errorf("transaction already active")
	}

	if db.MVCCManager == nil {
		return fmt.Errorf("MVCC not enabled")
	}

	conn.MVCCTxn = db.MVCCManager.BeginTransaction()
	return nil
}

// CommitTransaction commits the current transaction for a connection
func (db *Database) CommitTransaction(conn *Connection) error {
	if conn.MVCCTxn == nil || conn.MVCCTxn.State != TxActive {
		return fmt.Errorf("no active transaction")
	}

	if db.MVCCManager == nil {
		return fmt.Errorf("MVCC not enabled")
	}

	err := db.MVCCManager.CommitTransaction(conn.MVCCTxn)
	conn.MVCCTxn = nil
	return err
}

// RollbackTransaction aborts the current transaction for a connection
func (db *Database) RollbackTransaction(conn *Connection) error {
	if conn.MVCCTxn == nil || conn.MVCCTxn.State != TxActive {
		return fmt.Errorf("no active transaction")
	}

	if db.MVCCManager == nil {
		return fmt.Errorf("MVCC not enabled")
	}

	err := db.MVCCManager.AbortTransaction(conn.MVCCTxn)
	conn.MVCCTxn = nil
	return err
}

// GetActiveTransaction returns the active transaction for a connection
func (db *Database) GetActiveTransaction(conn *Connection) *MVCCTransaction {
	return conn.MVCCTxn
}

// HasActiveTransaction checks if a connection has an active transaction
func (db *Database) HasActiveTransaction(conn *Connection) bool {
	return conn.MVCCTxn != nil && conn.MVCCTxn.State == TxActive
}

// HasAggregates checks if query plan contains aggregate functions
func (qp *QueryPlan) HasAggregates() bool {
	for _, col := range qp.Columns {
		if col == "COUNT(*)" ||
		   col == "SUM(age)" || col == "AVG(age)" ||
		   col == "MIN(age)" || col == "MAX(age)" {
			return true
		}
	}
	return false
}