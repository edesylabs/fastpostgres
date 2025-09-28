// Package engine provides MVCC (Multi-Version Concurrency Control) implementation.
// This enables snapshot isolation and concurrent transactions with ACID properties.
package engine

import (
	"sync"
	"sync/atomic"
	"time"
)

// TransactionID represents a unique transaction identifier
type TransactionID uint64

// GlobalTransactionCounter provides globally unique transaction IDs
var GlobalTransactionCounter uint64

// RowVersion represents a single version of a row in MVCC
type RowVersion struct {
	Data         map[string]interface{} // The actual row data
	CreatedByTxn TransactionID          // Transaction that created this version
	DeletedByTxn TransactionID          // Transaction that deleted this version (0 if not deleted)
	CreatedAt    time.Time              // When this version was created
	Next         *RowVersion            // Next version in the chain (older)
}

// MVCCRow represents a row with multiple versions
type MVCCRow struct {
	RowID       uint64       // Unique row identifier
	LatestVersion *RowVersion // Most recent version (head of version chain)
	mu          sync.RWMutex // Protects the version chain
}

// MVCCTransaction represents an active transaction with snapshot isolation
type MVCCTransaction struct {
	ID          TransactionID
	StartTime   time.Time
	SnapshotID  TransactionID // Snapshot of committed transactions at start time
	State       TransactionState
	ReadSet     map[string]uint64    // Tables/rows read (for conflict detection)
	WriteSet    map[string]*MVCCRow  // Rows written by this transaction
	Savepoints  []TransactionID      // For nested transactions/savepoints
	mu          sync.Mutex
}

// MVCCManager manages all MVCC operations and transactions
type MVCCManager struct {
	activeTransactions sync.Map // map[TransactionID]*MVCCTransaction
	commitLog         []TransactionID // Log of committed transactions (for snapshot isolation)
	commitLogMu       sync.RWMutex
	gcThreshold       time.Duration // How old versions to keep for GC
	gcTicker          *time.Ticker  // Garbage collector
	stopGC            chan struct{}
}

// Note: TransactionState is defined in core.go to avoid redeclaration
const (
	TxPrepared TransactionState = 3 // For 2PC (extends existing constants)
)

// NewMVCCManager creates a new MVCC manager
func NewMVCCManager() *MVCCManager {
	mgr := &MVCCManager{
		gcThreshold: 1 * time.Hour, // Keep versions for 1 hour
		stopGC:      make(chan struct{}),
	}

	// Start garbage collector (runs every 10 minutes)
	mgr.gcTicker = time.NewTicker(10 * time.Minute)
	go mgr.garbageCollector()

	return mgr
}

// BeginTransaction starts a new transaction with snapshot isolation
func (mgr *MVCCManager) BeginTransaction() *MVCCTransaction {
	txnID := atomic.AddUint64(&GlobalTransactionCounter, 1)

	// Create snapshot of currently committed transactions
	mgr.commitLogMu.RLock()
	snapshotID := TransactionID(0)
	if len(mgr.commitLog) > 0 {
		snapshotID = mgr.commitLog[len(mgr.commitLog)-1]
	}
	mgr.commitLogMu.RUnlock()

	txn := &MVCCTransaction{
		ID:         TransactionID(txnID),
		StartTime:  time.Now(),
		SnapshotID: snapshotID,
		State:      TxActive,
		ReadSet:    make(map[string]uint64),
		WriteSet:   make(map[string]*MVCCRow),
	}

	mgr.activeTransactions.Store(txnID, txn)
	return txn
}

// CommitTransaction commits a transaction and makes its changes visible
func (mgr *MVCCManager) CommitTransaction(txn *MVCCTransaction) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.State != TxActive {
		return ErrTransactionNotActive
	}

	// Mark transaction as committed
	txn.State = TxCommitted
	commitTime := time.Now()

	// Add to commit log for future snapshots
	mgr.commitLogMu.Lock()
	mgr.commitLog = append(mgr.commitLog, txn.ID)
	mgr.commitLogMu.Unlock()

	// Apply all writes atomically (they're already in the version chains)
	for _, row := range txn.WriteSet {
		row.mu.Lock()
		// Update commit timestamp for all versions created by this transaction
		for version := row.LatestVersion; version != nil; version = version.Next {
			if version.CreatedByTxn == txn.ID {
				version.CreatedAt = commitTime
				break
			}
		}
		row.mu.Unlock()
	}

	// Remove from active transactions
	mgr.activeTransactions.Delete(uint64(txn.ID))

	return nil
}

// AbortTransaction rolls back a transaction
func (mgr *MVCCManager) AbortTransaction(txn *MVCCTransaction) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.State != TxActive {
		return ErrTransactionNotActive
	}

	// Mark as aborted
	txn.State = TxAborted

	// Remove all versions created by this transaction
	for _, row := range txn.WriteSet {
		row.mu.Lock()
		mgr.removeTransactionVersions(row, txn.ID)
		row.mu.Unlock()
	}

	// Remove from active transactions
	mgr.activeTransactions.Delete(uint64(txn.ID))

	return nil
}

// ReadRow reads a row with snapshot isolation
func (mgr *MVCCManager) ReadRow(txn *MVCCTransaction, row *MVCCRow) (map[string]interface{}, bool) {
	row.mu.RLock()
	defer row.mu.RUnlock()

	// Find the latest version visible to this transaction
	for version := row.LatestVersion; version != nil; version = version.Next {
		if mgr.isVersionVisible(txn, version) {
			// Check if this version was deleted
			if version.DeletedByTxn != 0 && mgr.isTransactionCommitted(version.DeletedByTxn, txn.SnapshotID) {
				continue // This version was deleted, try next
			}

			// Record read in read set for conflict detection
			txn.mu.Lock()
			txn.ReadSet[string(rune(row.RowID))] = uint64(version.CreatedByTxn)
			txn.mu.Unlock()

			return version.Data, true
		}
	}

	return nil, false // No visible version found
}

// WriteRow creates a new version of a row
func (mgr *MVCCManager) WriteRow(txn *MVCCTransaction, row *MVCCRow, data map[string]interface{}) error {
	if txn.State != TxActive {
		return ErrTransactionNotActive
	}

	row.mu.Lock()
	defer row.mu.Unlock()

	// Create new version
	newVersion := &RowVersion{
		Data:         make(map[string]interface{}),
		CreatedByTxn: txn.ID,
		DeletedByTxn: 0,
		CreatedAt:    time.Time{}, // Will be set on commit
		Next:         row.LatestVersion,
	}

	// Copy data
	for k, v := range data {
		newVersion.Data[k] = v
	}

	// Add to version chain
	row.LatestVersion = newVersion

	// Add to transaction's write set
	txn.mu.Lock()
	txn.WriteSet[string(rune(row.RowID))] = row
	txn.mu.Unlock()

	return nil
}

// DeleteRow marks a row as deleted by creating a delete marker
func (mgr *MVCCManager) DeleteRow(txn *MVCCTransaction, row *MVCCRow) error {
	if txn.State != TxActive {
		return ErrTransactionNotActive
	}

	row.mu.Lock()
	defer row.mu.Unlock()

	// Find the latest visible version
	for version := row.LatestVersion; version != nil; version = version.Next {
		if mgr.isVersionVisible(txn, version) && version.DeletedByTxn == 0 {
			// Mark this version as deleted
			version.DeletedByTxn = txn.ID

			// Add to write set
			txn.mu.Lock()
			txn.WriteSet[string(rune(row.RowID))] = row
			txn.mu.Unlock()

			return nil
		}
	}

	return ErrRowNotFound
}

// isVersionVisible checks if a version is visible to a transaction
func (mgr *MVCCManager) isVersionVisible(txn *MVCCTransaction, version *RowVersion) bool {
	// Version is visible if:
	// 1. It was created by this transaction, OR
	// 2. It was created by a transaction that committed before this transaction's snapshot

	if version.CreatedByTxn == txn.ID {
		return true // Own writes are always visible
	}

	return mgr.isTransactionCommitted(version.CreatedByTxn, txn.SnapshotID)
}

// isTransactionCommitted checks if a transaction was committed before a snapshot
func (mgr *MVCCManager) isTransactionCommitted(txnID, snapshotID TransactionID) bool {
	if txnID <= snapshotID {
		return true // Transaction committed before snapshot
	}

	// Check if transaction is in active transactions (uncommitted)
	if _, exists := mgr.activeTransactions.Load(uint64(txnID)); exists {
		return false
	}

	// Must be committed after snapshot
	return false
}

// removeTransactionVersions removes all versions created by a transaction (for abort)
func (mgr *MVCCManager) removeTransactionVersions(row *MVCCRow, txnID TransactionID) {
	// Remove versions from chain
	var prev *RowVersion
	current := row.LatestVersion

	for current != nil {
		if current.CreatedByTxn == txnID {
			if prev == nil {
				row.LatestVersion = current.Next
			} else {
				prev.Next = current.Next
			}
			next := current.Next
			current = next
		} else {
			// Also remove delete markers
			if current.DeletedByTxn == txnID {
				current.DeletedByTxn = 0
			}
			prev = current
			current = current.Next
		}
	}
}

// garbageCollector removes old versions that are no longer needed
func (mgr *MVCCManager) garbageCollector() {
	for {
		select {
		case <-mgr.gcTicker.C:
			mgr.performGarbageCollection()
		case <-mgr.stopGC:
			return
		}
	}
}

// performGarbageCollection removes old row versions
func (mgr *MVCCManager) performGarbageCollection() {
	cutoffTime := time.Now().Add(-mgr.gcThreshold)

	// Find oldest active transaction
	oldestActiveTxn := TransactionID(^uint64(0)) // Max value
	mgr.activeTransactions.Range(func(key, value interface{}) bool {
		txn := value.(*MVCCTransaction)
		if txn.ID < oldestActiveTxn {
			oldestActiveTxn = txn.ID
		}
		return true
	})

	// TODO: Implement actual GC logic to remove old versions
	// This would need to iterate through all tables and remove versions
	// that are older than cutoffTime and not visible to any active transaction

	// Prevent unused variable warning
	_ = cutoffTime
	_ = oldestActiveTxn
}

// Stop shuts down the MVCC manager
func (mgr *MVCCManager) Stop() {
	if mgr.gcTicker != nil {
		mgr.gcTicker.Stop()
	}
	close(mgr.stopGC)
}

// Custom errors
var (
	ErrTransactionNotActive = NewDBError("transaction is not active")
	ErrRowNotFound         = NewDBError("row not found")
	ErrConflictDetected    = NewDBError("write-write conflict detected")
)

// DBError represents a database error
type DBError struct {
	Message string
}

func (e *DBError) Error() string {
	return e.Message
}

func NewDBError(message string) *DBError {
	return &DBError{Message: message}
}