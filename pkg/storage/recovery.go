package storage

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"

	"fastpostgres/pkg/engine"
)

// RecoveryManager handles crash recovery from WAL.
type RecoveryManager struct {
	wal             *WAL
	db              *engine.Database
	checkpointMgr   *CheckpointManager
	lastCheckpoint  *CheckpointInfo
}

// NewRecoveryManager creates a recovery manager.
func NewRecoveryManager(wal *WAL, db *engine.Database, checkpointMgr *CheckpointManager) *RecoveryManager {
	return &RecoveryManager{
		wal:           wal,
		db:            db,
		checkpointMgr: checkpointMgr,
	}
}

// Recover performs crash recovery from WAL.
func (rm *RecoveryManager) Recover() error {
	log.Println("Starting recovery from WAL...")

	// Load last checkpoint
	checkpoint, err := rm.checkpointMgr.LoadCheckpointInfo()
	if err != nil {
		log.Printf("No checkpoint found, starting from beginning: %v", err)
		checkpoint = nil
	} else if checkpoint != nil {
		log.Printf("Found checkpoint at LSN %d", checkpoint.LSN)
		rm.lastCheckpoint = checkpoint
	}

	// Get all WAL segments
	segments, err := rm.wal.GetAllSegments()
	if err != nil {
		return fmt.Errorf("failed to get WAL segments: %w", err)
	}

	if len(segments) == 0 {
		log.Println("No WAL segments found, nothing to recover")
		return nil
	}

	// Sort segments by ID
	sort.Strings(segments)

	// Determine starting point
	startLSN := uint64(0)
	if checkpoint != nil {
		startLSN = checkpoint.LSN
	}

	log.Printf("Replaying WAL from LSN %d", startLSN)

	// Replay all WAL records
	recordCount := 0
	for _, segmentPath := range segments {
		records, err := rm.wal.ReadRecords(segmentPath)
		if err != nil {
			log.Printf("Warning: failed to read segment %s: %v", segmentPath, err)
			continue
		}

		for _, record := range records {
			// Skip records before checkpoint
			if record.LSN <= startLSN {
				continue
			}

			if err := rm.applyRecord(record); err != nil {
				log.Printf("Warning: failed to apply record LSN %d: %v", record.LSN, err)
				// Continue with next record (best effort recovery)
				continue
			}
			recordCount++
		}
	}

	log.Printf("Recovery complete. Replayed %d WAL records", recordCount)
	return nil
}

// applyRecord applies a single WAL record to rebuild database state.
func (rm *RecoveryManager) applyRecord(record *WALRecord) error {
	switch record.Type {
	case WALRecordCreateTable:
		return rm.replayCreateTable(record)

	case WALRecordInsert:
		return rm.replayInsert(record)

	case WALRecordUpdate:
		return rm.replayUpdate(record)

	case WALRecordDelete:
		return rm.replayDelete(record)

	case WALRecordDropTable:
		return rm.replayDropTable(record)

	case WALRecordCheckpoint:
		// Checkpoint records are just markers, no action needed
		return nil

	default:
		return fmt.Errorf("unknown WAL record type: %v", record.Type)
	}
}

// replayCreateTable recreates a table from WAL.
func (rm *RecoveryManager) replayCreateTable(record *WALRecord) error {
	// Parse table schema from record data
	var schema TableSchema
	if err := json.Unmarshal(record.Data, &schema); err != nil {
		return fmt.Errorf("failed to parse table schema: %w", err)
	}

	// Check if table already exists
	if _, exists := rm.db.Tables.Load(record.TableName); exists {
		// Table already exists, skip
		return nil
	}

	// Create table
	table := engine.NewTable(record.TableName)

	// Add columns
	for _, colDef := range schema.Columns {
		col := engine.NewColumn(colDef.Name, colDef.Type, 1000)
		table.AddColumn(col)
	}

	// Store table
	rm.db.Tables.Store(record.TableName, table)

	log.Printf("Recovered table: %s with %d columns", record.TableName, len(schema.Columns))
	return nil
}

// replayInsert replays an INSERT operation.
func (rm *RecoveryManager) replayInsert(record *WALRecord) error {
	// Get table
	tableInterface, exists := rm.db.Tables.Load(record.TableName)
	if !exists {
		return fmt.Errorf("table %s not found during recovery", record.TableName)
	}

	table := tableInterface.(*engine.Table)

	// Parse row data
	var rowData map[string]interface{}
	if err := json.Unmarshal(record.Data, &rowData); err != nil {
		return fmt.Errorf("failed to parse row data: %w", err)
	}

	// Insert row
	return table.InsertRow(rowData)
}

// replayUpdate replays an UPDATE operation.
func (rm *RecoveryManager) replayUpdate(record *WALRecord) error {
	// TODO: Implement when UPDATE is supported
	log.Printf("UPDATE recovery not yet implemented for table %s", record.TableName)
	return nil
}

// replayDelete replays a DELETE operation.
func (rm *RecoveryManager) replayDelete(record *WALRecord) error {
	// TODO: Implement when DELETE is supported
	log.Printf("DELETE recovery not yet implemented for table %s", record.TableName)
	return nil
}

// replayDropTable replays a DROP TABLE operation.
func (rm *RecoveryManager) replayDropTable(record *WALRecord) error {
	rm.db.Tables.Delete(record.TableName)
	log.Printf("Recovered DROP TABLE: %s", record.TableName)
	return nil
}

// TableSchema represents table structure for WAL.
type TableSchema struct {
	Name    string
	Columns []ColumnDef
}

// ColumnDef represents a column definition.
type ColumnDef struct {
	Name string
	Type engine.DataType
}

// VerifyRecovery verifies database state after recovery.
func (rm *RecoveryManager) VerifyRecovery() error {
	log.Println("Verifying recovery...")

	tableCount := 0
	rm.db.Tables.Range(func(key, value interface{}) bool {
		table := value.(*engine.Table)
		log.Printf("  Table: %s, Rows: %d, Columns: %d",
			table.Name, table.RowCount, len(table.Columns))
		tableCount++
		return true
	})

	log.Printf("Recovery verification complete. Found %d tables", tableCount)
	return nil
}

// CleanupOldWAL removes WAL segments older than the last checkpoint.
func (rm *RecoveryManager) CleanupOldWAL() error {
	if rm.lastCheckpoint == nil {
		log.Println("No checkpoint found, skipping WAL cleanup")
		return nil
	}

	segments, err := rm.wal.GetAllSegments()
	if err != nil {
		return err
	}

	removed := 0
	for _, segmentPath := range segments {
		// Parse segment to check if it's before checkpoint
		records, err := rm.wal.ReadRecords(segmentPath)
		if err != nil {
			continue
		}

		// If all records in segment are before checkpoint, remove it
		allBeforeCheckpoint := true
		for _, record := range records {
			if record.LSN > rm.lastCheckpoint.LSN {
				allBeforeCheckpoint = false
				break
			}
		}

		if allBeforeCheckpoint && len(records) > 0 {
			if err := rm.wal.RemoveSegment(segmentPath); err != nil {
				log.Printf("Warning: failed to remove old WAL segment %s: %v", segmentPath, err)
			} else {
				removed++
				log.Printf("Removed old WAL segment: %s", segmentPath)
			}
		}
	}

	log.Printf("WAL cleanup complete. Removed %d old segments", removed)
	return nil
}