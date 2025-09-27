// Package storage provides checkpoint management for crash recovery.
package storage

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"fastpostgres/pkg/engine"
)

// CheckpointManager handles periodic checkpointing of database state.
type CheckpointManager struct {
	wal         *WAL
	storage     *DiskStorage
	db          *engine.Database
	interval    time.Duration
	lastLSN     uint64
	checkpointMu sync.Mutex
	stopChan    chan struct{}
	running     bool
}

// CheckpointInfo stores metadata about a checkpoint.
type CheckpointInfo struct {
	LSN       uint64
	Timestamp time.Time
	Tables    []string
}

// NewCheckpointManager creates a new checkpoint manager.
func NewCheckpointManager(wal *WAL, storage *DiskStorage, db *engine.Database, interval time.Duration) *CheckpointManager {
	return &CheckpointManager{
		wal:      wal,
		storage:  storage,
		db:       db,
		interval: interval,
		stopChan: make(chan struct{}),
	}
}

// Start begins automatic checkpointing.
func (cm *CheckpointManager) Start() {
	if cm.running {
		return
	}

	cm.running = true
	go cm.checkpointLoop()
	log.Printf("Checkpoint manager started with interval: %v", cm.interval)
}

// Stop halts automatic checkpointing.
func (cm *CheckpointManager) Stop() {
	if !cm.running {
		return
	}

	cm.running = false
	close(cm.stopChan)
	log.Println("Checkpoint manager stopped")
}

// checkpointLoop runs periodic checkpoints.
func (cm *CheckpointManager) checkpointLoop() {
	ticker := time.NewTicker(cm.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := cm.CreateCheckpoint(); err != nil {
				log.Printf("Checkpoint failed: %v", err)
			}
		case <-cm.stopChan:
			// Final checkpoint before shutdown
			if err := cm.CreateCheckpoint(); err != nil {
				log.Printf("Final checkpoint failed: %v", err)
			}
			return
		}
	}
}

// CreateCheckpoint creates a checkpoint of all in-memory tables.
func (cm *CheckpointManager) CreateCheckpoint() error {
	cm.checkpointMu.Lock()
	defer cm.checkpointMu.Unlock()

	start := time.Now()
	log.Println("Starting checkpoint...")

	// Get current LSN
	currentLSN := cm.wal.GetCurrentLSN()

	// Save all tables to disk
	var savedTables []string
	cm.db.Tables.Range(func(key, value interface{}) bool {
		tableName := key.(string)
		table := value.(*engine.Table)

		if err := cm.storage.SaveTable(table); err != nil {
			log.Printf("Failed to save table %s: %v", tableName, err)
			return true // Continue with other tables
		}

		savedTables = append(savedTables, tableName)
		return true
	})

	// Save checkpoint info
	info := CheckpointInfo{
		LSN:       currentLSN,
		Timestamp: time.Now(),
		Tables:    savedTables,
	}

	if err := cm.saveCheckpointInfo(info); err != nil {
		return fmt.Errorf("failed to save checkpoint info: %w", err)
	}

	cm.lastLSN = currentLSN

	// Clean up old WAL segments (those before this checkpoint)
	if err := cm.cleanupOldWAL(currentLSN); err != nil {
		log.Printf("Failed to cleanup old WAL: %v", err)
	}

	log.Printf("Checkpoint completed in %v (LSN: %d, Tables: %d)",
		time.Since(start), currentLSN, len(savedTables))

	return nil
}

// saveCheckpointInfo writes checkpoint metadata.
func (cm *CheckpointManager) saveCheckpointInfo(info CheckpointInfo) error {
	data, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint info: %w", err)
	}

	checkpointPath := filepath.Join(cm.storage.dataDir, "checkpoint.json")
	if err := os.WriteFile(checkpointPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write checkpoint info: %w", err)
	}

	return nil
}

// LoadCheckpointInfo loads checkpoint metadata.
func (cm *CheckpointManager) LoadCheckpointInfo() (*CheckpointInfo, error) {
	checkpointPath := filepath.Join(cm.storage.dataDir, "checkpoint.json")

	data, err := os.ReadFile(checkpointPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No checkpoint exists
		}
		return nil, fmt.Errorf("failed to read checkpoint info: %w", err)
	}

	var info CheckpointInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("failed to unmarshal checkpoint info: %w", err)
	}

	return &info, nil
}

// cleanupOldWAL removes WAL segments that are no longer needed.
func (cm *CheckpointManager) cleanupOldWAL(checkpointLSN uint64) error {
	segments, err := cm.wal.GetAllSegments()
	if err != nil {
		return err
	}

	// Keep at least the last 2 segments for safety
	if len(segments) <= 2 {
		return nil
	}

	// Remove old segments (except last 2)
	for i := 0; i < len(segments)-2; i++ {
		log.Printf("Removing old WAL segment: %s", segments[i])
		if err := cm.wal.RemoveSegment(segments[i]); err != nil {
			log.Printf("Failed to remove WAL segment %s: %v", segments[i], err)
		}
	}

	return nil
}

// RecoverFromCheckpoint loads data from the last checkpoint and replays WAL.
func (cm *CheckpointManager) RecoverFromCheckpoint() error {
	log.Println("Starting recovery from checkpoint...")

	// Load checkpoint info
	checkpointInfo, err := cm.LoadCheckpointInfo()
	if err != nil {
		return fmt.Errorf("failed to load checkpoint info: %w", err)
	}

	if checkpointInfo == nil {
		log.Println("No checkpoint found, starting fresh")
		return nil
	}

	log.Printf("Found checkpoint at LSN %d with %d tables",
		checkpointInfo.LSN, len(checkpointInfo.Tables))

	// Load all tables from checkpoint
	for _, tableName := range checkpointInfo.Tables {
		table, err := cm.storage.LoadTable(tableName)
		if err != nil {
			log.Printf("Failed to load table %s: %v", tableName, err)
			continue
		}
		cm.db.Tables.Store(tableName, table)
		log.Printf("Loaded table: %s (%d rows)", tableName, table.RowCount)
	}

	// Replay WAL records after checkpoint
	if err := cm.replayWAL(checkpointInfo.LSN); err != nil {
		return fmt.Errorf("failed to replay WAL: %w", err)
	}

	log.Println("Recovery completed successfully")
	return nil
}

// replayWAL replays WAL records after the given LSN.
func (cm *CheckpointManager) replayWAL(fromLSN uint64) error {
	segments, err := cm.wal.GetAllSegments()
	if err != nil {
		return err
	}

	recordsReplayed := 0

	for _, segmentPath := range segments {
		records, err := cm.wal.ReadRecords(segmentPath)
		if err != nil {
			log.Printf("Failed to read WAL segment %s: %v", segmentPath, err)
			continue
		}

		for _, record := range records {
			if record.LSN <= fromLSN {
				continue // Skip records before checkpoint
			}

			if err := cm.applyWALRecord(record); err != nil {
				log.Printf("Failed to apply WAL record LSN %d: %v", record.LSN, err)
				continue
			}
			recordsReplayed++
		}
	}

	if recordsReplayed > 0 {
		log.Printf("Replayed %d WAL records", recordsReplayed)
	}

	return nil
}

// applyWALRecord applies a single WAL record to in-memory tables.
func (cm *CheckpointManager) applyWALRecord(record *WALRecord) error {
	switch record.Type {
	case WALRecordInsert:
		return cm.applyInsert(record)
	case WALRecordUpdate:
		return cm.applyUpdate(record)
	case WALRecordDelete:
		return cm.applyDelete(record)
	case WALRecordCreateTable:
		return cm.applyCreateTable(record)
	case WALRecordDropTable:
		return cm.applyDropTable(record)
	default:
		log.Printf("Unknown WAL record type: %d", record.Type)
	}
	return nil
}

// applyInsert replays an INSERT operation.
func (cm *CheckpointManager) applyInsert(record *WALRecord) error {
	// Deserialize row data
	var rowData map[string]interface{}
	if err := json.Unmarshal(record.Data, &rowData); err != nil {
		return fmt.Errorf("failed to unmarshal insert data: %w", err)
	}

	// Get table
	tableInterface, exists := cm.db.Tables.Load(record.TableName)
	if !exists {
		return fmt.Errorf("table %s not found", record.TableName)
	}

	table := tableInterface.(*engine.Table)
	return table.InsertRow(rowData)
}

// applyUpdate replays an UPDATE operation.
func (cm *CheckpointManager) applyUpdate(record *WALRecord) error {
	// TODO: Implement when UPDATE is added
	log.Printf("UPDATE replay not yet implemented")
	return nil
}

// applyDelete replays a DELETE operation.
func (cm *CheckpointManager) applyDelete(record *WALRecord) error {
	// TODO: Implement when DELETE is added
	log.Printf("DELETE replay not yet implemented")
	return nil
}

// applyCreateTable replays a CREATE TABLE operation.
func (cm *CheckpointManager) applyCreateTable(record *WALRecord) error {
	// TODO: Implement when CREATE TABLE is added
	log.Printf("CREATE TABLE replay not yet implemented")
	return nil
}

// applyDropTable replays a DROP TABLE operation.
func (cm *CheckpointManager) applyDropTable(record *WALRecord) error {
	cm.db.Tables.Delete(record.TableName)
	return nil
}