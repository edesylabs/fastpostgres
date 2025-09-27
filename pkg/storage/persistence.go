// Package storage provides helper functions for persistence operations.
package storage

import (
	"encoding/json"
	"fmt"

	"fastpostgres/pkg/engine"
)

// PersistenceHelper provides high-level persistence operations.
type PersistenceHelper struct {
	wal     *WAL
	storage *DiskStorage
}

// NewPersistenceHelper creates a new persistence helper.
func NewPersistenceHelper(wal *WAL, storage *DiskStorage) *PersistenceHelper {
	return &PersistenceHelper{
		wal:     wal,
		storage: storage,
	}
}

// InsertWithWAL inserts a row and writes to WAL.
func (ph *PersistenceHelper) InsertWithWAL(table *engine.Table, rowData map[string]interface{}) error {
	// Serialize row data
	data, err := json.Marshal(rowData)
	if err != nil {
		return fmt.Errorf("failed to marshal row data: %w", err)
	}

	// Write to WAL first (write-ahead logging)
	record := &WALRecord{
		Type:      WALRecordInsert,
		TableName: table.Name,
		Data:      data,
	}

	if err := ph.wal.WriteRecord(record); err != nil {
		return fmt.Errorf("failed to write WAL record: %w", err)
	}

	// Now apply to in-memory table
	if err := table.InsertRow(rowData); err != nil {
		return fmt.Errorf("failed to insert row: %w", err)
	}

	return nil
}

// UpdateWithWAL updates a row and writes to WAL.
func (ph *PersistenceHelper) UpdateWithWAL(table *engine.Table, rowID int, rowData map[string]interface{}) error {
	// Serialize update data
	updateData := map[string]interface{}{
		"row_id": rowID,
		"data":   rowData,
	}

	data, err := json.Marshal(updateData)
	if err != nil {
		return fmt.Errorf("failed to marshal update data: %w", err)
	}

	// Write to WAL
	record := &WALRecord{
		Type:      WALRecordUpdate,
		TableName: table.Name,
		Data:      data,
	}

	if err := ph.wal.WriteRecord(record); err != nil {
		return fmt.Errorf("failed to write WAL record: %w", err)
	}

	// TODO: Apply update to in-memory table when UPDATE is implemented
	return nil
}

// DeleteWithWAL deletes a row and writes to WAL.
func (ph *PersistenceHelper) DeleteWithWAL(table *engine.Table, rowID int) error {
	// Serialize delete data
	deleteData := map[string]interface{}{
		"row_id": rowID,
	}

	data, err := json.Marshal(deleteData)
	if err != nil {
		return fmt.Errorf("failed to marshal delete data: %w", err)
	}

	// Write to WAL
	record := &WALRecord{
		Type:      WALRecordDelete,
		TableName: table.Name,
		Data:      data,
	}

	if err := ph.wal.WriteRecord(record); err != nil {
		return fmt.Errorf("failed to write WAL record: %w", err)
	}

	// TODO: Apply delete to in-memory table when DELETE is implemented
	return nil
}