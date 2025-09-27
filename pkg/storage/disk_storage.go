// Package storage provides disk-based persistence for columnar tables.
package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"unsafe"

	"fastpostgres/pkg/engine"
)

// DiskStorage manages table persistence to disk.
type DiskStorage struct {
	dataDir string
	mu      sync.RWMutex
}

// TableMetadata stores table schema information.
type TableMetadata struct {
	Name      string
	RowCount  uint64
	Columns   []ColumnMetadata
	CreatedAt int64
	UpdatedAt int64
}

// ColumnMetadata stores column schema information.
type ColumnMetadata struct {
	Name     string
	Type     engine.DataType
	Length   uint64
	Capacity uint64
}

// NewDiskStorage creates a new disk storage manager.
func NewDiskStorage(dataDir string) (*DiskStorage, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	return &DiskStorage{
		dataDir: dataDir,
	}, nil
}

// SaveTable writes a table to disk in columnar format.
func (ds *DiskStorage) SaveTable(table *engine.Table) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	tableDir := filepath.Join(ds.dataDir, table.Name)
	if err := os.MkdirAll(tableDir, 0755); err != nil {
		return fmt.Errorf("failed to create table directory: %w", err)
	}

	// Save metadata
	if err := ds.saveMetadata(table, tableDir); err != nil {
		return err
	}

	// Save each column
	for _, col := range table.Columns {
		if err := ds.saveColumn(col, tableDir); err != nil {
			return fmt.Errorf("failed to save column %s: %w", col.Name, err)
		}
	}

	return nil
}

// LoadTable reads a table from disk.
func (ds *DiskStorage) LoadTable(tableName string) (*engine.Table, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	tableDir := filepath.Join(ds.dataDir, tableName)

	// Load metadata
	metadata, err := ds.loadMetadata(tableDir)
	if err != nil {
		return nil, err
	}

	// Create table
	table := engine.NewTable(metadata.Name)
	table.RowCount = metadata.RowCount

	// Load each column
	for _, colMeta := range metadata.Columns {
		col, err := ds.loadColumn(colMeta, tableDir)
		if err != nil {
			return nil, fmt.Errorf("failed to load column %s: %w", colMeta.Name, err)
		}
		table.AddColumn(col)
	}

	return table, nil
}

// saveMetadata writes table metadata to disk.
func (ds *DiskStorage) saveMetadata(table *engine.Table, tableDir string) error {
	metadata := TableMetadata{
		Name:     table.Name,
		RowCount: table.RowCount,
		Columns:  make([]ColumnMetadata, len(table.Columns)),
	}

	for i, col := range table.Columns {
		metadata.Columns[i] = ColumnMetadata{
			Name:     col.Name,
			Type:     col.Type,
			Length:   col.Length,
			Capacity: col.Capacity,
		}
	}

	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	metaPath := filepath.Join(tableDir, "metadata.json")
	if err := os.WriteFile(metaPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	return nil
}

// loadMetadata reads table metadata from disk.
func (ds *DiskStorage) loadMetadata(tableDir string) (*TableMetadata, error) {
	metaPath := filepath.Join(tableDir, "metadata.json")
	data, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	var metadata TableMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	return &metadata, nil
}

// saveColumn writes column data to disk.
func (ds *DiskStorage) saveColumn(col *engine.Column, tableDir string) error {
	colPath := filepath.Join(tableDir, fmt.Sprintf("%s.col", col.Name))
	file, err := os.Create(colPath)
	if err != nil {
		return fmt.Errorf("failed to create column file: %w", err)
	}
	defer file.Close()

	// Write column header
	header := make([]byte, 24)
	binary.LittleEndian.PutUint64(header[0:8], uint64(col.Type))
	binary.LittleEndian.PutUint64(header[8:16], col.Length)
	binary.LittleEndian.PutUint64(header[16:24], col.Capacity)

	if _, err := file.Write(header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Write null bitmap
	nullBitmap := make([]byte, (col.Length+7)/8)
	for i := uint64(0); i < col.Length; i++ {
		if col.Nulls[i] {
			byteIdx := i / 8
			bitIdx := i % 8
			nullBitmap[byteIdx] |= 1 << bitIdx
		}
	}
	if _, err := file.Write(nullBitmap); err != nil {
		return fmt.Errorf("failed to write null bitmap: %w", err)
	}

	// Write data based on type
	switch col.Type {
	case engine.TypeInt64:
		data := (*[]int64)(col.Data)
		for i := uint64(0); i < col.Length; i++ {
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, uint64((*data)[i]))
			if _, err := file.Write(buf); err != nil {
				return fmt.Errorf("failed to write int64 data: %w", err)
			}
		}

	case engine.TypeString:
		data := (*[]string)(col.Data)
		for i := uint64(0); i < col.Length; i++ {
			str := (*data)[i]
			// Write string length
			lenBuf := make([]byte, 4)
			binary.LittleEndian.PutUint32(lenBuf, uint32(len(str)))
			if _, err := file.Write(lenBuf); err != nil {
				return fmt.Errorf("failed to write string length: %w", err)
			}
			// Write string data
			if _, err := file.Write([]byte(str)); err != nil {
				return fmt.Errorf("failed to write string data: %w", err)
			}
		}

	default:
		return fmt.Errorf("unsupported column type: %v", col.Type)
	}

	return file.Sync()
}

// loadColumn reads column data from disk.
func (ds *DiskStorage) loadColumn(colMeta ColumnMetadata, tableDir string) (*engine.Column, error) {
	colPath := filepath.Join(tableDir, fmt.Sprintf("%s.col", colMeta.Name))
	file, err := os.Open(colPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open column file: %w", err)
	}
	defer file.Close()

	// Read header
	header := make([]byte, 24)
	if _, err := io.ReadFull(file, header); err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	dataType := engine.DataType(binary.LittleEndian.Uint64(header[0:8]))
	length := binary.LittleEndian.Uint64(header[8:16])
	capacity := binary.LittleEndian.Uint64(header[16:24])

	// Create column
	col := engine.NewColumn(colMeta.Name, dataType, capacity)
	col.Length = length

	// Read null bitmap
	nullBitmap := make([]byte, (length+7)/8)
	if _, err := io.ReadFull(file, nullBitmap); err != nil {
		return nil, fmt.Errorf("failed to read null bitmap: %w", err)
	}

	// Decode null bitmap
	col.Nulls = make([]bool, length)
	for i := uint64(0); i < length; i++ {
		byteIdx := i / 8
		bitIdx := i % 8
		col.Nulls[i] = (nullBitmap[byteIdx] & (1 << bitIdx)) != 0
	}

	// Read data based on type
	switch dataType {
	case engine.TypeInt64:
		data := make([]int64, capacity)
		for i := uint64(0); i < length; i++ {
			buf := make([]byte, 8)
			if _, err := io.ReadFull(file, buf); err != nil {
				return nil, fmt.Errorf("failed to read int64 data: %w", err)
			}
			data[i] = int64(binary.LittleEndian.Uint64(buf))
		}
		col.Data = unsafe.Pointer(&data)

	case engine.TypeString:
		data := make([]string, capacity)
		for i := uint64(0); i < length; i++ {
			// Read string length
			lenBuf := make([]byte, 4)
			if _, err := io.ReadFull(file, lenBuf); err != nil {
				return nil, fmt.Errorf("failed to read string length: %w", err)
			}
			strLen := binary.LittleEndian.Uint32(lenBuf)

			// Read string data
			strBuf := make([]byte, strLen)
			if _, err := io.ReadFull(file, strBuf); err != nil {
				return nil, fmt.Errorf("failed to read string data: %w", err)
			}
			data[i] = string(strBuf)
		}
		col.Data = unsafe.Pointer(&data)

	default:
		return nil, fmt.Errorf("unsupported column type: %v", dataType)
	}

	return col, nil
}

// ListTables returns all table names in the data directory.
func (ds *DiskStorage) ListTables() ([]string, error) {
	entries, err := os.ReadDir(ds.dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read data directory: %w", err)
	}

	var tables []string
	for _, entry := range entries {
		if entry.IsDir() {
			// Check if it has metadata.json
			metaPath := filepath.Join(ds.dataDir, entry.Name(), "metadata.json")
			if _, err := os.Stat(metaPath); err == nil {
				tables = append(tables, entry.Name())
			}
		}
	}

	return tables, nil
}

// DeleteTable removes a table from disk.
func (ds *DiskStorage) DeleteTable(tableName string) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	tableDir := filepath.Join(ds.dataDir, tableName)
	return os.RemoveAll(tableDir)
}