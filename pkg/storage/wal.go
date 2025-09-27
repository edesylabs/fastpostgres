// Package storage provides Write-Ahead Logging (WAL) for durability.
// WAL ensures all changes are written to disk before being applied to in-memory structures.
package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// WAL manages the write-ahead log for durability.
type WAL struct {
	dir            string
	currentSegment *WALSegment
	segmentSize    int64
	segments       []*WALSegment
	lsn            uint64 // Log Sequence Number
	mu             sync.Mutex
}

// WALSegment represents a single WAL file.
type WALSegment struct {
	id       uint64
	file     *os.File
	path     string
	size     int64
	created  time.Time
	synced   bool
}

// WALRecord represents a single log record.
type WALRecord struct {
	LSN       uint64
	Type      WALRecordType
	TableName string
	Data      []byte
	Timestamp time.Time
	CRC       uint32
}

// WALRecordType identifies the type of operation.
type WALRecordType uint8

const (
	WALRecordInsert WALRecordType = iota
	WALRecordUpdate
	WALRecordDelete
	WALRecordCheckpoint
	WALRecordCreateTable
	WALRecordDropTable
)

const (
	defaultSegmentSize = 16 * 1024 * 1024 // 16MB per segment
	walRecordHeader    = 28                 // Size of fixed header
)

// NewWAL creates a new WAL manager.
func NewWAL(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	wal := &WAL{
		dir:         dir,
		segmentSize: defaultSegmentSize,
		segments:    make([]*WALSegment, 0),
		lsn:         1,
	}

	// Create first segment
	if err := wal.createNewSegment(); err != nil {
		return nil, err
	}

	return wal, nil
}

// WriteRecord writes a record to the WAL and syncs to disk.
func (w *WAL) WriteRecord(record *WALRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Assign LSN
	record.LSN = w.lsn
	w.lsn++
	record.Timestamp = time.Now()

	// Serialize record
	data, err := w.serializeRecord(record)
	if err != nil {
		return fmt.Errorf("failed to serialize record: %w", err)
	}

	// Check if we need a new segment
	if w.currentSegment.size+int64(len(data)) > w.segmentSize {
		if err := w.createNewSegment(); err != nil {
			return err
		}
	}

	// Write to current segment
	n, err := w.currentSegment.file.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	w.currentSegment.size += int64(n)

	// Sync to disk for durability
	if err := w.currentSegment.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL: %w", err)
	}

	w.currentSegment.synced = true
	return nil
}

// serializeRecord converts a WAL record to bytes.
func (w *WAL) serializeRecord(record *WALRecord) ([]byte, error) {
	// Calculate size: header + table name + data
	tableNameLen := len(record.TableName)
	dataLen := len(record.Data)
	totalSize := walRecordHeader + tableNameLen + dataLen

	buf := make([]byte, totalSize)
	offset := 0

	// Write header
	binary.LittleEndian.PutUint64(buf[offset:], record.LSN)
	offset += 8
	buf[offset] = byte(record.Type)
	offset += 1
	binary.LittleEndian.PutUint64(buf[offset:], uint64(record.Timestamp.Unix()))
	offset += 8
	binary.LittleEndian.PutUint32(buf[offset:], uint32(tableNameLen))
	offset += 4
	binary.LittleEndian.PutUint32(buf[offset:], uint32(dataLen))
	offset += 4

	// Write table name
	copy(buf[offset:], []byte(record.TableName))
	offset += tableNameLen

	// Write data
	copy(buf[offset:], record.Data)
	offset += dataLen

	// Calculate and write CRC (for the entire record)
	crc := crc32.ChecksumIEEE(buf[:offset])
	binary.LittleEndian.PutUint32(buf[offset-4:offset], crc)

	return buf, nil
}

// ReadRecords reads all records from a segment file.
func (w *WAL) ReadRecords(segmentPath string) ([]*WALRecord, error) {
	file, err := os.Open(segmentPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment: %w", err)
	}
	defer file.Close()

	var records []*WALRecord

	for {
		record, err := w.readNextRecord(file)
		if err == io.EOF {
			break
		}
		if err != nil {
			return records, fmt.Errorf("failed to read record: %w", err)
		}
		records = append(records, record)
	}

	return records, nil
}

// readNextRecord reads a single record from a file.
func (w *WAL) readNextRecord(file *os.File) (*WALRecord, error) {
	// Read header
	header := make([]byte, walRecordHeader)
	if _, err := io.ReadFull(file, header); err != nil {
		return nil, err
	}

	// Parse header
	offset := 0
	lsn := binary.LittleEndian.Uint64(header[offset:])
	offset += 8
	recordType := WALRecordType(header[offset])
	offset += 1
	timestamp := time.Unix(int64(binary.LittleEndian.Uint64(header[offset:])), 0)
	offset += 8
	tableNameLen := binary.LittleEndian.Uint32(header[offset:])
	offset += 4
	dataLen := binary.LittleEndian.Uint32(header[offset:])
	offset += 4
	crc := binary.LittleEndian.Uint32(header[offset:])

	// Read table name
	tableName := make([]byte, tableNameLen)
	if _, err := io.ReadFull(file, tableName); err != nil {
		return nil, err
	}

	// Read data
	data := make([]byte, dataLen)
	if _, err := io.ReadFull(file, data); err != nil {
		return nil, err
	}

	// Verify CRC
	allData := append(header[:walRecordHeader-4], tableName...)
	allData = append(allData, data...)
	calculatedCRC := crc32.ChecksumIEEE(allData)
	if calculatedCRC != crc {
		return nil, fmt.Errorf("CRC mismatch: expected %d, got %d", crc, calculatedCRC)
	}

	return &WALRecord{
		LSN:       lsn,
		Type:      recordType,
		TableName: string(tableName),
		Data:      data,
		Timestamp: timestamp,
		CRC:       crc,
	}, nil
}

// createNewSegment creates a new WAL segment file.
func (w *WAL) createNewSegment() error {
	segmentID := uint64(len(w.segments))
	filename := fmt.Sprintf("wal-%016d.log", segmentID)
	path := filepath.Join(w.dir, filename)

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to create WAL segment: %w", err)
	}

	segment := &WALSegment{
		id:      segmentID,
		file:    file,
		path:    path,
		size:    0,
		created: time.Now(),
		synced:  false,
	}

	w.currentSegment = segment
	w.segments = append(w.segments, segment)

	return nil
}

// Close closes all WAL segments.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, segment := range w.segments {
		if err := segment.file.Sync(); err != nil {
			return err
		}
		if err := segment.file.Close(); err != nil {
			return err
		}
	}

	return nil
}

// GetAllSegments returns paths to all WAL segment files.
func (w *WAL) GetAllSegments() ([]string, error) {
	files, err := filepath.Glob(filepath.Join(w.dir, "wal-*.log"))
	if err != nil {
		return nil, err
	}
	return files, nil
}

// RemoveSegment removes a WAL segment file (after checkpoint).
func (w *WAL) RemoveSegment(segmentPath string) error {
	return os.Remove(segmentPath)
}

// GetCurrentLSN returns the current log sequence number.
func (w *WAL) GetCurrentLSN() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lsn
}