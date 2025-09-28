package storage

import (
	"sync"
	"time"
)

// WALBuffer provides buffered, batched writing to WAL for better performance.
type WALBuffer struct {
	wal            *WAL
	buffer         []*WALRecord
	bufferSize     int
	flushInterval  time.Duration
	mu             sync.Mutex
	flushChan      chan struct{}
	stopChan       chan struct{}
	wg             sync.WaitGroup
}

// NewWALBuffer creates a buffered WAL writer.
func NewWALBuffer(wal *WAL, bufferSize int, flushInterval time.Duration) *WALBuffer {
	wb := &WALBuffer{
		wal:           wal,
		buffer:        make([]*WALRecord, 0, bufferSize),
		bufferSize:    bufferSize,
		flushInterval: flushInterval,
		flushChan:     make(chan struct{}, 1),
		stopChan:      make(chan struct{}),
	}

	// Start background flusher
	wb.wg.Add(1)
	go wb.backgroundFlusher()

	return wb
}

// Append adds a record to the buffer.
// Returns immediately for better throughput.
func (wb *WALBuffer) Append(record *WALRecord) error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	wb.buffer = append(wb.buffer, record)

	// Trigger flush if buffer is full
	if len(wb.buffer) >= wb.bufferSize {
		select {
		case wb.flushChan <- struct{}{}:
		default:
		}
	}

	return nil
}

// AppendSync adds a record and waits for it to be flushed to disk.
// Use this for critical operations that need durability guarantee.
func (wb *WALBuffer) AppendSync(record *WALRecord) error {
	if err := wb.Append(record); err != nil {
		return err
	}
	return wb.Flush()
}

// Flush writes all buffered records to disk.
func (wb *WALBuffer) Flush() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.buffer) == 0 {
		return nil
	}

	// Write all buffered records
	for _, record := range wb.buffer {
		if err := wb.wal.WriteRecord(record); err != nil {
			return err
		}
	}

	// Clear buffer
	wb.buffer = wb.buffer[:0]

	return nil
}

// backgroundFlusher periodically flushes the buffer.
func (wb *WALBuffer) backgroundFlusher() {
	defer wb.wg.Done()

	ticker := time.NewTicker(wb.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Periodic flush
			wb.Flush()

		case <-wb.flushChan:
			// Triggered flush (buffer full)
			wb.Flush()

		case <-wb.stopChan:
			// Final flush before shutdown
			wb.Flush()
			return
		}
	}
}

// Close stops the background flusher and flushes remaining records.
func (wb *WALBuffer) Close() error {
	close(wb.stopChan)
	wb.wg.Wait()
	return wb.Flush()
}

// GetBufferSize returns current buffer size.
func (wb *WALBuffer) GetBufferSize() int {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	return len(wb.buffer)
}

// GetStats returns WAL buffer statistics.
func (wb *WALBuffer) GetStats() WALBufferStats {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	return WALBufferStats{
		BufferedRecords: len(wb.buffer),
		BufferCapacity:  wb.bufferSize,
		BufferUsage:     float64(len(wb.buffer)) / float64(wb.bufferSize) * 100,
	}
}

// WALBufferStats contains WAL buffer statistics.
type WALBufferStats struct {
	BufferedRecords int
	BufferCapacity  int
	BufferUsage     float64 // Percentage
}