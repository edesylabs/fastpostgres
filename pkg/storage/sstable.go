// Package storage provides SSTable (Sorted String Table) implementation.
// SSTables store sorted key-value pairs on disk with bloom filters for fast lookups.
package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"
)

const (
	sstableMagic       = 0xFADB0001
	sstableVersion     = 1
	sstableBlockSize   = 4096
	indexBlockInterval = 64 // Create index entry every 64 keys
)

// SSTableFormat represents the on-disk format of an SSTable
// Layout: Header | Data Blocks | Index Block | Footer
type SSTableFormat struct {
	Header SSTableHeader
	Footer SSTableFooter
}

// SSTableHeader contains metadata about the SSTable
type SSTableHeader struct {
	Magic        uint32
	Version      uint32
	CreatedAt    int64
	KeyCount     uint64
	MinKeyLength uint32
	MaxKeyLength uint32
	BloomFilter  BloomFilterData
}

// SSTableFooter contains index information
type SSTableFooter struct {
	IndexOffset uint64
	IndexSize   uint64
	CRC32       uint32
}

// BloomFilterData stores serialized bloom filter
type BloomFilterData struct {
	Size     uint64
	Hashes   uint32
	Elements uint64
	Data     []byte
}

// writeSSTable writes an iterator's data to an SSTable file
func (lsm *LSMIndex) writeSSTable(path string, iterator Iterator) (*SSTable, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create SSTable file: %w", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	// Create bloom filter for this SSTable
	bloomFilter := NewBloomFilter(10000, 0.01) // Expect 10K keys, 1% false positive rate

	var keyCount uint64
	var minKey, maxKey []byte
	var indexEntries []IndexEntry
	var currentOffset int64

	// Write header placeholder (we'll update it later)
	headerOffset := currentOffset
	currentOffset += 1024 // Reserve space for header

	// Write data blocks
	keyIndex := 0

	for iterator.HasNext() {
		kv := iterator.Next()
		keyCount++

		// Add to bloom filter
		bloomFilter.Add(kv.Key)

		// Track min/max keys
		if minKey == nil || bytes.Compare(kv.Key, minKey) < 0 {
			minKey = append([]byte(nil), kv.Key...)
		}
		if maxKey == nil || bytes.Compare(kv.Key, maxKey) > 0 {
			maxKey = append([]byte(nil), kv.Key...)
		}

		// Create index entry every N keys
		if keyIndex%indexBlockInterval == 0 {
			indexEntries = append(indexEntries, IndexEntry{
				Key:    append([]byte(nil), kv.Key...),
				Offset: currentOffset,
			})
		}

		// Serialize key-value pair
		kvData, err := serializeKeyValue(kv)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize key-value: %w", err)
		}

		// Write length prefix + data
		if err := binary.Write(writer, binary.LittleEndian, uint32(len(kvData))); err != nil {
			return nil, err
		}
		if _, err := writer.Write(kvData); err != nil {
			return nil, err
		}

		currentOffset += 4 + int64(len(kvData))
		keyIndex++
	}

	// Write index block
	indexOffset := currentOffset
	indexData, err := serializeIndex(indexEntries)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize index: %w", err)
	}

	if _, err := writer.Write(indexData); err != nil {
		return nil, err
	}
	currentOffset += int64(len(indexData))

	// Write footer
	footer := SSTableFooter{
		IndexOffset: uint64(indexOffset),
		IndexSize:   uint64(len(indexData)),
		CRC32:       crc32.ChecksumIEEE(indexData),
	}

	footerData, err := serializeFooter(&footer)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize footer: %w", err)
	}

	if _, err := writer.Write(footerData); err != nil {
		return nil, err
	}

	// Go back and write the actual header
	if _, err := file.Seek(headerOffset, 0); err != nil {
		return nil, err
	}

	header := SSTableHeader{
		Magic:        sstableMagic,
		Version:      sstableVersion,
		CreatedAt:    time.Now().Unix(),
		KeyCount:     keyCount,
		MinKeyLength: uint32(len(minKey)),
		MaxKeyLength: uint32(len(maxKey)),
		BloomFilter:  serializeBloomFilter(bloomFilter),
	}

	headerData, err := serializeHeader(&header)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize header: %w", err)
	}

	if _, err := file.Write(headerData); err != nil {
		return nil, err
	}

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	// Create SSTable metadata
	sstable := &SSTable{
		id:          filepath.Base(path),
		path:        path,
		size:        fileInfo.Size(),
		minKey:      minKey,
		maxKey:      maxKey,
		bloomFilter: bloomFilter,
		index: &SSTableIndex{
			entries: indexEntries,
		},
		createdAt: time.Now(),
	}

	return sstable, nil
}

// searchSSTable searches for a key in an SSTable
func (lsm *LSMIndex) searchSSTable(sstable *SSTable, key []byte) (*KeyValue, bool, error) {
	// Check if key is in range
	if bytes.Compare(key, sstable.minKey) < 0 || bytes.Compare(key, sstable.maxKey) > 0 {
		return nil, false, nil
	}

	// Open SSTable file
	file, err := os.Open(sstable.path)
	if err != nil {
		return nil, false, err
	}
	defer file.Close()

	// Find approximate position using sparse index
	var searchOffset int64 = 1024 // Skip header

	for i := len(sstable.index.entries) - 1; i >= 0; i-- {
		entry := sstable.index.entries[i]
		if bytes.Compare(key, entry.Key) >= 0 {
			searchOffset = entry.Offset
			break
		}
	}

	// Seek to position
	if _, err := file.Seek(searchOffset, 0); err != nil {
		return nil, false, err
	}

	reader := bufio.NewReader(file)

	// Linear search from this position
	for {
		// Read length prefix
		var length uint32
		if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
			if err == io.EOF {
				break
			}
			return nil, false, err
		}

		// Read key-value data
		kvData := make([]byte, length)
		if _, err := io.ReadFull(reader, kvData); err != nil {
			return nil, false, err
		}

		// Deserialize key-value
		kv, err := deserializeKeyValue(kvData)
		if err != nil {
			return nil, false, err
		}

		// Compare keys
		cmp := bytes.Compare(kv.Key, key)
		if cmp == 0 {
			return kv, true, nil
		}
		if cmp > 0 {
			// Key not found (passed it)
			break
		}
	}

	return nil, false, nil
}

// newSSTableIterator creates an iterator for an SSTable
func (lsm *LSMIndex) newSSTableIterator(sstable *SSTable, startKey, endKey []byte) (Iterator, error) {
	file, err := os.Open(sstable.path)
	if err != nil {
		return nil, err
	}

	return &sstableIterator{
		file:     file,
		sstable:  sstable,
		startKey: startKey,
		endKey:   endKey,
		reader:   nil,
		started:  false,
	}, nil
}

// sstableIterator implements the Iterator interface for SSTables
type sstableIterator struct {
	file     *os.File
	sstable  *SSTable
	startKey []byte
	endKey   []byte
	reader   *bufio.Reader
	started  bool
	finished bool
}

func (iter *sstableIterator) HasNext() bool {
	if iter.finished {
		return false
	}

	if !iter.started {
		iter.started = true
		if err := iter.seekToStart(); err != nil {
			iter.finished = true
			return false
		}
		iter.reader = bufio.NewReader(iter.file)
	}

	return !iter.finished
}

func (iter *sstableIterator) Next() *KeyValue {
	if !iter.HasNext() {
		return nil
	}

	// Read length prefix
	var length uint32
	if err := binary.Read(iter.reader, binary.LittleEndian, &length); err != nil {
		iter.finished = true
		return nil
	}

	// Read key-value data
	kvData := make([]byte, length)
	if _, err := io.ReadFull(iter.reader, kvData); err != nil {
		iter.finished = true
		return nil
	}

	// Deserialize key-value
	kv, err := deserializeKeyValue(kvData)
	if err != nil {
		iter.finished = true
		return nil
	}

	// Check if we've passed the end key
	if iter.endKey != nil && bytes.Compare(kv.Key, iter.endKey) >= 0 {
		iter.finished = true
		return nil
	}

	return kv
}

func (iter *sstableIterator) Close() error {
	return iter.file.Close()
}

func (iter *sstableIterator) seekToStart() error {
	// Find starting position using index
	var seekOffset int64 = 1024 // Skip header

	if iter.startKey != nil {
		for i := len(iter.sstable.index.entries) - 1; i >= 0; i-- {
			entry := iter.sstable.index.entries[i]
			if bytes.Compare(iter.startKey, entry.Key) >= 0 {
				seekOffset = entry.Offset
				break
			}
		}
	}

	_, err := iter.file.Seek(seekOffset, 0)
	return err
}

// Serialization functions

func serializeKeyValue(kv *KeyValue) ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	// Write key length and key
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(kv.Key))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(kv.Key); err != nil {
		return nil, err
	}

	// Write value length and value
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(kv.Value))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(kv.Value); err != nil {
		return nil, err
	}

	// Write timestamp and deleted flag
	if err := binary.Write(buf, binary.LittleEndian, kv.Timestamp); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, kv.Deleted); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func deserializeKeyValue(data []byte) (*KeyValue, error) {
	buf := bytes.NewReader(data)

	// Read key
	var keyLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
		return nil, err
	}
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(buf, key); err != nil {
		return nil, err
	}

	// Read value
	var valueLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &valueLen); err != nil {
		return nil, err
	}
	value := make([]byte, valueLen)
	if _, err := io.ReadFull(buf, value); err != nil {
		return nil, err
	}

	// Read timestamp and deleted flag
	var timestamp int64
	var deleted bool
	if err := binary.Read(buf, binary.LittleEndian, &timestamp); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &deleted); err != nil {
		return nil, err
	}

	return &KeyValue{
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
		Deleted:   deleted,
	}, nil
}

func serializeHeader(header *SSTableHeader) ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	if err := binary.Write(buf, binary.LittleEndian, header.Magic); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, header.Version); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, header.CreatedAt); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, header.KeyCount); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, header.MinKeyLength); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, header.MaxKeyLength); err != nil {
		return nil, err
	}

	// Serialize bloom filter
	if err := binary.Write(buf, binary.LittleEndian, header.BloomFilter.Size); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, header.BloomFilter.Hashes); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, header.BloomFilter.Elements); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(header.BloomFilter.Data))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(header.BloomFilter.Data); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func serializeFooter(footer *SSTableFooter) ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	if err := binary.Write(buf, binary.LittleEndian, footer.IndexOffset); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, footer.IndexSize); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, footer.CRC32); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func serializeIndex(entries []IndexEntry) ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	// Write number of entries
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(entries))); err != nil {
		return nil, err
	}

	// Write each entry
	for _, entry := range entries {
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(entry.Key))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(entry.Key); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, entry.Offset); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func serializeBloomFilter(bf *BloomFilter) BloomFilterData {
	// Serialize bloom filter bits
	data := make([]byte, len(bf.bits)*8)
	for i, word := range bf.bits {
		binary.LittleEndian.PutUint64(data[i*8:], word)
	}

	return BloomFilterData{
		Size:     bf.size,
		Hashes:   uint32(bf.hashes),
		Elements: bf.elements,
		Data:     data,
	}
}