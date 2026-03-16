package uewal

import (
	"encoding/binary"
	"os"
	"sort"

	"github.com/aasyanov/uewal/internal/crc"
)

// sparseEntry is a single entry in the per-segment sparse index.
// 24 bytes, all fields 8B-aligned for direct mmap/binary read.
type sparseEntry struct {
	FirstLSN  LSN   // batch FirstLSN
	Offset    int64 // byte offset in .wal file
	Timestamp int64 // batch timestamp (UnixNano)
}

const sparseEntrySize = 24

// sparseIndex is a per-segment sparse index mapping batch FirstLSN/Timestamp
// to byte offset within the segment file. One entry per batch.
type sparseIndex struct {
	entries []sparseEntry
}

func (si *sparseIndex) append(e sparseEntry) {
	si.entries = append(si.entries, e)
}

func (si *sparseIndex) len() int {
	return len(si.entries)
}

func (si *sparseIndex) reset() {
	si.entries = si.entries[:0]
}

// findByLSN returns the byte offset of the batch containing lsn,
// or the closest batch before it. Returns -1 if no entry found.
func (si *sparseIndex) findByLSN(lsn LSN) int64 {
	if len(si.entries) == 0 {
		return -1
	}
	i := sort.Search(len(si.entries), func(i int) bool {
		return si.entries[i].FirstLSN > lsn
	})
	if i == 0 {
		return si.entries[0].Offset
	}
	return si.entries[i-1].Offset
}

// findByTimestamp returns the byte offset of the first batch with
// timestamp >= ts. Returns -1 if no entry found.
func (si *sparseIndex) findByTimestamp(ts int64) int64 {
	if len(si.entries) == 0 {
		return -1
	}
	i := sort.Search(len(si.entries), func(i int) bool {
		return si.entries[i].Timestamp >= ts
	})
	if i >= len(si.entries) {
		return -1
	}
	return si.entries[i].Offset
}

// lastLSN returns the FirstLSN of the last entry, or 0 if empty.
func (si *sparseIndex) lastLSN() LSN {
	if len(si.entries) == 0 {
		return 0
	}
	return si.entries[len(si.entries)-1].FirstLSN
}

// lastTimestamp returns the timestamp of the last entry, or 0.
func (si *sparseIndex) lastTimestamp() int64 {
	if len(si.entries) == 0 {
		return 0
	}
	return si.entries[len(si.entries)-1].Timestamp
}

// firstTimestamp returns the timestamp of the first entry, or 0.
func (si *sparseIndex) firstTimestamp() int64 {
	if len(si.entries) == 0 {
		return 0
	}
	return si.entries[0].Timestamp
}

// marshal serializes the index to binary with a trailing CRC-32C.
// Format: [entries...][CRC32C 4B]
func (si *sparseIndex) marshal() []byte {
	dataLen := len(si.entries) * sparseEntrySize
	buf := make([]byte, dataLen+4)
	for i, e := range si.entries {
		off := i * sparseEntrySize
		binary.LittleEndian.PutUint64(buf[off:], e.FirstLSN)
		binary.LittleEndian.PutUint64(buf[off+8:], uint64(e.Offset))
		binary.LittleEndian.PutUint64(buf[off+16:], uint64(e.Timestamp))
	}
	checksum := crc.Checksum(buf[:dataLen])
	binary.LittleEndian.PutUint32(buf[dataLen:], checksum)
	return buf
}

// unmarshalSparseIndex deserializes a sparse index from binary data with CRC check.
func unmarshalSparseIndex(data []byte) (*sparseIndex, error) {
	if len(data) < 4 {
		return &sparseIndex{}, nil
	}
	dataLen := len(data) - 4
	if dataLen%sparseEntrySize != 0 {
		return nil, ErrInvalidRecord
	}

	storedCRC := binary.LittleEndian.Uint32(data[dataLen:])
	computedCRC := crc.Checksum(data[:dataLen])
	if storedCRC != computedCRC {
		return nil, ErrCRCMismatch
	}

	count := dataLen / sparseEntrySize
	entries := make([]sparseEntry, count)
	for i := range entries {
		off := i * sparseEntrySize
		entries[i] = sparseEntry{
			FirstLSN:  binary.LittleEndian.Uint64(data[off:]),
			Offset:    int64(binary.LittleEndian.Uint64(data[off+8:])),
			Timestamp: int64(binary.LittleEndian.Uint64(data[off+16:])),
		}
	}
	return &sparseIndex{entries: entries}, nil
}

// writeSparseIndex writes the index to an .idx file. Overwrites if exists.
func writeSparseIndex(path string, si *sparseIndex) error {
	return os.WriteFile(path, si.marshal(), 0644)
}

// readSparseIndex reads and validates an .idx file.
func readSparseIndex(path string) (*sparseIndex, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return unmarshalSparseIndex(data)
}
