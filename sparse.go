package uewal

import (
	"encoding/binary"
	"os"
	"sort"
	"sync/atomic"

	"github.com/aasyanov/uewal/internal/crc"
)

// sparseEntry is a single entry in the per-segment sparse index.
// 24 bytes, all fields 8B-aligned for direct mmap/binary read.
type sparseEntry struct {
	FirstLSN  LSN   // batch FirstLSN
	Offset    int64 // byte offset in .wal file
	Timestamp int64 // batch timestamp (UnixNano)
}

const (
	sparseEntrySize = 24
	crcSize         = 4 // CRC-32C trailer size in bytes
)

// sparseIndex is a per-segment sparse index mapping batch FirstLSN/Timestamp
// to byte offset within the segment file. One entry per batch.
//
// Concurrency: the writer goroutine owns `entries` and appends freely.
// On publish(), a snapshot (slice header) is stored atomically so readers
// can search it without racing with append. If append causes a realloc,
// readers still hold the old backing array safely.
type sparseIndex struct {
	entries  []sparseEntry
	snapshot atomic.Pointer[[]sparseEntry] // reader-visible slice snapshot
}

const sparseIndexInitialCap = 256

// estimateSparseCapacity returns a reasonable pre-allocation capacity
// based on expected segment size. Assumes average batch frame ~4KB.
func estimateSparseCapacity(maxSegmentSize int64) int {
	const avgBatchFrame = 4096
	if maxSegmentSize <= 0 {
		return sparseIndexInitialCap
	}
	est := int(maxSegmentSize / avgBatchFrame)
	if est < sparseIndexInitialCap {
		return sparseIndexInitialCap
	}
	return est
}

func (si *sparseIndex) ensureCapacity(cap int) {
	if si.entries == nil {
		si.entries = make([]sparseEntry, 0, cap)
	}
}

func (si *sparseIndex) append(e sparseEntry) {
	si.entries = append(si.entries, e)
}

// publish makes all appended entries visible to concurrent readers.
// Must be called by the writer after a batch of appends.
// Stores a copy of the current slice header so readers see a frozen
// view even if a subsequent append reallocates the backing array.
func (si *sparseIndex) publish() {
	snap := si.entries[:len(si.entries):len(si.entries)]
	si.snapshot.Store(&snap)
}

func (si *sparseIndex) len() int {
	return len(si.entries)
}

// loadSnapshot returns the reader-visible entries slice.
// Safe for concurrent use with writer appends.
func (si *sparseIndex) loadSnapshot() []sparseEntry {
	p := si.snapshot.Load()
	if p == nil {
		return nil
	}
	return *p
}

// snapshotLen returns the number of entries visible to readers.
func (si *sparseIndex) snapshotLen() int {
	return len(si.loadSnapshot())
}

func (si *sparseIndex) reset() {
	si.entries = si.entries[:0]
	si.snapshot.Store(nil)
}

// adoptFrom takes ownership of entries from another index.
// Avoids struct copy that would trip the atomic copylocks check.
func (si *sparseIndex) adoptFrom(other *sparseIndex) {
	si.entries = other.entries
	si.publish()
}

// sparseSeek returns the starting byte offset for reading a segment from lsn.
// For sealed segments uses the full index; for active segments uses
// the atomically published snapshot to avoid racing with the writer.
func sparseSeek(si *sparseIndex, sealed bool, lsn LSN) int {
	var seekOff int64 = -1
	if sealed {
		if si.len() > 0 {
			seekOff = si.findByLSN(lsn)
		}
	} else {
		if si.snapshotLen() > 0 {
			seekOff = si.findByLSNSnapshot(lsn)
		}
	}
	if seekOff > 0 {
		return int(seekOff)
	}
	return 0
}

// findByLSN returns the byte offset of the batch containing lsn,
// or the closest batch before it. Returns -1 if no entry found.
// NOT safe for concurrent use with append -- use findByLSNSnapshot instead.
func (si *sparseIndex) findByLSN(lsn LSN) int64 {
	return si.findByLSNInRange(lsn, len(si.entries))
}

// findByLSNSnapshot is a concurrency-safe version of findByLSN.
// It reads the atomically published snapshot, safe to call while
// the writer appends to the same index.
func (si *sparseIndex) findByLSNSnapshot(lsn LSN) int64 {
	snap := si.loadSnapshot()
	return searchEntries(snap, lsn)
}

func (si *sparseIndex) findByLSNInRange(lsn LSN, n int) int64 {
	if n == 0 {
		return -1
	}
	return searchEntries(si.entries[:n], lsn)
}

func searchEntries(entries []sparseEntry, lsn LSN) int64 {
	n := len(entries)
	if n == 0 {
		return -1
	}
	i := sort.Search(n, func(i int) bool {
		return entries[i].FirstLSN > lsn
	})
	if i == 0 {
		return entries[0].Offset
	}
	return entries[i-1].Offset
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
	buf := make([]byte, dataLen+crcSize)
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
	if len(data) < crcSize {
		return &sparseIndex{}, nil
	}
	dataLen := len(data) - crcSize
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
	si := &sparseIndex{entries: entries}
	si.publish()
	return si, nil
}

// writeSparseIndex writes the index to an .idx file. Overwrites if exists.
func writeSparseIndex(path string, si *sparseIndex) error {
	return os.WriteFile(path, si.marshal(), defaultFileMode)
}

// readSparseIndex reads and validates an .idx file.
func readSparseIndex(path string) (*sparseIndex, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return unmarshalSparseIndex(data)
}
