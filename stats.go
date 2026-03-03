package uewal

import "sync/atomic"

// Stats holds a point-in-time snapshot of WAL runtime statistics.
//
// All cumulative counters (EventsWritten, BatchesWritten, etc.) are maintained
// via atomic operations and never decrease. The snapshot is consistent for
// each individual field but not across fields (no global lock).
type Stats struct {
	// EventsWritten is the total number of events the writer goroutine
	// has encoded and written to storage.
	EventsWritten uint64

	// BatchesWritten is the total number of write batches processed.
	// A single Append call produces one batch; group commit may combine
	// multiple batches into one write syscall, but the counter reflects
	// logical batches, not physical writes.
	BatchesWritten uint64

	// BytesWritten is the total number of encoded bytes written to storage.
	BytesWritten uint64

	// BytesSynced is the total number of bytes confirmed durable via fsync.
	BytesSynced uint64

	// SyncCount is the number of fsync calls performed by the writer.
	SyncCount uint64

	// Drops is the number of events silently dropped in DropMode when the
	// write queue was full.
	Drops uint64

	// CompressedBytes is the total number of bytes saved by compression.
	// Zero when no [Compressor] is configured.
	CompressedBytes uint64

	// Corruptions is the number of corruption events detected during
	// recovery or replay (truncated records, CRC mismatches).
	Corruptions uint64

	// QueueSize is the current number of pending batches in the write queue.
	// Zero when the WAL is closed.
	QueueSize int

	// FileSize is the current size of the storage file in bytes.
	// Zero when the WAL is closed.
	FileSize int64

	// LastLSN is the most recently persisted LSN (as seen by the writer).
	// This may lag behind the LSN returned by Append, which reflects
	// assignment, not persistence.
	LastLSN LSN

	// State is the current lifecycle state of the WAL.
	State State
}

// statsCollector holds atomic counters for lock-free stats collection.
// All methods are safe for concurrent use.
type statsCollector struct {
	eventsWritten   atomic.Uint64
	batchesWritten  atomic.Uint64
	bytesWritten    atomic.Uint64
	bytesSynced     atomic.Uint64
	syncCount       atomic.Uint64
	compressedBytes atomic.Uint64
	drops           atomic.Uint64
	corruptions     atomic.Uint64
	lastLSN         atomic.Uint64
}

func (sc *statsCollector) addEvents(n uint64)     { sc.eventsWritten.Add(n) }
func (sc *statsCollector) addBatches(n uint64)    { sc.batchesWritten.Add(n) }
func (sc *statsCollector) addBytes(n uint64)      { sc.bytesWritten.Add(n) }
func (sc *statsCollector) addSynced(n uint64)     { sc.bytesSynced.Add(n) }
func (sc *statsCollector) addSync()               { sc.syncCount.Add(1) }
func (sc *statsCollector) addCompressed(n uint64) { sc.compressedBytes.Add(n) }
func (sc *statsCollector) addDrop(n uint64)       { sc.drops.Add(n) }
func (sc *statsCollector) addCorruption()         { sc.corruptions.Add(1) }
func (sc *statsCollector) storeLSN(lsn LSN)       { sc.lastLSN.Store(lsn) }
func (sc *statsCollector) loadLSN() LSN           { return sc.lastLSN.Load() }

// snapshot captures the current state of all counters into a Stats value.
// queueSize, fileSize, and state are provided externally because they
// come from different subsystems (queue, storage, state machine).
func (sc *statsCollector) snapshot(queueSize int, fileSize int64, state State) Stats {
	return Stats{
		EventsWritten:  sc.eventsWritten.Load(),
		BatchesWritten: sc.batchesWritten.Load(),
		BytesWritten:   sc.bytesWritten.Load(),
		BytesSynced:    sc.bytesSynced.Load(),
		SyncCount:      sc.syncCount.Load(),
		CompressedBytes: sc.compressedBytes.Load(),
		Drops:          sc.drops.Load(),
		Corruptions:    sc.corruptions.Load(),
		QueueSize:      queueSize,
		FileSize:       fileSize,
		LastLSN:        sc.lastLSN.Load(),
		State:          state,
	}
}
