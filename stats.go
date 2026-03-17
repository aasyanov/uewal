package uewal

import "sync/atomic"

// Stats holds a point-in-time snapshot of WAL runtime statistics.
// Individual fields are consistent but cross-field consistency is not
// guaranteed (no global lock).
type Stats struct {
	EventsWritten     uint64
	BatchesWritten    uint64
	BytesWritten      uint64
	BytesSynced       uint64
	SyncCount         uint64
	Drops             uint64
	CompressedBytes   uint64
	Corruptions       uint64
	RotationCount     uint64 // number of segment rotations
	RetentionDeleted  uint64 // segments deleted by retention/compaction
	RetentionBytes    uint64 // bytes reclaimed by retention/compaction
	ImportBatches     uint64 // batch frames received via ImportBatch/ImportSegment
	ImportBytes       uint64 // bytes received via ImportBatch/ImportSegment
	LastSyncNano      int64  // UnixNano of last successful fsync (0 = never)
	QueueSize         int
	TotalSize         int64  // sum of all segment file sizes
	ActiveSegmentSize int64  // current active segment size
	SegmentCount      int
	FirstLSN          LSN
	LastLSN           LSN
	State             State
}

// statsCollector holds atomic counters for lock-free stats collection.
// Hot writer-goroutine fields are grouped first; reader-side fields follow.
type statsCollector struct {
	// Writer-hot counters (updated every batch).
	eventsWritten  atomic.Uint64
	batchesWritten atomic.Uint64
	bytesWritten   atomic.Uint64
	lastLSN        atomic.Uint64
	_padWriter     [32]byte //nolint:unused // separate writer-hot from infrequent

	// Infrequently updated.
	bytesSynced      atomic.Uint64
	syncCount        atomic.Uint64
	compressedBytes  atomic.Uint64
	drops            atomic.Uint64
	corruptions      atomic.Uint64
	firstLSN         atomic.Uint64
	rotationCount    atomic.Uint64
	retentionDeleted atomic.Uint64
	retentionBytes   atomic.Uint64
	importBatches    atomic.Uint64
	importBytes      atomic.Uint64
	lastSyncNano     atomic.Int64
}

func (sc *statsCollector) addEvents(n uint64)     { sc.eventsWritten.Add(n) }
func (sc *statsCollector) addBatches(n uint64)    { sc.batchesWritten.Add(n) }
func (sc *statsCollector) addBytes(n uint64)      { sc.bytesWritten.Add(n) }
func (sc *statsCollector) addSynced(n uint64)     { sc.bytesSynced.Add(n) }
func (sc *statsCollector) addSync()               { sc.syncCount.Add(1) }
func (sc *statsCollector) addCompressed(n uint64) { sc.compressedBytes.Add(n) }
func (sc *statsCollector) addDrop(n uint64)       { sc.drops.Add(n) }
func (sc *statsCollector) addCorruption()                     { sc.corruptions.Add(1) }
func (sc *statsCollector) addRotation()                       { sc.rotationCount.Add(1) }
func (sc *statsCollector) addRetentionDeleted(segs, bytes uint64) {
	sc.retentionDeleted.Add(segs)
	sc.retentionBytes.Add(bytes)
}
func (sc *statsCollector) addImport(batches, bytes uint64) {
	sc.importBatches.Add(batches)
	sc.importBytes.Add(bytes)
}
func (sc *statsCollector) storeLastSync(nano int64) { sc.lastSyncNano.Store(nano) }
func (sc *statsCollector) storeLSN(lsn LSN)         { sc.lastLSN.Store(lsn) }
func (sc *statsCollector) loadLSN() LSN             { return sc.lastLSN.Load() }
func (sc *statsCollector) storeFirstLSN(lsn LSN) {
	sc.firstLSN.CompareAndSwap(0, lsn)
}

func (sc *statsCollector) snapshot(queueSize int, totalSize, activeSize int64, segCount int, state State) Stats {
	return Stats{
		EventsWritten:    sc.eventsWritten.Load(),
		BatchesWritten:   sc.batchesWritten.Load(),
		BytesWritten:     sc.bytesWritten.Load(),
		BytesSynced:      sc.bytesSynced.Load(),
		SyncCount:        sc.syncCount.Load(),
		CompressedBytes:  sc.compressedBytes.Load(),
		Drops:            sc.drops.Load(),
		Corruptions:      sc.corruptions.Load(),
		RotationCount:    sc.rotationCount.Load(),
		RetentionDeleted: sc.retentionDeleted.Load(),
		RetentionBytes:   sc.retentionBytes.Load(),
		ImportBatches:    sc.importBatches.Load(),
		ImportBytes:      sc.importBytes.Load(),
		LastSyncNano:     sc.lastSyncNano.Load(),
		QueueSize:        queueSize,
		TotalSize:        totalSize,
		ActiveSegmentSize: activeSize,
		SegmentCount:     segCount,
		FirstLSN:         sc.firstLSN.Load(),
		LastLSN:          sc.lastLSN.Load(),
		State:            state,
	}
}
