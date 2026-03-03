// Package uewal provides a production-grade, embedded Write-Ahead Log (WAL)
// for Go 1.21+ applications.
//
// UEWAL is a strict, minimalist, and high-performance WAL engine designed for
// event sourcing, state machine recovery, embedded database logs, durable queues,
// snapshot + replay, audit logging, and high-frequency buffering.
//
// # Architecture
//
// The WAL is append-only, single-process, and uses a single-writer goroutine.
// Events submitted via [WAL.Append] or [WAL.AppendBatch] are assigned monotonic
// [LSN] values and enqueued into a bounded write queue. The writer goroutine
// drains the queue, encodes events into batch frames with CRC-32C integrity,
// and writes them to the underlying [Storage].
//
// Group commit is performed automatically: the writer drains all immediately
// available batches before issuing a single write system call, amortizing
// per-event overhead.
//
// Replay uses memory-mapped I/O ([WAL.Replay], [WAL.Iterator]) for zero-copy
// sequential reads. On platforms where mmap is unavailable or when a custom
// [Storage] is used, a fallback reader based on [Storage.ReadAt] is employed.
//
// # Lifecycle
//
// A WAL progresses through four states:
//
//	INIT → RUNNING → DRAINING → CLOSED
//
// [Open] creates and transitions the WAL to RUNNING. [WAL.Shutdown] performs
// a graceful shutdown: it stops accepting new appends, drains the queue,
// flushes remaining data, optionally syncs, and closes storage. [WAL.Close]
// performs an immediate close without draining.
//
// # Durability
//
// Durability is controlled via [SyncMode]:
//   - [SyncNever]: no explicit fsync; relies on OS page cache.
//   - [SyncBatch]: fsync after every write batch (strongest guarantee).
//   - [SyncInterval]: fsync at a configurable time interval.
//
// [WAL.Flush] waits for the writer to process all pending queue items and write
// them to storage. [WAL.Sync] issues an fsync to make written data durable.
// These are distinct operations: Flush ensures data reaches storage; Sync
// ensures it survives a power failure.
//
// # Backpressure
//
// When the write queue is full, behavior is controlled by [BackpressureMode]:
//   - [BlockMode]: caller blocks until space is available (default).
//   - [DropMode]: events are silently dropped; [Stats.Drops] is incremented.
//   - [ErrorMode]: [ErrQueueFull] is returned immediately.
//
// # Event Metadata
//
// Each [Event] has an optional [Event.Meta] field for application-level
// metadata (event type, aggregate ID, tags, etc.). The WAL stores Meta
// alongside the Payload but never interprets its contents. When Meta is nil,
// zero bytes are written (MetaLen=0).
//
// # Compression
//
// An optional [Compressor] can be configured via [WithCompressor]. When set,
// the records region of each batch frame is compressed before writing, and
// the compressed flag is stored in the batch header. Decompression is
// transparent during replay. The WAL is not responsible for [Compressor]
// buffer management — implementations should use sync.Pool internally if
// needed.
//
// # Indexing
//
// An optional [Indexer] can be configured via [WithIndex]. The [Indexer.OnAppend]
// method is called from the writer goroutine after each event is persisted,
// providing the LSN, Meta, and file offset. Panics in OnAppend are recovered
// and silently discarded.
//
// # Observability
//
// Runtime metrics are available via [WAL.Stats] using lock-free atomic counters.
// Lifecycle and pipeline events can be observed through [Hooks], which are
// invoked with panic recovery to ensure WAL consistency.
//
// # Storage
//
// The [Storage] interface allows custom persistence backends. The default
// implementation, [FileStorage], uses [os.File] with file-locking (flock/LockFileEx)
// to prevent concurrent access from multiple WAL instances.
//
// # Batch Frame Format (v2)
//
// Events are encoded as batch frames with a single CRC-32C per batch,
// providing true batch atomicity:
//
//	┌──────────────────────────────────────────────────┐
//	│ Magic        4 bytes   "UWAL"                    │
//	│ Version      2 bytes   (2)                       │
//	│ Flags        2 bytes   (bit 0 = compressed)      │
//	│ RecordCount  4 bytes                             │
//	│ FirstLSN     8 bytes                             │
//	│ BatchSize    4 bytes   (total frame incl. CRC)   │
//	├──────────────────────────────────────────────────┤
//	│ Records region (possibly compressed):            │
//	│   Record 0:                                      │
//	│     PayloadLen  4 bytes                          │
//	│     MetaLen     2 bytes                          │
//	│     Meta        MetaLen bytes                    │
//	│     Payload     PayloadLen bytes                 │
//	│   Record 1: ...                                  │
//	├──────────────────────────────────────────────────┤
//	│ CRC32C       4 bytes   (covers Magic..records)   │
//	└──────────────────────────────────────────────────┘
//
// Batch header: 24 bytes. Per-record overhead: 6 bytes (PayloadLen + MetaLen).
// Batch overhead: 28 bytes (header + CRC trailer).
// CRC covers all bytes from Magic through the last record byte, exclusive of
// the CRC field itself. When compression is enabled, CRC covers compressed data.
//
// # Crash Recovery
//
// On [Open], the WAL scans existing batch frames to recover the last valid [LSN].
// If corruption is detected (CRC mismatch or truncated frame), the file is
// automatically truncated to the last valid batch boundary. This ensures the WAL
// is always in a consistent state after a crash. A corrupted batch is entirely
// discarded (true batch atomicity).
//
// # Thread Safety
//
// All public methods on [WAL] are safe for concurrent use. The internal
// architecture guarantees that only a single goroutine writes to storage,
// while [LSN] assignment uses atomic operations for lock-free contention.
//
// # Deviations from Specification
//
// [WAL.Iterator] returns (*[Iterator], error) rather than *[Iterator] alone.
// This allows callers to distinguish initialization errors (e.g., storage
// failures) from an empty WAL, which is a safer API contract.
//
// File rotation (OnRotate hook, Rotations stat, MaxFileSize option) is
// intentionally omitted. The specification included these as forward-looking
// placeholders, but no rotation logic was specified. They have been removed
// to maintain architectural purity (no dead code).
//
// # Basic Usage
//
//	w, err := uewal.Open("/path/to/wal")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer w.Shutdown(context.Background())
//
//	lsn, err := w.Append(uewal.Event{Payload: []byte("hello")})
//
//	err = w.Replay(0, func(e uewal.Event) error {
//	    fmt.Println(string(e.Payload))
//	    return nil
//	})
package uewal
