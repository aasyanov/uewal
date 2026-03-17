# UEWAL — Ultra-fast Embedded Write-Ahead Log

[![CI](https://github.com/aasyanov/uewal/actions/workflows/ci.yml/badge.svg)](https://github.com/aasyanov/uewal/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/aasyanov/uewal.svg)](https://pkg.go.dev/github.com/aasyanov/uewal)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Ready-to-use embedded WAL for Go 1.24+. Zero external dependencies.

```
go get github.com/aasyanov/uewal
```

## Overview

UEWAL is a strict, minimalist, high-performance Write-Ahead Log engine for single-process Go applications. Designed for event sourcing, state machine recovery, embedded database logs, durable queues, audit logging, replication, and high-frequency buffering.

**Not** a distributed log, server, cloud backend, or multi-process shared WAL.

## Architecture

```
Write()  ──► lsnCounter (atomic) ──► writeQueue ──► writer goroutine ──► Storage
                                        ▲                  │
                                   Flush barrier        group commit
                                                    encodeBatch + CRC32C
                                                    compress (optional)
                                                    write + maybeSync
                                                    indexer.OnAppend
                                                    segment rotation
```

- **Append-only**, single-writer goroutine
- **Multi-segment** with automatic rotation by size, age, and count
- **Lock-free** LSN assignment via `atomic.Uint64`
- **Group commit**: writer drains all available batches before issuing a single write
- **Batch-framed format** with single CRC-32C per batch (true batch atomicity)
- **Zero-copy replay** via mmap (with `ReadAt` fallback for custom Storage)
- **Sparse index** per segment for O(log n) LSN/timestamp lookup
- **Manifest-based recovery** for fast startup (falls back to full scan)
- **Live tail** via `Follow()` iterator with blocking `Next()` and auto segment crossing
- **Replication** via `ImportBatch()` / `ImportSegment()` / `OpenSegment()`
- **Pluggable** compression (`Compressor`), indexing (`Indexer`), and storage (`StorageFactory`)
- **0 allocations per Write** (`sync.Pool` for record slices), 0 on encode, 1 on decode

## Quick Start

```go
w, err := uewal.Open("/path/to/wal",
    uewal.WithSyncMode(uewal.SyncBatch),
    uewal.WithMaxSegmentSize(256 << 20),
)
if err != nil {
    log.Fatal(err)
}
defer w.Shutdown(context.Background())

// Single event
batch := uewal.NewBatch(1)
batch.Append([]byte("hello"), nil, nil)
lsn, err := w.Write(batch)

// Event with key and metadata
batch.Reset()
batch.Append([]byte("user_created"), []byte("user-123"), []byte("created"))
lsn, err = w.Write(batch)

// Batch write (atomic, one CRC)
payload := []byte("event-data")
batch = uewal.NewBatch(100)
for i := 0; i < 100; i++ {
    batch.Append(payload, nil, nil)
}
lsn, err = w.Write(batch)

// Replay (zero-copy via mmap)
err = w.Replay(0, func(ev uewal.Event) error {
    fmt.Printf("LSN=%d Key=%s Payload=%s\n", ev.LSN, ev.Key, ev.Payload)
    return nil
})
```

## Public API

| Method | Description |
|---|---|
| `Open(path, opts...)` | Create or open a WAL |
| `Write(batch)` | Write a batch atomically (copies records) |
| `WriteUnsafe(batch)` | Write without copy (caller must not reuse batch until `Flush`) |
| `Flush()` | Wait for writer to process all queued batches |
| `Sync()` | fsync the active segment |
| `WaitDurable(lsn)` | Block until the given LSN is fsynced |
| `Replay(from, fn)` | Callback-based read (zero-copy via mmap) |
| `ReplayRange(from, to, fn)` | Bounded replay within an LSN range |
| `ReplayBatches(from, fn)` | Batch-level replay callback |
| `Follow(from)` | Live tail-follow iterator with blocking `Next()` |
| `Iterator(from)` | Pull-based read iterator (zero-copy via mmap) |
| `Snapshot(fn)` | Consistent read snapshot during concurrent writes |
| `Rotate()` | Manual segment rotation |
| `Segments()` | Returns `[]SegmentInfo` for all segments |
| `DeleteBefore(lsn)` | Delete segments before LSN |
| `DeleteOlderThan(ts)` | Delete segments older than timestamp |
| `OpenSegment(firstLSN)` | Open a sealed segment for raw reading (replication) |
| `ImportBatch(frame)` | Import a raw batch frame from a primary |
| `ImportSegment(path)` | Import a sealed segment file from a primary |
| `FirstLSN()` / `LastLSN()` | First and last LSN accessors |
| `Dir()` | Returns the WAL directory path |
| `State()` | Returns the current lifecycle state |
| `Stats()` | Lock-free runtime statistics snapshot |
| `Shutdown(ctx)` | Graceful shutdown with context deadline |
| `Close()` | Immediate close without draining |

## Configuration

All configuration is via functional options passed to `Open`:

```go
w, err := uewal.Open(path,
    uewal.WithSyncMode(uewal.SyncBatch),           // fsync after every write
    uewal.WithSyncInterval(50*time.Millisecond),    // for SyncInterval mode
    uewal.WithSyncCount(10),                        // fsync every 10 batches
    uewal.WithSyncSize(1 << 20),                    // fsync every 1 MB
    uewal.WithBackpressure(uewal.BlockMode),        // block when queue full
    uewal.WithQueueSize(4096),                      // write queue capacity
    uewal.WithBufferSize(64*1024),                  // encoder buffer size
    uewal.WithMaxSegmentSize(256 << 20),            // segment rotation at 256 MB
    uewal.WithMaxSegmentAge(time.Hour),             // segment rotation by age
    uewal.WithMaxSegments(100),                     // max segments before retention
    uewal.WithMaxBatchSize(4 << 20),                // reject batches > 4 MB
    uewal.WithRetentionSize(10 << 30),              // auto-delete when WAL exceeds 10 GB
    uewal.WithRetentionAge(24*time.Hour),           // auto-delete segments older than 24h
    uewal.WithPreallocate(true),                    // preallocate segment files
    uewal.WithStorageFactory(factory),              // custom Storage backend
    uewal.WithCompressor(zstdCompressor),           // optional compression
    uewal.WithIndex(myIndexer),                     // optional indexer
    uewal.WithHooks(hooks),                         // observability callbacks
    uewal.WithStartLSN(1000),                       // initial LSN for fresh WALs
)
```

### Durability Modes

| Mode | Behavior | Data Loss Window |
|---|---|---|
| `SyncNever` | No fsync, OS page cache only | All unsynced data |
| `SyncBatch` | fsync after every write batch | None |
| `SyncInterval` | fsync at configurable interval | Up to one interval |
| `SyncCount` | fsync after every N batches | Up to N-1 batches |
| `SyncSize` | fsync after every N bytes | Up to N-1 bytes |

### Backpressure Modes

| Mode | Behavior |
|---|---|
| `BlockMode` | Caller blocks until queue has space (default) |
| `DropMode` | Returns LSN 0, fires `Hooks.OnDrop`, increments `Stats.Drops` |
| `ErrorMode` | Returns `ErrQueueFull` immediately |

## Lifecycle

```
StateInit ──► StateRunning ──► StateDraining ──► StateClosed
   Open()       Write/Replay      Shutdown()        Done
```

- `Shutdown(ctx)`: graceful — drains queue, flushes, syncs, closes storage. Respects context cancellation. Idempotent.
- `Close()`: immediate — stops writer, closes storage, discards pending data. Idempotent.

## Batch Frame Format (v1)

```
Header (28 bytes):
  Magic(4) Version(1) Flags(1) Count(2)
  FirstLSN(8) Timestamp(8) BatchSize(4)

Per-record (8-16 bytes overhead + data):
  [Timestamp(8)]  — only if flagPerRecordTS
  KeyLen(2) MetaLen(2) PayloadLen(4)
  Key Meta Payload

Trailer: CRC32C(4) over entire frame
```

- CRC-32C (Castagnoli) with hardware acceleration (SSE4.2 / ARM CRC)
- Little-endian encoding, 4-byte magic "EWAL" for frame detection
- **True batch atomicity**: single CRC covers entire batch; on recovery, either all events in a frame are valid or the entire frame is discarded
- **Uniform-timestamp optimization**: when all records share the same timestamp, per-record timestamps are omitted (saves 8 bytes per record)
- **Compression**: when `Compressor` is set, the records region is compressed; CRC covers compressed bytes

## Compressor Interface

```go
type Compressor interface {
    Compress(src []byte) ([]byte, error)
    Decompress(src []byte) ([]byte, error)
}
```

The WAL calls `Compress` from the writer goroutine and `Decompress` during replay. Typical implementations wrap zstd, lz4, or snappy.

`ScratchCompressor` extends `Compressor` with `CompressTo(dst, src)` / `DecompressTo(dst, src)` for zero-allocation buffer reuse on the hot path.

## Indexer Interface

```go
type Indexer interface {
    OnAppend(info IndexInfo)
}
```

Called from the writer goroutine after each event is persisted. Panics are recovered. The indexer is not notified of segment deletions; use `Hooks.OnDelete` to maintain external index consistency.

`IndexInfo` contains: `LSN`, `Timestamp`, `Key`, `Meta`, `Offset`, `Segment`.

## Storage Interface

```go
type Storage interface {
    Write(p []byte) (n int, err error)
    Sync() error
    Close() error
    Size() (int64, error)
    Truncate(size int64) error
    ReadAt(p []byte, off int64) (n int, err error)
}
```

The default `FileStorage` uses `os.File` with flock/LockFileEx to prevent concurrent access. Custom implementations can back the WAL with any persistence layer.

Performance hint: if the implementation also satisfies `WriteNoLock()` and `SyncNoLock()`, the writer goroutine bypasses the mutex for higher throughput.

## Crash Recovery

On `Open`, the WAL reads the manifest for fast segment recovery. If the manifest is absent, a full directory scan is performed as fallback. The active segment is validated via mmap — corrupted or truncated batch frames are discarded at the last valid batch boundary. Orphan segment files (from interrupted `ImportSegment` calls) are cleaned up automatically. Recovery memory usage is O(1) independent of WAL size.

## Replication

```go
// Primary: export sealed segments
for _, seg := range primary.Segments() {
    if seg.Sealed {
        rc, info, _ := primary.OpenSegment(seg.FirstLSN)
        data, _ := io.ReadAll(rc)
        rc.Close()
        // ship data to replica
    }
}

// Replica: import segments in LSN order
err := replica.ImportSegment("/path/to/shipped/segment.wal")
```

`ImportBatch` imports individual batch frames; `ImportSegment` imports entire sealed segments. Both validate CRC integrity. Callers must import in LSN order to avoid duplicate events.

## Observability

### Hooks

```go
uewal.WithHooks(uewal.Hooks{
    OnStart:         func() { ... },
    OnShutdownStart: func() { ... },
    OnShutdownDone:  func(elapsed time.Duration) { ... },
    AfterAppend:     func(firstLSN, lastLSN uewal.LSN, count int) { ... },
    BeforeWrite:     func(bytes int) { ... },
    AfterWrite:      func(bytes int, elapsed time.Duration) { ... },
    BeforeSync:      func() { ... },
    AfterSync:       func(bytes int, elapsed time.Duration) { ... },
    OnCorruption:    func(segmentPath string, offset int64) { ... },
    OnDrop:          func(count int) { ... },
    OnError:         func(err error) { ... },
    OnRecovery:      func(info uewal.RecoveryInfo) { ... },
    OnImport:        func(firstLSN, lastLSN uewal.LSN, bytes int) { ... },
    OnRotation:      func(sealed uewal.SegmentInfo) { ... },
    OnDelete:        func(deleted uewal.SegmentInfo) { ... },
})
```

All 15 hooks are optional, panic-safe, and never affect WAL consistency.

### Stats

```go
s := w.Stats()
// s.EventsWritten, s.BatchesWritten, s.BytesWritten, s.BytesSynced,
// s.SyncCount, s.CompressedBytes, s.Drops, s.Corruptions,
// s.RotationCount, s.RetentionDeleted, s.RetentionBytes,
// s.ImportBatches, s.ImportBytes, s.LastSyncNano,
// s.QueueSize, s.TotalSize, s.ActiveSegmentSize, s.SegmentCount,
// s.FirstLSN, s.LastLSN, s.State
```

All 21 fields are lock-free (atomic). Safe to call in any state, including after `Close`.

## Errors

26 sentinel errors, all comparable with `==` and `errors.Is`:

| Error | Returned by |
|---|---|
| `ErrClosed` | Any operation on a closed WAL |
| `ErrDraining` | `Write` during graceful shutdown |
| `ErrNotRunning` | Operations requiring `StateRunning` |
| `ErrQueueFull` | `Write` in `ErrorMode` |
| `ErrEmptyBatch` | `Write` with zero events |
| `ErrBatchTooLarge` | Batch exceeds `MaxBatchSize` |
| `ErrShortWrite` | Storage returns n < len(p) without error |
| `ErrCorrupted` | Data corruption detected |
| `ErrCRCMismatch` | CRC-32C validation failure |
| `ErrInvalidRecord` | Truncated/unsupported record header |
| `ErrInvalidLSN` | Invalid LSN argument |
| `ErrLSNOutOfRange` | LSN out of range |
| `ErrInvalidState` | Illegal lifecycle transition |
| `ErrCompressorRequired` | Compressed data without `Compressor` |
| `ErrDecompress` | Decompression failure |
| `ErrDirectoryLocked` | Directory is locked by another instance |
| `ErrCreateDir` | Directory creation failed |
| `ErrLockFile` | Lock file open failed |
| `ErrSync` | fsync failed |
| `ErrMmap` | mmap failed |
| `ErrSegmentNotFound` | Segment not found or not sealed |
| `ErrCreateSegment` | Segment creation failed |
| `ErrSealSegment` | Segment seal failed |
| `ErrScanDir` | Directory scan failed |
| `ErrManifestTruncated` / `ErrManifestVersion` / `ErrManifestWrite` | Manifest errors |
| `ErrImportInvalid` / `ErrImportRead` / `ErrImportWrite` | Import errors |
| `ErrWriterPanic` | Writer goroutine panicked (user Compressor/Indexer) |

## License

MIT
