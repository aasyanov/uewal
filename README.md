# uewal — Embedded Write-Ahead Log for Go

[![CI](https://github.com/aasyanov/uewal/actions/workflows/ci.yml/badge.svg)](https://github.com/aasyanov/uewal/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/aasyanov/uewal.svg)](https://pkg.go.dev/github.com/aasyanov/uewal)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Segmented write-ahead log engine for Go 1.24+. Zero external dependencies.

```
go get github.com/aasyanov/uewal
```

## The Problem

Any system that needs crash recovery — databases, event stores, message brokers, state machines — must persist state changes before acknowledging them. The standard approach is a write-ahead log: write the change to a durable log first, acknowledge, then apply lazily.

The implementation challenge is balancing four competing concerns: write latency (the caller blocks until the write is persisted), throughput (how many writes per second the log can sustain), recovery speed (how fast the system restarts after a crash), and operational complexity (segment management, retention, replication).

uewal addresses all four. Writes are non-blocking — a lock-free LSN counter and a bounded MPSC queue decouple producers from the single writer goroutine, which batches multiple writes into a single syscall (group commit). Recovery is O(1) memory via mmap and manifest-based segment loading. Segments rotate automatically by size, age, or count, with configurable retention policies. Sealed segments can be shipped to replicas.

## Architecture

```
Write()  ──► lsnCounter (atomic) ──► writeQueue (MPSC) ──► writer goroutine ──► Storage
                                         ▲                       │
                                    Flush barrier           group commit
                                                       encodeBatch + CRC32C
                                                       compress (optional)
                                                       write + maybeSync
                                                       indexer.OnAppend
                                                       segment rotation
```

1. `Write` assigns LSNs via a single `atomic.Add` — no lock, no contention.
2. The batch is enqueued into a bounded MPSC queue. If the queue is full, the caller blocks (`BlockMode`), gets an error (`ErrorMode`), or the batch is silently dropped (`DropMode`).
3. The writer goroutine drains all available batches, encodes them into a single contiguous buffer with CRC-32C, and issues one `write` syscall (group commit).
4. After writing, the goroutine optionally fsyncs (depending on `SyncMode`), notifies the indexer, checks rotation thresholds, and signals `Flush`/`WaitDurable` barriers.

The pipeline is append-only. There is exactly one writer goroutine — no concurrent writes to storage, no write-write locks. Reads (replay, iterator, follow) use mmap and never block writes.

## How It Works

### Segments

The WAL is a sequence of immutable sealed segments and one active segment. Each segment is a flat file of concatenated batch frames. When the active segment exceeds the size threshold (`WithMaxSegmentSize`), age threshold (`WithMaxSegmentAge`), or the segment count limit (`WithMaxSegments`) is reached, the writer seals the current segment and creates a new one.

```
wal-directory/
├── manifest.bin                     ← binary manifest for fast recovery
├── 00000000000000000001.wal         ← sealed segment (firstLSN=1)
├── 00000000000000000001.idx         ← sparse index for segment 1
├── 00000000000000010001.wal         ← sealed segment (firstLSN=10001)
├── 00000000000000010001.idx         ← sparse index for segment 10001
├── 00000000000000020001.wal         ← active segment (firstLSN=20001)
└── LOCK                             ← flock / LockFileEx
```

Segment files are named by their first LSN, zero-padded to 20 digits. The manifest stores metadata (firstLSN, lastLSN, timestamps, size, sealed flag) for all segments, enabling O(1) recovery without scanning `.wal` files.

### Batch Frame Format (v1)

```
Header (28 bytes):
  Magic(4)       "EWAL"
  Version(1)     0x01
  Flags(1)       flagCompressed(1<<0), flagPerRecordTS(1<<1)
  Count(2)       number of records
  FirstLSN(8)    first LSN in batch
  Timestamp(8)   batch-level nanosecond timestamp
  BatchSize(4)   total frame size including header and trailer

Per-record (8–16 bytes overhead + data):
  [Timestamp(8)] — present only if flagPerRecordTS is set
  KeyLen(2) MetaLen(2) PayloadLen(4)
  Key Meta Payload

Trailer: CRC32C(4) over entire frame (header + records region)
```

A single CRC-32C covers the entire batch. On recovery, either all events in a frame are valid or the entire frame is discarded — true batch atomicity. No partial batch can be read.

**Uniform-timestamp optimization**: when all records in a batch share the same timestamp, the `flagPerRecordTS` flag is cleared and per-record timestamp fields are omitted, saving 8 bytes per record.

**Compression**: when a `Compressor` is configured, the records region (not the header) is compressed. The CRC covers the compressed bytes. The header's `flagCompressed` bit is set.

### Sparse Index

Each sealed segment has a companion `.idx` file — a sorted array of `(LSN, Timestamp, Offset)` tuples. When replaying from a specific LSN, `findByLSN` performs a binary search on the index to find the starting file offset, skipping the scan of earlier records. This turns point lookups on large segments from O(n) to O(log k) where k is the number of index entries.

The index is written atomically (tmp + fsync + rename) during segment rotation.

### Recovery

On `Open`, the WAL reads `manifest.bin` for segment metadata. If the manifest is absent (first run, or crash before manifest write), a full directory scan is performed as fallback. The active segment is validated via mmap — `scanBatchFrame` walks the file, verifying CRC-32C for each batch. Corrupted or truncated frames at the tail are discarded. Orphan `.wal`, `.idx`, and `.tmp` files not listed in the manifest are cleaned up.

Recovery memory usage is O(1): the active segment is scanned via mmap (no heap allocation), and sealed segments are loaded from manifest metadata without reading their contents.

### Replication

Sealed segments can be shipped to replicas via `OpenSegment` (returns an `io.ReadCloser` for raw reading) and `ImportSegment` (imports a segment file with CRC validation). Individual batch frames can be shipped via `ImportBatch`. Both import methods validate CRC integrity. Callers must import in LSN order.

```go
// Primary: export sealed segments
for _, seg := range primary.Segments() {
    if seg.Sealed {
        rc, _, _ := primary.OpenSegment(seg.FirstLSN)
        data, _ := io.ReadAll(rc)
        rc.Close()
        // ship data to replica
    }
}

// Replica: import
replica.ImportSegment("/path/to/shipped/segment.wal")
```

### Live Tail

`Follow(from)` returns an iterator that blocks on `Next()` when it reaches the end of available data, automatically crossing segment boundaries. When new data is written, the iterator wakes up and delivers the next event. This is the building block for CDC (Change Data Capture) and real-time replication consumers.

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
batch.Append([]byte("user_created"), []byte("user-123"), []byte("v1"))
lsn, err = w.Write(batch)

// Batch write (atomic, single CRC)
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

## API

### WAL

| Method | Description |
|---|---|
| `Open(dir, opts...)` | Create or open a WAL, acquire directory-level file lock |
| `Write(batch)` | Write a batch atomically (copies payload/key/meta into internal buffer) |
| `WriteUnsafe(batch)` | Write without copy — caller must not modify batch slices until `Flush` returns |
| `Flush()` | Block until the writer goroutine has processed all queued batches |
| `Sync()` | Force fsync of the active segment regardless of SyncMode |
| `WaitDurable(lsn)` | Block until the given LSN has been fsynced to disk |
| `Replay(from, fn)` | Iterate all events with LSN ≥ from via callback (zero-copy via mmap) |
| `ReplayRange(from, to, fn)` | Iterate events in [from, to) LSN range |
| `ReplayBatches(from, fn)` | Iterate batch frames — callback receives `[]Event` per batch |
| `Iterator(from)` | Pull-based cross-segment iterator, returns `(*Iterator, error)` |
| `Follow(from)` | Tail-follow iterator — `Next()` blocks when no new data is available |
| `Snapshot(fn)` | Run callback with `SnapshotController` for consistent reads + compaction |
| `Rotate()` | Force segment rotation (seal active, create new) |
| `Segments()` | Returns `[]SegmentInfo` with metadata for all segments |
| `DeleteBefore(lsn)` | Delete sealed segments whose LastLSN < lsn |
| `DeleteOlderThan(ts)` | Delete sealed segments whose LastTimestamp < ts |
| `OpenSegment(firstLSN)` | Open a sealed segment as `io.ReadCloser` for raw reading |
| `ImportBatch(frame)` | Import a raw CRC-validated batch frame (replication receive path) |
| `ImportSegment(path)` | Import a sealed segment file with full CRC validation |
| `FirstLSN()` / `LastLSN()` | Atomic LSN accessors (0.4–0.6 ns, see benchmarks) |
| `Dir()` | WAL directory path |
| `State()` | Current lifecycle state (`StateInit` / `StateRunning` / `StateDraining` / `StateClosed`) |
| `Stats()` | Lock-free statistics snapshot (43 ns, 0 allocs, see benchmarks) |
| `Shutdown(ctx)` | Graceful shutdown: drain queue → flush → sync → close. Respects context deadline |
| `Close()` | Immediate close: stop writer, discard pending, close storage. No drain |

**`Write` vs `WriteUnsafe`**: `Write` copies all payload/key/meta slices into a single contiguous allocation per record — the caller can freely reuse buffers after `Write` returns. `WriteUnsafe` takes ownership of the slices (no copy) — lower latency (saves one `make` + `copy` per record), but the caller must not touch those slices until after `Flush`. The `owned` flag on each record tracks this. Use `Write` by default; switch to `WriteUnsafe` only when allocation profiling shows `Write` copies as a bottleneck.

**`Flush` vs `Sync` vs `WaitDurable`**: `Flush` waits until all queued batches are *written* to storage (write syscall complete). `Sync` forces an fsync of the active segment file. `WaitDurable` waits until a specific LSN is *fsynced* — it triggers a sync if one hasn't already covered that LSN. The `durableNotifier` coalesces multiple `WaitDurable` callers behind a single fsync, amortizing the cost (benchmark: 274 µs on ext4, 4.4 ms on NTFS per coalesced fsync).

**`Replay` vs `Iterator` vs `Follow`**: Three read models for different use cases:

- `Replay(from, fn)` — push-based, zero-alloc callback, best for full scans. The `fn` callback receives `Event` structs whose `Key`/`Meta`/`Payload` slices point directly into mmap memory (zero-copy). Data is valid only within the callback — copy it if it must outlive the call. Returns when all events are consumed or `fn` returns an error.
- `Iterator(from)` — pull-based, `Next()`/`Event()`/`Close()` pattern. Allocates one `[]Event` decode buffer (25 KB for 100K events, see benchmarks) and reuses it across batches. Best for streaming consumption with backpressure control.
- `Follow(from)` — like `Iterator` but `Next()` blocks when data is exhausted, waking up when new events are written. Crosses segment boundaries automatically. This is the CDC/replication consumer primitive.

**`Snapshot`**: Runs a callback with a `SnapshotController` that provides:

- `Iterator()` / `IteratorFrom(lsn)` — read iterators pinned to the segment list at snapshot creation time
- `Segments()` — segment metadata at snapshot time
- `Checkpoint(lsn)` / `CheckpointOlderThan(ts)` — mark compaction boundaries
- `Compact()` — delete sealed segments before the checkpoint (active segment is never deleted)

Writes continue concurrently during the snapshot. The snapshot sees data written *before* the snapshot was created.

### Batch

| Method | Description |
|---|---|
| `NewBatch(n)` | Pre-allocate batch for n records (internal `[]record` slice) |
| `Append(payload, key, meta, opts...)` | Add record with copy semantics — allocates one `[]byte` for payload+key+meta |
| `AppendWithTimestamp(payload, key, meta, ts)` | Same as `Append` but with explicit nanosecond timestamp, avoids closure alloc |
| `AppendUnsafe(payload, key, meta, opts...)` | Zero-copy — takes ownership of slices, caller must not modify after call |
| `AppendUnsafeWithTimestamp(payload, key, meta, ts)` | Zero-copy with explicit timestamp, avoids closure alloc |
| `MarkNoCompress()` | Skip compression for entire batch (no per-record closure cost) |
| `Len()` | Number of records currently in the batch |
| `Reset()` | Clear records, reset flags, retain backing allocation for reuse |

**Copy semantics detail**: `Append` allocates `make([]byte, len(payload)+len(key)+len(meta))` and copies all three into it. The resulting `record.payload`, `record.key`, and `record.meta` are sub-slices of this single allocation. This means one alloc per record regardless of how many fields are populated. Empty fields become `nil` (not empty slices).

**Timestamp tracking**: The batch tracks whether all records share the same timestamp (`tsUniform` flag). If uniform, the encoder omits per-record timestamp fields, saving 8 bytes per record in the wire format. `AppendWithTimestamp` / `AppendUnsafeWithTimestamp` avoid the closure allocation that `WithTimestamp(ts)` incurs — use them on hot paths.

**Reuse pattern**: `Reset` clears the record slice (`b.records = b.records[:0]`) and resets all tracking flags, but retains the backing array. A batch used with `Reset` in a loop produces 0 allocs/op after the first iteration (see `BenchmarkBatchAppend_Reuse_100`).

**Record options**:

| Option | Effect |
|---|---|
| `WithTimestamp(ts)` | Override auto-assigned `time.Now().UnixNano()`. Costs 1 closure alloc — prefer `AppendWithTimestamp` on hot paths |
| `WithNoCompress()` | Mark this record to skip compression. If *any* record in a batch is marked, the *entire* batch is written uncompressed. Use for pre-compressed data (protobuf, JPEG, zstd) |

### Iterator

| Method | Description |
|---|---|
| `Next()` | Advance to next event. Returns `false` at end-of-data or on error |
| `Event()` | Current event. Valid only after `Next()` returns `true`. Slices point into mmap or decode buffer |
| `Err()` | Error that caused `Next()` to return `false` (nil at normal end-of-data) |
| `Close()` | Release mmap readers and segment references. Must be called to avoid resource leaks |

> [!WARNING]
> `Event()` returns an `Event` whose `Payload`, `Key`, and `Meta` slices borrow memory from the underlying mmap mapping or decode buffer. These slices are invalidated on the next `Next()` call or on `Close()`. Copy any data that must outlive the current iteration step.

## Configuration

All configuration is via functional options passed to `Open`:

| Option | Default | Description |
|---|---|---|
| `WithSyncMode(m)` | `SyncNever` | Fsync strategy (see Durability Modes below) |
| `WithSyncInterval(d)` | `100ms` | Fsync interval for `SyncInterval` mode |
| `WithSyncCount(n)` | — | Sets `SyncCount` mode: fsync every N batches |
| `WithSyncSize(n)` | — | Sets `SyncSize` mode: fsync every N bytes |
| `WithBackpressure(m)` | `BlockMode` | Behavior when queue is full (see Backpressure Modes below) |
| `WithQueueSize(n)` | `4096` | MPSC queue capacity. Rounded to power of 2 internally |
| `WithBufferSize(n)` | `64 KB` | Encoder buffer size. Larger buffers reduce syscall frequency for small batches |
| `WithMaxBatchSize(n)` | `4 MB` | Reject batches exceeding this encoded size with `ErrBatchTooLarge` |
| `WithMaxSegmentSize(n)` | `256 MB` | Segment rotation size threshold. After a write crosses this, the segment is sealed |
| `WithMaxSegmentAge(d)` | — | Rotation by age. Checked after each write; seals if segment age exceeds threshold |
| `WithMaxSegments(n)` | — | Max segment count. Oldest sealed segments deleted after rotation exceeds this |
| `WithRetentionSize(n)` | — | Auto-delete oldest sealed segments when total WAL size exceeds this |
| `WithRetentionAge(d)` | — | Auto-delete sealed segments older than this duration |
| `WithPreallocate(v)` | `false` | Preallocate segment files to `MaxSegmentSize` on creation (reduces fragmentation) |
| `WithStartLSN(lsn)` | — | Starting LSN for fresh WALs. Ignored if segments already exist |
| `WithCompressor(c)` | `nil` | Batch compression (see Compressor below) |
| `WithIndex(idx)` | `nil` | Per-event indexer callback (see Indexer below) |
| `WithHooks(h)` | — | Observability callbacks (see Hooks below) |
| `WithStorageFactory(f)` | `NewFileStorage` | Custom storage backend factory (see Storage below) |

### Durability Modes

| Mode | Behavior | Data loss window |
|---|---|---|
| `SyncNever` | No fsync, OS page cache only | All unsynced data (seconds to minutes) |
| `SyncBatch` | fsync after every write batch | None — every `Write` acknowledged is durable |
| `SyncInterval` | fsync at configurable interval (default 100ms) | Up to one interval |
| `SyncCount` | fsync after every N batches processed by the writer | Up to N−1 batches |
| `SyncSize` | fsync after every N bytes written to the segment | Up to N−1 bytes |

`SyncBatch` provides the strongest guarantee but is dominated by fsync latency (274 µs on ext4, 4.4 ms on NTFS). `SyncInterval` at 100ms gives near-`SyncNever` throughput with a bounded loss window. On Linux ext4, `SyncBatch` is actually *faster* than `SyncNever` for some workloads because the sync-complete signal enables tighter writer goroutine scheduling (see benchmarks).

### Backpressure Modes

| Mode | Behavior |
|---|---|
| `BlockMode` | Caller blocks on `Write` until queue has space. Default and safest. No data loss |
| `DropMode` | `Write` returns `LSN 0, nil`. Fires `Hooks.OnDrop(count)`, increments `Stats.Drops`. Caller must check `lsn == 0` |
| `ErrorMode` | `Write` returns `0, ErrQueueFull` immediately. Caller decides what to do |

`BlockMode` is appropriate for most workloads. `DropMode` is for metrics/telemetry where losing some data is acceptable. `ErrorMode` is for applications that need explicit control over retry/fallback logic.

## Lifecycle

```
StateInit ──► StateRunning ──► StateDraining ──► StateClosed
   Open()       Write/Replay      Shutdown()        Done
```

- `Shutdown(ctx)` — sets state to `StateDraining` (rejects new writes with `ErrDraining`), waits for the writer goroutine to drain all queued batches, fsyncs the active segment, writes manifest, closes all storage handles. If the context is cancelled, pending writes are discarded. Idempotent — safe to call multiple times or concurrently.
- `Close()` — immediate stop. Signals the writer goroutine to exit, closes storage, does not drain pending writes. Use when you need fast shutdown and can tolerate data loss of unprocessed queue entries. Idempotent.
- `State()` returns the current state. Safe to call from any goroutine.
- After `Close` or `Shutdown`, all operations return `ErrClosed`. `Stats()` still works — cumulative counters are preserved for post-mortem analysis, gauge fields (`QueueSize`, `TotalSize`, etc.) are zeroed.

## Extensibility

### Compressor

```go
type Compressor interface {
    Compress(src []byte) ([]byte, error)
    Decompress(src []byte) ([]byte, error)
}
```

`Compress` is called from the writer goroutine with the records region (everything between the header and CRC trailer). The returned bytes replace the records region in the batch frame. `Decompress` is called during replay/iterator when the `flagCompressed` bit is set in the batch header.

**Allocation-free path**: if the `Compressor` also implements `ScratchCompressor`, the WAL uses `CompressTo(dst, src)` / `DecompressTo(dst, src)` instead — passing a reusable scratch buffer as `dst`. This avoids per-batch heap allocations on the hot path.

```go
type ScratchCompressor interface {
    Compressor
    CompressTo(dst, src []byte) ([]byte, error)
    DecompressTo(dst, src []byte) ([]byte, error)
}
```

**Panic safety**: if `Compress` or `Decompress` panics, the WAL recovers, wraps the panic into `ErrWriterPanic`, and fires `Hooks.OnError`. The writer goroutine continues operating — one bad batch does not bring down the WAL.

**Per-record skip**: `WithNoCompress()` on any record in a batch or `batch.MarkNoCompress()` disables compression for the entire batch. This is essential for pre-compressed payloads (protobuf, JPEG, zstd) — double-compression wastes CPU and often *increases* size.

**Typical implementation** (zstd example):

```go
type ZstdCompressor struct{}

func (c *ZstdCompressor) Compress(src []byte) ([]byte, error) {
    return zstd.Compress(nil, src)
}
func (c *ZstdCompressor) Decompress(src []byte) ([]byte, error) {
    return zstd.Decompress(nil, src)
}
```

### Indexer

```go
type Indexer interface {
    OnAppend(info IndexInfo)
}

type IndexInfo struct {
    LSN       LSN    // event LSN
    Timestamp int64  // event timestamp (nanoseconds)
    Key       []byte // borrowed — valid only during callback
    Meta      []byte // borrowed — valid only during callback
    Offset    int64  // batch frame byte offset within segment file
    Segment   LSN    // segment identifier (= segment's FirstLSN)
}
```

Called from the writer goroutine, **once per event**, after the batch is successfully written to storage. This is the integration point for building secondary indexes — B-tree by key, time-series index by timestamp, full-text index, etc.

**Key details**:

- **Goroutine**: always the writer goroutine. No concurrent calls. No locks needed in the indexer.
- **Borrowed slices**: `Key` and `Meta` point into the encoder buffer. Copy them if they must outlive the callback.
- **Panic safety**: panics are recovered. A panicking indexer does not crash the WAL or lose data.
- **Segment deletion**: the indexer is *not* notified when segments are deleted (by retention, compaction, or explicit `DeleteBefore`/`DeleteOlderThan`). To keep an external index consistent, set `Hooks.OnDelete` to remove stale entries when segments are removed.
- **Offset field**: `Offset` is the byte offset of the *batch frame* (not the individual record) within the segment file. Multiple events in the same batch share the same `Offset`. This is useful for building offset-based seek indexes.

### Storage

```go
type Storage interface {
    Write(p []byte) (n int, err error)  // append data to segment
    Sync() error                         // fsync the segment file
    Close() error                        // close the file handle
    Size() (int64, error)               // current file size
    Truncate(size int64) error          // truncate at recovery
    ReadAt(p []byte, off int64) (n int, err error) // random read for non-mmap fallback
}
```

The WAL creates one `Storage` instance per segment via `StorageFactory`:

```go
type StorageFactory func(path string) (Storage, error)
```

If not set, the default factory is `NewFileStorage`, which wraps `os.File` with a mutex.

**Lock bypass optimization**: the writer goroutine is the sole writer to each segment. If the `Storage` implementation also satisfies `WriteNoLock([]byte)(int,error)` and/or `SyncNoLock()error`, the writer goroutine calls those methods instead of `Write`/`Sync`, skipping the mutex. The default `FileStorage` implements both. This avoids one mutex lock/unlock per write syscall on the hot path.

**Custom backends**: implement `Storage` to back the WAL with any persistence layer — in-memory (for testing), encrypted, direct I/O (O_DIRECT), network-attached storage, S3-compatible object store, etc. The WAL only calls `Write` sequentially from one goroutine, `Sync` from the same goroutine, and `ReadAt` concurrently from replay/iterator goroutines. `Close` and `Truncate` are called during shutdown and recovery respectively.

**ReadAt for non-mmap replay**: the WAL uses mmap for replay on sealed segments (zero-copy). For the active segment or when mmap fails, it falls back to `ReadAt`. Custom `Storage` implementations that don't support mmap (e.g., in-memory, network) should provide efficient `ReadAt`.

## Observability

### Hooks

15 callbacks across 5 categories. All optional — `nil` fields are no-ops. All panic-safe — panics in hooks are recovered internally via `defer recover()` and never affect WAL correctness.

```go
uewal.WithHooks(uewal.Hooks{
    // ── Lifecycle (caller goroutine) ──
    OnStart:         func() { /* WAL opened, writer started */ },
    OnShutdownStart: func() { /* Shutdown() called, draining begins */ },
    OnShutdownDone:  func(elapsed time.Duration) { /* shutdown complete */ },

    // ── Write pipeline (writer goroutine, must not block) ──
    AfterAppend:     func(firstLSN, lastLSN uewal.LSN, count int) { /* batch persisted */ },
    BeforeWrite:     func(bytes int) { /* about to write N bytes to storage */ },
    AfterWrite:      func(bytes int, elapsed time.Duration) { /* write syscall done */ },
    BeforeSync:      func() { /* about to fsync */ },
    AfterSync:       func(bytes int, elapsed time.Duration) { /* fsync done */ },

    // ── Errors (writer goroutine) ──
    OnCorruption:    func(segmentPath string, offset int64) { /* CRC mismatch during recovery */ },
    OnDrop:          func(count int) { /* events dropped in DropMode */ },
    OnError:         func(err error) { /* write/sync/rotation error */ },

    // ── Recovery (during Open, before writer starts) ──
    OnRecovery:      func(info uewal.RecoveryInfo) { /* recovery complete */ },

    // ── Replication ──
    OnImport:        func(firstLSN, lastLSN uewal.LSN, bytes int) { /* import success */ },

    // ── Segment lifecycle (writer goroutine or caller goroutine) ──
    OnRotation:      func(sealed uewal.SegmentInfo) { /* segment sealed, new one created */ },
    OnDelete:        func(deleted uewal.SegmentInfo) { /* segment file deleted */ },
})
```

**Goroutine model**: write-pipeline hooks (`AfterAppend`, `BeforeWrite`, `AfterWrite`, `BeforeSync`, `AfterSync`) run inside the writer goroutine — they **must not block** for extended periods (no I/O, no locks held elsewhere). Use them for metrics counters, not for calling external services. Lifecycle hooks (`OnStart`, `OnShutdownStart`, `OnShutdownDone`) run in the caller goroutine. `OnDelete` may run in either goroutine depending on who triggers deletion (writer for retention, caller for explicit `DeleteBefore`/`Snapshot.Compact`).

**RecoveryInfo**: passed to `OnRecovery` during `Open`:

```go
type RecoveryInfo struct {
    SegmentCount   int   // number of segments recovered
    TruncatedBytes int64 // bytes truncated from active segment tail (corrupt frames)
    Corrupted      bool  // true if any corruption was detected and cleaned
}
```

**Use cases**: Prometheus counters on `AfterWrite`/`AfterSync`. Alerting on `OnCorruption`/`OnDrop`/`OnError`. Replication lag tracking via `AfterAppend` LSN range. External index cleanup on `OnDelete`. Shutdown duration monitoring via `OnShutdownDone(elapsed)`.

### Stats

```go
s := w.Stats()
```

| Field | Type | Description |
|---|---|---|
| `EventsWritten` | `uint64` | Cumulative events written (monotonic) |
| `BatchesWritten` | `uint64` | Cumulative batches written (monotonic) |
| `BytesWritten` | `uint64` | Cumulative bytes written to storage (monotonic) |
| `BytesSynced` | `uint64` | Cumulative bytes covered by fsync (monotonic) |
| `SyncCount` | `uint64` | Cumulative fsync operations (monotonic) |
| `Drops` | `uint64` | Events dropped in DropMode (monotonic) |
| `CompressedBytes` | `uint64` | Bytes saved by compression (monotonic) |
| `Corruptions` | `uint64` | Corrupted frames detected at recovery (monotonic) |
| `RotationCount` | `uint64` | Segment rotations (monotonic) |
| `RetentionDeleted` | `uint64` | Segments deleted by retention policy (monotonic) |
| `RetentionBytes` | `uint64` | Bytes reclaimed by retention (monotonic) |
| `ImportBatches` | `uint64` | Batches imported via replication (monotonic) |
| `ImportBytes` | `uint64` | Bytes imported via replication (monotonic) |
| `LastSyncNano` | `int64` | UnixNano timestamp of last fsync (0 = never synced) |
| `QueueSize` | `int` | Current write queue depth (gauge, 0 after close) |
| `TotalSize` | `int64` | Sum of all segment file sizes in bytes (gauge) |
| `ActiveSegmentSize` | `int64` | Current active segment file size (gauge) |
| `SegmentCount` | `int` | Total segment count (gauge) |
| `FirstLSN` | `LSN` | Lowest LSN across all segments (gauge, 0 if empty) |
| `LastLSN` | `LSN` | Highest assigned LSN (gauge) |
| `State` | `State` | Current lifecycle state (gauge) |

All 21 fields use `sync/atomic` — no locks on the read path (43 ns per snapshot, 0 allocs). Safe to call from any goroutine in any state, including after `Close`. After close, monotonic counters are preserved (post-mortem analysis); gauge fields are zeroed.

**Consistency model**: each field is individually atomic, but the snapshot as a whole is not globally linearizable — no single lock is held during collection. In practice, fields are updated in a deterministic order within the writer goroutine, so a snapshot taken between writes is consistent. A snapshot taken during a write may show `EventsWritten` incremented before `BytesWritten` for the same batch.

## Errors

26 sentinel errors, all comparable with `==` and `errors.Is`:

| Error | Condition |
|---|---|
| `ErrClosed` | Operation on a closed WAL |
| `ErrDraining` | Write during graceful shutdown |
| `ErrNotRunning` | Operation requiring StateRunning |
| `ErrInvalidState` | Illegal lifecycle transition |
| `ErrQueueFull` | Write in ErrorMode when queue is full |
| `ErrEmptyBatch` | Write with zero events |
| `ErrBatchTooLarge` | Batch exceeds MaxBatchSize |
| `ErrShortWrite` | Storage returned n < len(p) without error |
| `ErrCorrupted` | Data corruption detected |
| `ErrCRCMismatch` | CRC-32C validation failure |
| `ErrInvalidRecord` | Truncated or unsupported record header |
| `ErrInvalidLSN` | Invalid LSN argument |
| `ErrLSNOutOfRange` | LSN outside available range |
| `ErrCompressorRequired` | Compressed data without Compressor configured |
| `ErrDecompress` | Decompression failure |
| `ErrDirectoryLocked` | Directory locked by another process |
| `ErrCreateDir` | Directory creation failed |
| `ErrLockFile` | Lock file open failed |
| `ErrSync` | fsync failed |
| `ErrMmap` | Memory mapping failed |
| `ErrSegmentNotFound` | Segment not found or not sealed |
| `ErrCreateSegment` | Segment creation failed |
| `ErrSealSegment` | Segment seal failed |
| `ErrScanDir` | Directory scan failed |
| `ErrManifestTruncated` / `ErrManifestVersion` / `ErrManifestWrite` | Manifest errors |
| `ErrImportInvalid` / `ErrImportRead` / `ErrImportWrite` | Import/replication errors |
| `ErrWriterPanic` | Writer goroutine panicked (user Compressor/Indexer) |

## Benchmarks

Three environments, two hardware classes, two operating systems. All values are **medians**. `B/op` and `allocs/op` are deterministic — they depend on code, not hardware.

### Environments

| | Laptop | CI Server (Linux) | CI Server (Windows) |
|---|---|---|---|
| CPU | Intel Core i7-10510U, 4C/8T | AMD EPYC 7763, 4 vCPU | AMD EPYC 7763, 4 vCPU |
| TDP | 15W (mobile, throttles) | 280W (server, stable) | 280W (server, stable) |
| OS | Windows 10 (NTFS) | Ubuntu (ext4) | Windows Server 2022 (NTFS) |
| Go | 1.24 | 1.26 | 1.26 |
| GOMAXPROCS | 8 | 4 | 4 |
| Runs | 3 (`-count=3`) | 3 (`-count=3`) | 3 (`-count=3`) |

This gives three comparison axes: **laptop vs server** (hardware scaling), **Linux vs Windows** (OS/filesystem impact on same hardware), and **NTFS vs ext4** (fsync and I/O path differences).

### Write Path

| Benchmark | Laptop | Linux | Windows | B/op | allocs/op |
|---|---|---|---|---|---|
| Append 128B | 522 ns / 245 MB/s | 517 ns / 248 MB/s | 1.24 µs / 103 MB/s | 381 | 2 |
| Append 1KB | 1.6 µs / 638 MB/s | 2.5 µs / 403 MB/s | 6.6 µs / 155 MB/s | 1361 | 2 |
| Append 64KB | 116 µs / 567 MB/s | 153 µs / 429 MB/s | 669 µs / 98 MB/s | 127K | 3 |
| Append WithKey | 512 ns / 270 MB/s | 544 ns / 254 MB/s | 1.28 µs / 108 MB/s | 386 | 2 |
| Append WithTimestamp | 520 ns / 246 MB/s | 507 ns / 252 MB/s | 1.29 µs / 99 MB/s | 404 | 3 |
| Batch×1 | 318 ns / 403 MB/s | 536 ns / 239 MB/s | 1.14 µs / 112 MB/s | 251 | 1 |
| Batch×10 | 2.0 µs / 641 MB/s | 3.5 µs / 364 MB/s | 8.7 µs / 149 MB/s | 1037 | 1 |
| Batch×100 | 19.0 µs / 673 MB/s | 35.0 µs / 366 MB/s | 134 µs / 96 MB/s | 12.4K | 1 |
| Batch×1000 | 189 µs / 677 MB/s | 342 µs / 375 MB/s | 1.41 ms / 91 MB/s | 238K | 1 |
| Batch×100 Copy | 19.9 µs / 643 MB/s | 34.6 µs / 370 MB/s | 93 µs / 137 MB/s | 25.4K | 101 |
| Batch×100 Unsafe | 19.5 µs / 636 MB/s | 34.5 µs / 371 MB/s | 129 µs / 100 MB/s | 12.5K | 1 |
| Batch×100 Reuse | 20.5 µs / 623 MB/s | 34.8 µs / 367 MB/s | 128 µs / 100 MB/s | 4.8K | 0 |
| Batch×100 4KB payload | 505 µs / 811 MB/s | 987 µs / 415 MB/s | 4.2 ms / 98 MB/s | 434K | 1 |
| Batch×100 KeyMeta | 22.9 µs / 642 MB/s | 39.2 µs / 375 MB/s | 141 µs / 104 MB/s | 13.3K | 1 |

### Durability

| Benchmark | Laptop | Linux | Windows | B/op | allocs/op |
|---|---|---|---|---|---|
| Flush SingleEvent | 5.8 µs | 3.6 µs | 17.9 µs | 462 | 4 |
| Flush+Sync | 1.15 ms | 276 µs | 4.37 ms | 376 | 4 |
| WaitDurable SyncNever | 1.18 ms | 278 µs | 4.43 ms | 404 | 4 |
| WaitDurable SyncBatch | 1.13 ms | 274 µs | 4.41 ms | 377 | 4 |

### Sync Modes

| Benchmark | Laptop | Linux | Windows | B/op | allocs/op |
|---|---|---|---|---|---|
| SyncNever | 509 ns / 252 MB/s | 502 ns / 255 MB/s | 1.24 µs / 103 MB/s | 393 | 2 |
| SyncBatch | 1.46 µs / 88 MB/s | 474 ns / 270 MB/s | 2.04 µs / 63 MB/s | 362 | 2 |
| SyncInterval 100ms | 547 ns / 234 MB/s | 428 ns / 299 MB/s | 1.17 µs / 109 MB/s | 397 | 2 |
| SyncCount 10 | 1.49 µs / 85 MB/s | 480 ns / 267 MB/s | 1.94 µs / 66 MB/s | 357 | 2 |
| SyncSize 4KB | 1.52 µs / 84 MB/s | 475 ns / 269 MB/s | 1.95 µs / 66 MB/s | 357 | 2 |

### Parallel Writers

| Benchmark | Laptop | Linux | Windows | B/op | allocs/op |
|---|---|---|---|---|---|
| 4 Writers | 549 ns / 233 MB/s | 550 ns / 232 MB/s | 1.12 µs / 115 MB/s | 386 | 2 |
| 16 Writers | 580 ns / 221 MB/s | 548 ns / 234 MB/s | 1.17 µs / 110 MB/s | 388 | 2 |
| Parallel SyncBatch | 935 ns / 137 MB/s | 543 ns / 236 MB/s | 2.03 µs / 63 MB/s | 373 | 2 |
| Batch×100 Parallel | 19.7 µs / 649 MB/s | 34.8 µs / 368 MB/s | 124 µs / 103 MB/s | 12.2K | 1 |
| WithKeyMeta Parallel | 606 ns / 238 MB/s | 580 ns / 248 MB/s | 1.32 µs / 110 MB/s | 395 | 2 |

### Compression

| Benchmark | Laptop | Linux | Windows | B/op | allocs/op |
|---|---|---|---|---|---|
| NopCompressor | 2.1 µs / 486 MB/s | 2.8 µs / 364 MB/s | 6.8 µs / 150 MB/s | 2527 | 3 |
| Shrink 50% | 1.25 µs / 821 MB/s | 1.47 µs / 697 MB/s | 3.5 µs / 289 MB/s | 1921 | 3 |
| NoCompress flag | 1.72 µs / 596 MB/s | 2.6 µs / 399 MB/s | 6.5 µs / 157 MB/s | 1386 | 3 |
| Batch×100 Compression | 79 µs / 1.30 GB/s | 125 µs / 820 MB/s | 518 µs / 198 MB/s | 118K | 2 |

### Encoding (pure CPU — sequential writes into pre-allocated buffer, no I/O)

| Benchmark | Laptop | Linux | Windows | allocs/op |
|---|---|---|---|---|
| Encode 10×128B | 211 ns / 6.1 GB/s | 210 ns / 6.1 GB/s | 218 ns / 5.9 GB/s | 0 |
| Encode 100×128B | 1.64 µs / 7.8 GB/s | 1.59 µs / 8.0 GB/s | 1.71 µs / 7.5 GB/s | 0 |
| Encode 1000×128B | 17.7 µs / 7.2 GB/s | 16.2 µs / 7.9 GB/s | 17.0 µs / 7.5 GB/s | 0 |
| Encode 100×4KB | 30.8 µs / 13.3 GB/s | 32.1 µs / 12.8 GB/s | 30.9 µs / 13.2 GB/s | 0 |
| Encode UniformTS | 1.61 µs / 7.9 GB/s | 1.62 µs / 7.9 GB/s | 1.70 µs / 7.5 GB/s | 0 |
| Encode PerRecordTS | 1.69 µs / 7.5 GB/s | 1.69 µs / 7.6 GB/s | 1.77 µs / 7.2 GB/s | 0 |
| Encode WithKeyMeta | 2.54 µs / 5.8 GB/s | 2.53 µs / 5.8 GB/s | 2.63 µs / 5.6 GB/s | 0 |
| RecordsRegion 100×128B | 932 ns / 14.6 GB/s | 845 ns / 16.2 GB/s | 926 ns / 14.7 GB/s | 0 |
| BatchFrame 100×128B | 1.64 µs / 8.3 GB/s | 1.60 µs / 8.5 GB/s | 1.68 µs / 8.1 GB/s | 0 |

### Decoding (pointer slicing into existing buffer, not memcpy)

| Benchmark | Laptop | Linux | Windows | B/op | allocs/op |
|---|---|---|---|---|---|
| Decode 10×128B | 427 ns / 3.3 GB/s | 411 ns / 3.4 GB/s | 633 ns / 2.2 GB/s | 896 | 1 |
| Decode 100×128B | 3.4 µs / 4.0 GB/s | 3.5 µs / 3.9 GB/s | 5.4 µs / 2.5 GB/s | 9472 | 1 |
| Decode 1000×128B | 35.8 µs / 3.8 GB/s | 46.7 µs / 2.9 GB/s | 53.7 µs / 2.5 GB/s | 90K | 1 |
| Decode 100×4KB | 18.2 µs / 22.5 GB/s | 21.2 µs / 19.4 GB/s | 24.7 µs / 16.6 GB/s | 9472 | 1 |
| DecodeInto Reuse | 1.71 µs / 8.0 GB/s | 1.78 µs / 7.7 GB/s | 1.93 µs / 7.1 GB/s | 0 | 0 |
| DecodeInto NoReuse | 3.2 µs / 4.3 GB/s | 3.3 µs / 4.1 GB/s | 5.9 µs / 2.3 GB/s | 9472 | 1 |

### Read Path (Replay & Iterator — includes mmap page faults, CRC verification, decode)

| Benchmark | Laptop | Linux | Windows | B/op | allocs/op |
|---|---|---|---|---|---|
| Replay 10K×256B | 1.77 ms / 1.4 GB/s | 965 µs / 2.7 GB/s | 1.96 ms / 1.3 GB/s | 496 | 5 |
| Replay 100K×256B | 18.4 ms / 1.4 GB/s | 9.5 ms / 2.7 GB/s | 21.0 ms / 1.2 GB/s | 517 | 5 |
| Replay FromMiddle 100K | 6.1 ms / 1.05 GB/s | 3.5 ms / 1.8 GB/s | 6.2 ms / 1.0 GB/s | 502 | 5 |
| ReplayRange Last1K of 100K | 191 µs / 669 MB/s | 103 µs / 1.24 GB/s | 212 µs / 605 MB/s | 496 | 5 |
| ReplayBatches 100K | 12.3 ms / 1.04 GB/s | 6.1 ms / 2.09 GB/s | 13.1 ms / 978 MB/s | 25K | 6 |
| Replay MultiSeg 100K | 6.9 ms / 1.86 GB/s | 5.8 ms / 2.20 GB/s | 6.05 ms / 2.12 GB/s | 580 | 5 |
| Replay SparseSeek 100K | 189 µs / 681 MB/s | 96 µs / 1.33 GB/s | 208 µs / 617 MB/s | 496 | 5 |
| Replay Batched 1K×100 | 8.3 ms / 1.54 GB/s | 3.8 ms / 3.35 GB/s | 7.9 ms / 1.63 GB/s | 505 | 5 |
| Iterator 10K | 1.83 ms / 1.40 GB/s | 929 µs / 2.76 GB/s | 1.88 ms / 1.34 GB/s | 25K | 7 |
| Iterator 100K | 18.2 ms / 1.41 GB/s | 9.0 ms / 2.84 GB/s | 24.2 ms / 1.06 GB/s | 25K | 7 |
| Iterator FromMiddle 100K | 6.2 ms / 1.04 GB/s | 3.5 ms / 1.81 GB/s | 6.7 ms / 955 MB/s | 25K | 7 |
| Iterator MultiSeg 100K | 7.0 ms / 1.83 GB/s | 5.4 ms / 2.39 GB/s | 5.53 ms / 2.31 GB/s | 25K | 7 |
| Iterator ReadPayload 100K | 12.7 ms / 1.01 GB/s | 7.5 ms / 1.71 GB/s | 14.2 ms / 900 MB/s | 25K | 7 |
| Follow PreSeeded 10K | 1.46 ms / 878 MB/s | 966 µs / 1.33 GB/s | 1.45 ms / 885 MB/s | 929 | 8 |
| Follow ConcurrentWrite | 864 µs / 148 MB/s | 899 µs / 142 MB/s | 1.96 ms / 65 MB/s | 381K | 2032 |
| Snapshot Iterate 10K | 1.21 ms / 1.06 GB/s | 697 µs / 1.84 GB/s | 1.19 ms / 1.08 GB/s | 25K | 7 |

### Recovery & Lifecycle

| Benchmark | Laptop | Linux | Windows | B/op | allocs/op |
|---|---|---|---|---|---|
| Recovery 1K events | 7.1 ms | 1.09 ms | 13.5 ms | 1.2 MB | 83 |
| Recovery 100K events | 31.4 ms | 13.9 ms | 29.4 ms | 15.0 MB | 100 |
| Recovery MultiSeg 100K | 57.4 ms | 3.3 ms | 17.9 ms | 6.2 MB | 556 |
| Recovery LargePayload 4KB×10K | 38.3 ms | 8.7 ms | 40.4 ms | 1.4 MB | 78 |
| Open EmptyDir | 7.1 ms | 1.22 ms | 41.0 ms | 2.7 MB | 72 |
| Open WithManifest 100Seg | 24.6 ms | 2.25 ms | 35.4 ms | 1.8 MB | 1515 |
| Shutdown WithPending | 8.2 ms | 2.05 ms | 62.5 ms | 3.2 MB | 2125 |

### Segments & Rotation

| Benchmark | Laptop | Linux | Windows | B/op | allocs/op |
|---|---|---|---|---|---|
| Rotation Manual | 10.1 ms | 1.42 ms | 96 ms | 1.6 MB | 39 |
| Rotation Auto 1MB | 35 µs / 117 MB/s | 10.6 µs / 387 MB/s | 350 µs / 11.7 MB/s | 4.5K | 2 |
| SmallSeg 256KB | 26.6 µs / 38 MB/s | 6.8 µs / 151 MB/s | 378 µs / 2.7 MB/s | 1.3K | 2 |
| LargeSeg 256MB | 1.64 µs / 626 MB/s | 2.5 µs / 407 MB/s | 6.4 µs / 159 MB/s | 1.34K | 2 |
| Preallocate On | 616 ns / 208 MB/s | 444 ns / 288 MB/s | 1.24 µs / 103 MB/s | 382 | 2 |
| Preallocate Off | 599 ns / 214 MB/s | 447 ns / 286 MB/s | 1.34 µs / 96 MB/s | 394 | 2 |
| DeleteBefore MultiSeg | 11.9 ms | 1.75 ms | 39.5 ms | 30K | 257 |

### Internals

| Benchmark | Laptop | Linux | Windows | allocs/op |
|---|---|---|---|---|
| Queue EnqueueDequeue | 84.9 ns | 30.5 ns | 47.5 ns | 0 |
| Queue TryEnqueue | 80.9 ns | 19.1 ns | 47.0 ns | 0 |
| Queue Contended 8P | 156 ns | 88.3 ns | 97.7 ns | 0 |
| DurableNotifier Advance | 19.8 ns | 7.3 ns | 7.5 ns | 0 |
| DurableNotifier Wait | 1.73 ns | 2.18 ns | 2.56 ns | 0 |
| SparseIndex FindByLSN 10K | 13.8 ns | 12.5 ns | 11.6 ns | 0 |
| SparseIndex FindByTS 10K | 12.9 ns | 12.3 ns | 11.3 ns | 0 |
| SparseIndex Append | 30.6 ns | 16.7 ns | 15.9 ns | 0 |
| Manifest Marshal 100 | 2.3 µs | 2.02 µs | 2.19 µs | 0 |
| Manifest Unmarshal 100 | 2.78 µs | 2.86 µs | 2.87 µs | 0 |
| RecordSlicePool GetPut | 27.9 ns | 24.8 ns | 24.2 ns | 0 |
| Stats Read | 52.9 ns | 41.4 ns | 43.3 ns | 0 |
| FirstLSN / LastLSN | ~0.5 ns | ~0.6 ns | ~0.4 ns | 0 |
| CRC-32C 28B header | 9.2 ns | 8.4 ns | 8.9 ns | 0 |
| CRC-32C 8KB typical (SSE4.2) | 275 ns / 29.8 GB/s | 342 ns / 23.9 GB/s | 350 ns / 23.4 GB/s | 0 |
| CRC-32C 1MB (SSE4.2) | 38.0 µs / 27.6 GB/s | 47.6 µs / 22.0 GB/s | 48.9 µs / 21.4 GB/s | 0 |

### End-to-End

| Benchmark | Laptop | Linux | Windows | B/op | allocs/op |
|---|---|---|---|---|---|
| E2E 1K Events | 16.0 ms / 32 MB/s | 2.4 ms / 213 MB/s | 71 ms / 7.2 MB/s | 3.5 MB | 2136 |
| E2E Batch 10K | 21.1 ms / 121 MB/s | 4.6 ms / 558 MB/s | 73 ms / 35 MB/s | 5.7 MB | 202 |
| Burst 10K×128B | 5.0 ms / 256 MB/s | 5.4 ms / 239 MB/s | 12.1 ms / 108 MB/s | 4.0 MB | 20K |
| Burst 10K×1KB | 16.0 ms / 640 MB/s | 25.5 ms / 401 MB/s | 69 ms / 148 MB/s | 13.3 MB | 20K |
| Burst 100×100 Batches | 2.0 ms / 640 MB/s | 3.5 ms / 368 MB/s | 8.8 ms / 146 MB/s | 962K | 107 |
| Burst Parallel | 548 ns / 234 MB/s | 539 ns / 236 MB/s | 1.26 µs / 101 MB/s | 384 | 2 |
| ImportBatch 100 Records | 146 µs / 93 MB/s | 35 µs / 394 MB/s | 180 µs / 76 MB/s | 15K | 3 |

### Analysis

**Three axes of comparison.** The data reveals three distinct performance regimes:

1. **Pure CPU operations (encoding, decoding, CRC, sparse index)**: nearly identical across all three environments — within 10–15%. These are memory-bound and unaffected by OS or filesystem. The laptop's i7-10510U matches or beats the EPYC on single-threaded decode (3.8 GB/s vs 2.9 GB/s for 1000×128B) due to higher single-core boost clocks.

2. **Write path (Append, Batch)**: the laptop (522 ns) matches Linux CI (517 ns) for 128B appends — both use efficient page-cache writes. Windows CI is 2.4x slower (1.24 µs) because NTFS adds overhead to every write syscall even without fsync. At larger payloads, the gap widens: 64KB append is 116 µs (laptop) vs 153 µs (Linux) vs 669 µs (Windows) — a 5.8x Windows/Linux spread.

3. **I/O-bound operations (fsync, recovery, rotation, lifecycle)**: the widest variation. `Flush+Sync` is 276 µs on Linux, 1.15 ms on laptop, 4.37 ms on Windows CI — a **16x** Linux/Windows spread. This is entirely filesystem: ext4 fsync on a hypervisor is ~276 µs, NTFS fsync on a CI VM is ~4.4 ms, NTFS on a laptop NVMe is ~1.1 ms.

**Linux ext4 dominates I/O-heavy workloads.** Recovery of 100K events: 13.9 ms (Linux) vs 31.4 ms (laptop) vs 29.4 ms (Windows). Multi-segment recovery (100 segments): 3.3 ms (Linux) vs 17.9 ms (Windows) — 5.4x. `Open EmptyDir`: 1.22 ms (Linux) vs 41 ms (Windows) — 33x. Rotation: 1.4 ms (Linux) vs 96 ms (Windows) — 68x. This is not a bug: segment rotation involves `fsync` + `close` + `rename` (index file) + `open` + optional `fallocate`, and NTFS metadata journal operations are structurally slower than ext4, especially in virtualized environments where each metadata operation may trigger a separate host I/O. These numbers are consistent with general NTFS vs ext4 metadata benchmarks.

**SyncBatch on Linux CI: near-zero cost.** On ext4, SyncBatch (474 ns) shows similar or slightly better latency than SyncNever (502 ns). The ~28 ns difference is within benchmark noise, but the trend is consistent across runs. Two factors explain it: (1) on a CI VM with a writeback-cached virtual disk, `fdatasync` returns almost immediately (the hypervisor batches the actual I/O), and (2) the sync-complete notification gives the writer goroutine a natural scheduling yield that reduces queue contention. On real production storage with durable fsync, SyncBatch will add measurable latency — the laptop numbers confirm this: 1.46 µs (SyncBatch) vs 509 ns (SyncNever), a 2.9x cost that reflects actual NVMe fsync. On NTFS in the same CI VM: 2.04 µs (SyncBatch) vs 1.24 µs (SyncNever), because NTFS fsync latency is 10–15x higher than ext4.

**Batch amortization: consistent 91%+ across platforms.** Single 128B append: 522 ns (laptop). Batch×100: 19.0 µs = 190 ns/event. The fixed overhead (queue, CRC, syscall) is amortized across 100 events regardless of OS. At batch×1000 on Linux: 342 µs / 1000 = 342 ns/event — encoding dominates. Batch throughput on Linux: **375 MB/s** sustained for 1000-record batches.

**Encoding: 6–8 GB/s, zero allocations, OS-independent.** 100×128B records encode in 1.59 µs on Linux (8.0 GB/s), 1.64 µs on laptop (7.8 GB/s), 1.71 µs on Windows (7.5 GB/s). The 7% spread is purely CPU clock differences. `EncodeRecordsRegion` hits 16.2 GB/s on Linux — this is the memcpy-bound inner loop writing record headers and payloads into a contiguous buffer.

**Replay: 2.7 GB/s on Linux via mmap.** 100K events replay in 9.5 ms on Linux (2.7 GB/s) vs 18.4 ms on laptop (1.4 GB/s) vs 21.0 ms on Windows (1.2 GB/s). The Linux speedup comes from mmap page fault handling in ext4 — pages are demand-faulted from the page cache with minimal kernel overhead. Multi-segment replay narrows the gap (5.8 ms / 6.9 ms / 6.05 ms) because pre-warmed mmap caches on sealed segments reduce page faults.

**Sparse index seek: 96 µs (Linux) vs 212 µs (Windows) for point lookup in 100K events.** Binary search on the sparse index finds the starting offset, then mmap replay reads only the target range. This turns a 9.5 ms full scan into a 96 µs point lookup — **99x speedup**.

**Parallel writers: near-zero contention everywhere.** 4 writers on Linux: 550 ns vs 517 ns single-threaded — only 6% increase. 16 writers on Linux: 548 ns — no measurable degradation from 4. The lock-free CAS on the queue head and single-writer goroutine architecture prevent contention regardless of producer count. The slight *improvement* at 4 writers on Windows (1.12 µs vs 1.24 µs) comes from larger group commits filling the queue faster.

**Queue internals: 30 ns (Linux) vs 85 ns (laptop) per enqueue+dequeue.** The 2.8x difference is not OS but hardware: the EPYC's Zen3 pipeline resolves CAS contention faster than the mobile Comet Lake. Under 8-producer contention: 88 ns (Linux) vs 156 ns (laptop) — still linear scaling. The queue is never the bottleneck: even at 156 ns, it's 3x below the ~500 ns write path cost.

**fsync is the variable.** The same WAL code yields 276 µs (ext4), 1.15 ms (NTFS/NVMe), or 4.4 ms (NTFS/VM) per fsync. The WAL's `WaitDurable` coalesces multiple waiters behind a single fsync via `durableNotifier`, amortizing this cost across concurrent producers. On ext4, `SyncBatch` adds negligible overhead. On NTFS, use `SyncInterval` or `SyncCount` to batch fsync operations.

**CRC-32C: hardware-accelerated, 22–30 GB/s.** Uses SSE4.2 CRC instructions. These numbers are CRC compute throughput only — not WAL write throughput. The laptop's higher single-core frequency gives 27.6 GB/s (1MB) vs 22.0 GB/s (Linux) — clock speed, not architecture. CRC is never the bottleneck: even a 1MB batch takes 38–49 µs for CRC vs 116–669 µs for the write syscall.

**FirstLSN / LastLSN: sub-nanosecond.** These are `atomic.Load` on a hot cache line — compiles to a single `MOV` on x86 (no `LOCK` prefix). At ~0.5 ns (~1.5 CPU cycles at 3 GHz), this is a realistic measurement for a cache-resident atomic read, not a benchmark artifact.

**Why a laptop can match or beat a CI server.** The hot path is single-threaded: one writer goroutine handles drain → encode → CRC → write → sync. Server CPUs (EPYC) are optimized for multi-core throughput, not single-thread latency. The i7-10510U boosts to 4.9 GHz vs EPYC's ~3.0 GHz — a 1.6x clock advantage on the code path that matters. Additionally, CI VMs add I/O indirection (`write()` → virtio → hypervisor → host FS → disk), while the laptop writes directly through NTFS to NVMe. NUMA effects on multi-socket server CPUs can add 70–100 ns of memory latency to atomic operations and queue CAS, which the laptop's single-socket UMA architecture avoids entirely. This is typical for single-writer WAL architectures: PostgreSQL, RocksDB, and Kafka all have the same single-log-writer bottleneck. The group commit mechanism amortizes this by draining multiple batches per syscall, but the fundamental throughput ceiling is one core.

**Batch-level CRC.** The wire format computes one CRC-32C per batch frame, not per record. A 100-record batch produces one CRC over the entire frame (~13 KB) instead of 100 individual CRCs over ~128 B headers. This reduces CRC overhead per event by ~10x at batch sizes ≥10, and is a key reason the encode path stays at 0 allocs and 8 GB/s. The trade-off: a single corrupted byte invalidates the entire batch on recovery, not just one record. For a WAL where batches are the atomic commit unit, this is the correct trade-off.

## Quality

| Metric | Value |
|---|---|
| Test functions | 326 |
| Fuzz targets | 5 |
| Benchmarks | 128 |
| Examples | 15 |
| Coverage | 88.8% |
| Race detector | All tests pass with `-race` |
| Linter | 0 issues (`golangci-lint`, 11 linters) |
| Go version | 1.24+ |
| External deps | 0 (stdlib only) |

## File Structure

```
uewal/
├── doc.go              # Package documentation
├── wal.go              # WAL struct, Open, public API
├── append.go           # Write path, LSN counter, backpressure
├── writer.go           # Single writer goroutine, group commit
├── encoding.go         # Wire format, batch encode/decode, CRC-32C
├── event.go            # Event, LSN, Batch, RecordOption
├── queue.go            # Bounded MPSC queue (CAS + condvar)
├── pool.go             # Record slice pool (sync.Pool)
├── segment.go          # Segment struct, mmap cache, seal
├── manager.go          # Segment manager: recovery, rotation, retention
├── manifest.go         # Binary manifest format, atomic write
├── sparse.go           # Per-segment sparse index (LSN → offset)
├── replay.go           # Replay engine (callback-based)
├── iterator.go         # Cross-segment iterator (pull-based)
├── follow.go           # Tail-follow iterator (blocking Next)
├── snapshot.go         # Snapshot controller for consistent reads
├── replication.go      # OpenSegment, ImportBatch, ImportSegment
├── durable.go          # Coalesced fsync notification (durableNotifier)
├── storage.go          # Storage interface, FileStorage
├── mmap.go             # Platform-agnostic mmap API
├── mmap_unix.go        # Unix mmap implementation
├── mmap_windows.go     # Windows MapViewOfFile implementation
├── flock.go            # File lock interface
├── flock_unix.go       # Unix flock
├── flock_windows.go    # Windows LockFileEx
├── options.go          # Functional options, SyncMode, BackpressureMode
├── hooks.go            # Hooks struct, panic-safe runner
├── stats.go            # Stats, atomic statsCollector
├── state.go            # Lifecycle state machine
├── errors.go           # 26 sentinel errors
├── internal/
│   └── crc/
│       └── crc.go      # CRC-32C (Castagnoli) with hardware acceleration
└── go.mod              # Zero dependencies
```

## License

MIT
