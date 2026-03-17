# uewal — LLM Reference

> Module: `github.com/aasyanov/uewal` | Go 1.24+ | Zero dependencies

## CRITICAL RULES

1. **Single process only** — one `Open` per directory. LOCK file enforces exclusivity. Two processes = `ErrDirectoryLocked`
2. **Always `defer w.Shutdown(ctx)`** — never leave a WAL open. Skipping shutdown leaks goroutines and loses unflushed data
3. **Replay slices are borrowed** — `Event.Payload`, `Event.Key`, `Event.Meta` in replay/iterator callbacks point into mmap or decode buffers. Copy if data must outlive the callback or `Next()` call
4. **`WriteUnsafe` requires `Flush`** — after `WriteUnsafe`, do not modify the batch until `Flush()` returns. `Write` copies internally and is safe immediately
5. **LSN 0 means drop** — in `DropMode`, `Write` returns `(0, nil)` on queue full. Always check `lsn == 0` if using `DropMode`
6. **Batch is the unit of atomicity** — single CRC per batch. On recovery, partial batches are discarded entirely
7. **Import in LSN order** — `ImportBatch`/`ImportSegment` must be called in ascending LSN order. Out-of-order import produces duplicates
8. **Hooks must not block** — write-pipeline hooks run in the writer goroutine. Blocking a hook blocks all writes
9. **Indexer is not notified of deletions** — use `Hooks.OnDelete` to clean up external indexes when segments are removed

---

## API

### Open & Shutdown

```go
w, err := uewal.Open("/path/to/wal", opts...)  // creates dir if needed, acquires LOCK
defer w.Shutdown(context.Background())           // drain → flush → sync → close

w.State()   // StateInit | StateRunning | StateDraining | StateClosed
w.Dir()     // directory path
```

### Write

```go
batch := uewal.NewBatch(n)                          // pre-allocate for n records
batch.Append(payload, key, meta)                     // copies all slices
batch.AppendWithTimestamp(payload, key, meta, ts)     // explicit int64 ts (UnixNano), no closure alloc
batch.AppendUnsafe(payload, key, meta)               // zero-copy, caller must not modify slices
batch.AppendUnsafeWithTimestamp(payload, key, meta, ts)
batch.MarkNoCompress()                               // skip compression for entire batch
batch.Reset()                                        // reuse batch, 0 allocs after first use

lsn, err := w.Write(batch)       // copies records internally, safe to reuse batch
lsn, err := w.WriteUnsafe(batch) // zero-copy, must call Flush() before reusing batch
```

**Return values**:
- `(lastLSN, nil)` — success. `lastLSN` = LSN of the last event in the batch
- `(0, ErrEmptyBatch)` — nil or empty batch
- `(0, ErrBatchTooLarge)` — exceeds `MaxBatchSize`
- `(0, ErrClosed)` / `(0, ErrDraining)` — WAL not running
- `(0, ErrQueueFull)` — `ErrorMode` only
- `(0, nil)` — `DropMode` only, batch was dropped

### Durability

```go
w.Flush()              // block until all queued writes are written to storage
w.Sync()               // force fsync of active segment
w.WaitDurable(lsn)     // block until LSN is fsynced (coalesces multiple callers)
```

### Read

```go
// Push-based replay (zero-copy via mmap)
w.Replay(fromLSN, func(ev uewal.Event) error { ... })
w.ReplayRange(fromLSN, toLSN, func(ev uewal.Event) error { ... })
w.ReplayBatches(fromLSN, func(events []uewal.Event) error { ... })

// Pull-based iterator
it, err := w.Iterator(fromLSN)
defer it.Close()
for it.Next() {
    ev := it.Event()  // valid until next Next() call
}
if err := it.Err(); err != nil { ... }

// Live tail (blocks on Next when no new data)
it, err := w.Follow(fromLSN)
defer it.Close()
for it.Next() { ... }  // wakes up on new writes, crosses segments

// Consistent snapshot with compaction
w.Snapshot(func(sc *uewal.SnapshotController) error {
    it, _ := sc.Iterator()
    defer it.Close()
    for it.Next() { ... }
    sc.Checkpoint(processedLSN)
    return sc.Compact()  // deletes segments before checkpoint
})
```

### Segments & Retention

```go
w.Rotate()                // force segment rotation
w.Segments()              // []SegmentInfo
w.DeleteBefore(lsn)       // delete sealed segments with LastLSN < lsn
w.DeleteOlderThan(tsNano) // delete sealed segments with LastTimestamp < ts
w.FirstLSN()              // atomic, 0.4-0.6 ns
w.LastLSN()               // atomic, 0.4-0.6 ns
```

### Replication

```go
// Primary: export sealed segments
rc, info, err := primary.OpenSegment(firstLSN) // io.ReadCloser
data, _ := io.ReadAll(rc)
rc.Close()

// Replica: import (must be in LSN order)
err = replica.ImportBatch(rawFrame)           // single batch frame
err = replica.ImportSegment("/path/to.wal")   // entire sealed segment
```

### Stats

```go
s := w.Stats()  // 43 ns, 0 allocs, safe in any state
// s.EventsWritten, s.BatchesWritten, s.BytesWritten, s.BytesSynced,
// s.SyncCount, s.Drops, s.CompressedBytes, s.Corruptions,
// s.RotationCount, s.RetentionDeleted, s.RetentionBytes,
// s.ImportBatches, s.ImportBytes, s.LastSyncNano,
// s.QueueSize, s.TotalSize, s.ActiveSegmentSize, s.SegmentCount,
// s.FirstLSN, s.LastLSN, s.State
```

---

## OPTIONS

```go
// Durability
WithSyncMode(SyncNever)              // default; no fsync
WithSyncMode(SyncBatch)             // fsync after every write (no data loss)
WithSyncMode(SyncInterval)               // periodic fsync
WithSyncInterval(100 * time.Millisecond) // interval value (requires SyncInterval mode)
WithSyncCount(10)                    // fsync every 10 batches (implies SyncCount mode)
WithSyncSize(1 << 20)               // fsync every 1 MB (implies SyncSize mode)

// Backpressure
WithBackpressure(BlockMode)          // default; caller blocks until space
WithBackpressure(DropMode)           // drop + return LSN 0
WithBackpressure(ErrorMode)          // return ErrQueueFull

// Queue & Buffer
WithQueueSize(4096)                  // default; MPSC queue capacity
WithBufferSize(64 << 10)             // default 64 KB; encoder buffer
WithMaxBatchSize(4 << 20)            // default 4 MB; reject larger batches

// Segments
WithMaxSegmentSize(256 << 20)        // default 256 MB; rotation threshold
WithMaxSegmentAge(time.Hour)         // rotation by age
WithMaxSegments(100)                 // max segments, oldest deleted
WithRetentionSize(10 << 30)          // auto-delete when WAL > 10 GB
WithRetentionAge(24 * time.Hour)     // auto-delete segments older than 24h
WithPreallocate(true)                // preallocate segment files

// Extensions
WithCompressor(comp)                 // Compressor or ScratchCompressor
WithIndex(indexer)                   // Indexer — per-event callback
WithHooks(hooks)                     // Hooks — 15 lifecycle callbacks
WithStorageFactory(factory)          // custom Storage backend
WithStartLSN(1000)                   // initial LSN for fresh WALs
```

---

## ERRORS

```go
// Lifecycle
ErrClosed, ErrDraining, ErrNotRunning, ErrInvalidState

// Write path
ErrQueueFull        // ErrorMode only
ErrEmptyBatch       // nil or empty batch
ErrBatchTooLarge    // exceeds MaxBatchSize
ErrShortWrite       // storage wrote fewer bytes than requested

// Data integrity
ErrCorrupted, ErrCRCMismatch, ErrInvalidRecord
ErrInvalidLSN, ErrLSNOutOfRange

// Compression
ErrCompressorRequired  // compressed data but no Compressor configured
ErrDecompress          // decompression failure

// Infrastructure
ErrDirectoryLocked, ErrCreateDir, ErrLockFile
ErrSync, ErrMmap
ErrSegmentNotFound, ErrCreateSegment, ErrSealSegment, ErrScanDir

// Manifest
ErrManifestTruncated, ErrManifestVersion, ErrManifestWrite

// Replication
ErrImportInvalid, ErrImportRead, ErrImportWrite

// Internal
ErrWriterPanic  // user Compressor/Indexer panicked
```

All created with `errors.New`, comparable with `==` and `errors.Is`.

---

## EXTENSIBILITY INTERFACES

### Compressor

```go
type Compressor interface {
    Compress(src []byte) ([]byte, error)     // writer goroutine, records region only
    Decompress(src []byte) ([]byte, error)   // replay goroutine
}

// Optional zero-alloc extension:
type ScratchCompressor interface {
    Compressor
    CompressTo(dst, src []byte) ([]byte, error)   // reuse dst buffer
    DecompressTo(dst, src []byte) ([]byte, error)
}
```

- Panics recovered → `ErrWriterPanic` + `Hooks.OnError`
- Per-record `WithNoCompress()` or `batch.MarkNoCompress()` skips compression for entire batch
- Header is never compressed; CRC covers compressed bytes

### Indexer

```go
type Indexer interface {
    OnAppend(info IndexInfo)
}

type IndexInfo struct {
    LSN       LSN      // event LSN
    Timestamp int64    // nanoseconds
    Key       []byte   // BORROWED — copy if needed beyond callback
    Meta      []byte   // BORROWED — copy if needed beyond callback
    Offset    int64    // batch frame offset in segment file
    Segment   LSN      // segment identifier (= segment FirstLSN)
}
```

- Called per-event from writer goroutine. Must not block. Panics recovered.
- NOT notified on segment deletion → use `Hooks.OnDelete` for index cleanup
- Multiple events in same batch share the same `Offset`

### Storage

```go
type Storage interface {
    Write(p []byte) (n int, err error)
    Sync() error
    Close() error
    Size() (int64, error)
    Truncate(size int64) error
    ReadAt(p []byte, off int64) (n int, err error)
}

type StorageFactory func(path string) (Storage, error)
```

- One Storage per segment. Writer goroutine is sole writer.
- If Storage also has `WriteNoLock([]byte)(int,error)` / `SyncNoLock()error`, writer skips mutex
- `ReadAt` used for non-mmap replay fallback

### Hooks

```go
type Hooks struct {
    // Lifecycle — caller goroutine
    OnStart         func()
    OnShutdownStart func()
    OnShutdownDone  func(elapsed time.Duration)

    // Write pipeline — writer goroutine, MUST NOT BLOCK
    AfterAppend func(firstLSN, lastLSN LSN, count int)
    BeforeWrite func(bytes int)
    AfterWrite  func(bytes int, elapsed time.Duration)
    BeforeSync  func()
    AfterSync   func(bytes int, elapsed time.Duration)

    // Errors — writer goroutine
    OnCorruption func(segmentPath string, offset int64)
    OnDrop       func(count int)
    OnError      func(err error)

    // Recovery — during Open
    OnRecovery func(info RecoveryInfo)

    // Replication
    OnImport func(firstLSN, lastLSN LSN, bytes int)

    // Segment lifecycle — writer or caller goroutine
    OnRotation func(sealed SegmentInfo)
    OnDelete   func(deleted SegmentInfo)
}
```

All nil-safe. All panic-safe (`defer recover()`). 15 hooks total.

---

## LIFECYCLE

```
Open(dir) ──► StateRunning ──► Shutdown(ctx) ──► StateDraining ──► StateClosed
                                  Close() ──────────────────────► StateClosed

StateRunning:  Write, Replay, Iterator, Follow, Snapshot, Rotate, Segments, Stats — all work
StateDraining: Write → ErrDraining; reads still work; queue draining
StateClosed:   all ops → ErrClosed; Stats still returns (cumulative counters preserved)
```

`Shutdown(ctx)`: drain queue → flush → sync → write manifest → close storage. Idempotent.
`Close()`: stop writer → close storage → discard pending. Idempotent.

---

## INTERNALS

### Write Pipeline

```
Write(batch) → lsn = atomic.Add(count) → writeQueue.enqueue(writeBatch)
                                               ↓
writer goroutine: dequeueAllInto → for each batch:
    encodeBatch(records, buffer) → CRC32C → compress (optional)
    → storage.Write(frame) → maybeSync → indexer.OnAppend (per event)
    → check rotation → signal Flush/WaitDurable barriers
```

- LSN assignment is lock-free (`atomic.Add` on cache-line-padded counter)
- MPSC queue: bounded ring buffer with CAS on head, condvar wakeup
- Group commit: writer drains ALL available batches before one write syscall
- Encoder writes directly into pre-sized buffer — 0 allocs on encode

### Batch Frame (v1)

```
[Magic 4B "EWAL"] [Version 1B] [Flags 1B] [Count 2B]
[FirstLSN 8B] [Timestamp 8B] [BatchSize 4B]
─── records region (optionally compressed) ───
  per record: [Timestamp 8B (if flagPerRecordTS)] [KeyLen 2B] [MetaLen 2B] [PayloadLen 4B] [Key] [Meta] [Payload]
─── end records region ───
[CRC32C 4B]
```

- Uniform-timestamp optimization: when all records share timestamp, per-record TS omitted (saves 8B/record)
- CRC covers header + (compressed) records region
- Total header: 28 bytes. Per-record overhead: 8 bytes (uniform TS) or 16 bytes (per-record TS)

### Segment Files

```
{dir}/
  LOCK                              ← flock/LockFileEx
  manifest.bin                      ← binary segment metadata
  00000000000000000001.wal          ← segment file (name = zero-padded firstLSN)
  00000000000000000001.idx          ← sparse index (LSN, Timestamp, Offset)
```

- Sparse index: sorted array, binary search for O(log k) LSN/timestamp seek
- Index written atomically: tmp → fsync → rename
- Manifest enables O(1) recovery without scanning .wal files

### Recovery

```
Open → read manifest.bin (or fallback: full dir scan)
     → sealed segments: load metadata from manifest (no content read)
     → active segment: mmap → scanBatchFrame (CRC per batch)
     → truncate at last valid batch boundary
     → clean up orphan .wal/.idx/.tmp files
```

Memory: O(1) — active segment scanned via mmap, sealed segments from manifest only.

---

## EDGE CASES

```
Write(nil)                    → (0, ErrEmptyBatch)
Write(empty batch)            → (0, ErrEmptyBatch)
Replay(0, fn) on empty WAL   → nil (no events, no error)
Iterator(0) on empty WAL     → iterator with Next()=false, Err()=nil
Follow(0) on empty WAL       → blocks until data is written
DeleteBefore(0)               → no-op
DeleteOlderThan(0)            → no-op
Shutdown after Shutdown       → no-op (idempotent)
Close after Shutdown          → no-op (idempotent)
Stats after Close             → valid (cumulative preserved, gauges zeroed)
Write after Shutdown          → (0, ErrDraining) or (0, ErrClosed)
Open on locked directory      → ErrDirectoryLocked
Batch with nil payload        → valid (empty payload event)
```

---

## MISTAKES TO AVOID

1. **Not deferring `Shutdown`** — leaks writer goroutine, unflushed data lost, LOCK file held
2. **Holding replay Event slices after callback** — mmap memory recycled on next `Next()` or segment close
3. **Modifying batch after `WriteUnsafe`** — data race with writer goroutine; use `Write` or call `Flush` first
4. **Ignoring `lsn == 0` in DropMode** — write was silently dropped, not an error
5. **Blocking in Hooks** — write-pipeline hooks run in writer goroutine; blocking freezes all writes
6. **Importing out of LSN order** — produces duplicate or out-of-order events, no deduplication
7. **Assuming Stats snapshot is globally consistent** — fields are individually atomic, not a linearizable snapshot
8. **Opening same directory from two processes** — LOCK prevents this, but `ErrDirectoryLocked` must be handled
9. **Not copying Indexer Key/Meta** — borrowed slices valid only during `OnAppend` callback
10. **Using `WithNoCompress` with pre-compressed compressor** — if ANY record has the flag, the ENTIRE batch skips compression

## TYPICAL PATTERNS

### Event sourcing

```go
w, _ := uewal.Open(dir, uewal.WithSyncMode(uewal.SyncBatch))
defer w.Shutdown(ctx)

batch := uewal.NewBatch(1)
batch.Append(eventPayload, []byte("aggregate-123"), []byte("UserCreated"))
lsn, _ := w.Write(batch)
```

### High-throughput buffering

```go
w, _ := uewal.Open(dir,
    uewal.WithSyncMode(uewal.SyncInterval),
    uewal.WithSyncInterval(50*time.Millisecond),
    uewal.WithBackpressure(uewal.DropMode),
)
defer w.Shutdown(ctx)

batch := uewal.NewBatch(100)
for _, msg := range messages {
    batch.AppendUnsafe(msg, nil, nil)
}
w.WriteUnsafe(batch)
w.Flush()
batch.Reset()
```

### Replay with snapshot + compaction

```go
w.Snapshot(func(sc *uewal.SnapshotController) error {
    it, _ := sc.Iterator()
    defer it.Close()
    var lastLSN uewal.LSN
    for it.Next() {
        ev := it.Event()
        applyToState(ev)
        lastLSN = ev.LSN
    }
    sc.Checkpoint(lastLSN)
    return sc.Compact()
})
```

### CDC / replication consumer

```go
it, _ := w.Follow(lastProcessedLSN + 1)
defer it.Close()
for it.Next() {
    ev := it.Event()
    sendDownstream(ev.LSN, ev.Payload) // copy Payload if async
}
```

## NOT IN SCOPE

Not a distributed log. Not a server. No multi-process shared WAL. No built-in networking. No encryption (use custom `Storage`). No built-in compressor (bring your own zstd/lz4/snappy).
