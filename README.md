# UEWAL — Ultra-fast Embedded Write-Ahead Log

[![CI](https://github.com/aasyanov/uewal/actions/workflows/ci.yml/badge.svg)](https://github.com/aasyanov/uewal/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/aasyanov/uewal.svg)](https://pkg.go.dev/github.com/aasyanov/uewal)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Ready to use embedded WAL for Go 1.21+.

```
go get github.com/aasyanov/uewal
```

## Overview

UEWAL is a strict, minimalist, high-performance Write-Ahead Log engine for single-process Go applications. It is designed for event sourcing, state machine recovery, embedded database logs, durable queues, audit logging, and high-frequency buffering.

**Not** a distributed log, server, cloud backend, or multi-process shared WAL. This is a low-level infrastructure component.

## Architecture

```
Append()  ──► lsnCounter (atomic) ──► writeQueue ──► writer goroutine ──► Storage
                                         ▲                  │
                                    Flush barrier        group commit
                                                     encodeBatch + CRC32C
                                                     compress (optional)
                                                     write + maybeSync
                                                     indexer.OnAppend
```

- **Append-only**, single-writer goroutine
- **Lock-free** LSN assignment via `atomic.Uint64`
- **Group commit**: writer drains all available batches before issuing a single write
- **Batch-framed format** with single CRC-32C per batch (true batch atomicity)
- **Zero-copy replay** via mmap (with `ReadAt` fallback for custom Storage)
- **Optional compression** via pluggable `Compressor` interface
- **Optional indexing** via pluggable `Indexer` interface
- **Event metadata** via `Event.Meta` field (zero-cost when nil)
- **2 allocations per Append** (event copy + queue slot), 1 allocation on decode (events slice)
- No reflection, no `interface{}` in hot path

## Quick Start

```go
w, err := uewal.Open("/path/to/wal")
if err != nil {
    log.Fatal(err)
}
defer w.Shutdown(context.Background())

// Write
lsn, err := w.Append(uewal.Event{Payload: []byte("hello")})

// Write with metadata
lsn, err = w.Append(uewal.Event{
    Payload: []byte("user_created"),
    Meta:    []byte("aggregate:user:123"),
})

// Batch write
batch := uewal.NewBatch(3)
batch.Add([]byte("event-1"))
batch.AddWithMeta([]byte("event-2"), []byte("type:update"))
batch.Add([]byte("event-3"))
lsn, err = w.AppendBatch(batch)

// Read (zero-copy via mmap)
w.Replay(0, func(e uewal.Event) error {
    fmt.Printf("LSN=%d meta=%s payload=%s\n", e.LSN, e.Meta, e.Payload)
    return nil
})
```

## Public API

| Method | Description |
|---|---|
| `Open(path, opts...)` | Create or open a WAL |
| `Append(events...)` | Write events, returns last LSN |
| `AppendBatch(batch)` | Write a batch of events |
| `Flush()` | Wait for writer to process all queued batches |
| `Sync()` | fsync to make written data durable |
| `Replay(from, fn)` | Callback-based read (zero-copy) |
| `Iterator(from)` | Pull-based read (zero-copy) |
| `LastLSN()` | Most recently persisted LSN |
| `Stats()` | Lock-free runtime statistics snapshot |
| `Shutdown(ctx)` | Graceful shutdown with context |
| `Close()` | Immediate close without draining |

## Configuration

All configuration is via functional options passed to `Open`:

```go
w, err := uewal.Open(path,
    uewal.WithSyncMode(uewal.SyncBatch),       // fsync after every write
    uewal.WithSyncInterval(50*time.Millisecond), // for SyncInterval mode
    uewal.WithBackpressure(uewal.BlockMode),     // block when queue full
    uewal.WithQueueSize(4096),                   // write queue capacity
    uewal.WithBufferSize(64*1024),               // encoder buffer size
    uewal.WithStorage(customStorage),            // custom Storage backend
    uewal.WithCompressor(zstdCompressor),        // optional compression
    uewal.WithIndex(myIndexer),                  // optional indexer
    uewal.WithHooks(hooks),                      // observability callbacks
)
```

### Durability Modes

| Mode | Behavior | Throughput |
|---|---|---|
| `SyncNever` | No fsync, OS page cache only | Highest |
| `SyncBatch` | fsync after every write batch | Lowest latency risk |
| `SyncInterval` | fsync at configurable interval | Balanced |

### Backpressure Modes

| Mode | Behavior |
|---|---|
| `BlockMode` | Caller blocks until queue has space (default) |
| `DropMode` | Events silently dropped, `Stats.Drops` incremented |
| `ErrorMode` | `ErrQueueFull` returned immediately |

## Lifecycle

```
StateInit ──► StateRunning ──► StateDraining ──► StateClosed
   Open()       Append/Replay     Shutdown()        Done
```

- `Shutdown(ctx)`: graceful — drains queue, flushes, syncs, closes storage. Respects context cancellation. Idempotent.
- `Close()`: immediate — stops writer, closes storage, discards pending data. Idempotent.

## Batch Frame Format (v2)

```
┌──────────────────────────────────────────────────┐
│ Magic        4 bytes   "UWAL"                    │
│ Version      2 bytes   (2)                       │
│ Flags        2 bytes   (bit 0 = compressed)      │
│ RecordCount  4 bytes                             │
│ FirstLSN     8 bytes                             │
│ BatchSize    4 bytes   (total frame incl. CRC)   │
├──────────────────────────────────────────────────┤
│ Records region (possibly compressed):            │
│   Record 0:                                      │
│     PayloadLen  4 bytes                          │
│     MetaLen     2 bytes                          │
│     Meta        MetaLen bytes                    │
│     Payload     PayloadLen bytes                 │
│   Record 1: ...                                  │
├──────────────────────────────────────────────────┤
│ CRC32C       4 bytes   (covers Magic..records)   │
└──────────────────────────────────────────────────┘
```

- **Batch header**: 24 bytes. **Per-record overhead**: 6 bytes. **Batch overhead**: 28 bytes.
- CRC-32C (Castagnoli) with hardware acceleration (SSE4.2 / ARM CRC)
- Little-endian encoding, 4-byte magic "UWAL" for frame detection
- **True batch atomicity**: single CRC covers entire batch; on recovery, either all events in a frame are valid or the entire frame is discarded
- **Compression**: when `Compressor` is set, the records region is compressed and CRC covers compressed bytes
- **Meta zero-cost**: MetaLen=0 when Meta is nil, no extra bytes written

## Compressor Interface

```go
type Compressor interface {
    Compress(src []byte) ([]byte, error)
    Decompress(src []byte) ([]byte, error)
}
```

Implementations manage their own buffers. The WAL calls `Compress` from the writer goroutine and `Decompress` during replay. Typical implementations wrap zstd, lz4, or snappy.

## Indexer Interface

```go
type Indexer interface {
    OnAppend(lsn LSN, meta []byte, offset int64)
}
```

Called from the writer goroutine after each event is persisted. Panics are recovered. Useful for building external indexes, LSN-to-offset lookup tables, or routing events by metadata.

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

## Crash Recovery

On `Open`, the WAL scans all existing batch frames to recover the last valid LSN. If corruption is detected (CRC mismatch or truncated frame), the file is automatically truncated to the last valid batch boundary. A corrupted batch is entirely discarded (true batch atomicity). This process is O(n) in file size but runs only once at startup.

## Flush vs Sync

These are distinct operations with different guarantees:

| Operation | What it does | Durability |
|---|---|---|
| `Flush()` | Waits for writer goroutine to process all pending queue items and write them to storage via `write()` | Data is in OS page cache |
| `Sync()` | Calls `fsync()` on the underlying file | Data survives power failure |

For maximum durability: `Flush()` then `Sync()`.

## Observability

### Hooks

```go
uewal.WithHooks(uewal.Hooks{
    OnStart:         func() { ... },
    OnShutdownStart: func() { ... },
    OnShutdownDone:  func(elapsed time.Duration) { ... },
    BeforeAppend:    func(b *uewal.Batch) { ... },
    AfterAppend:     func(lsn uewal.LSN, count int) { ... },
    BeforeWrite:     func(size int) { ... },
    AfterWrite:      func(n int) { ... },
    BeforeSync:      func() { ... },
    AfterSync:       func(elapsed time.Duration) { ... },
    OnCorruption:    func(offset int64) { ... },
    OnDrop:          func(count int) { ... },
})
```

All hooks are optional, panic-safe, and do not affect WAL consistency.

### Stats

```go
s := w.Stats()
// s.EventsWritten, s.BatchesWritten, s.BytesWritten, s.BytesSynced,
// s.SyncCount, s.CompressedBytes, s.Drops, s.Corruptions,
// s.QueueSize, s.FileSize, s.LastLSN, s.State
```

All counters are lock-free (atomic). Safe to call in any state.

## Benchmark Results

Measured on Intel Core i7-10510U @ 1.80GHz, Windows 10, Go 1.21+, SSD (NVMe).

### Write Path

| Benchmark | ops/sec | Throughput | Allocs/op |
|---|---|---|---|
| AppendAsync (128B payload) | ~9.0M | ~361 MB/s | 2 |
| AppendDurable (128B, SyncBatch) | ~4.9M | ~261 MB/s | 2 |
| AppendBatch10 (10x128B) | ~1.8M batches | ~219 MB/s | 2 |
| AppendBatch100 (100x128B) | ~510K batches | ~794 MB/s | 2 |
| AppendParallel (8 goroutines) | ~5.9M | ~287 MB/s | 2 |

### Flush & Sync

| Benchmark | Latency |
|---|---|
| Flush (write barrier) | ~11.5 μs |
| Flush + Sync (write + fsync) | ~5.3 ms |

### Read Path (100K events, 256B payload)

| Benchmark | Time |
|---|---|
| Replay (callback, mmap) | ~50 ms |
| Iterator (pull-based, mmap) | ~52 ms |

### Encoding (hot path)

| Benchmark | Throughput | Allocs |
|---|---|---|
| EncodeBatch (10x128B) | ~3.6 GB/s | 0 |
| DecodeBatch (10x128B) | ~1.6 GB/s | 1 |

### Recovery

| Benchmark | Time |
|---|---|
| Recovery (100K events) | ~45 ms |

### Analysis

**Write path**: async append achieves ~9M ops/sec. Durable append (SyncBatch) achieves ~4.9M ops/sec. Group commit via batch append dramatically increases throughput — AppendBatch100 delivers ~794 MB/s by amortizing batch frame overhead.

**Read path**: Replay and Iterator show similar performance (~50ms for 100K events). The batch-framed format is efficient due to batch-level CRC verification rather than per-record CRC.

**Encoding**: Zero-allocation encode at ~3.6 GB/s. Decode allocates one slice (events) per batch at ~1.6 GB/s. CRC-32C hardware acceleration (SSE4.2) contributes significantly.

**Recovery**: ~45ms for 100K records means the WAL can recover from a 1M-record file in ~450ms — acceptable for production startup.

**Memory**: 2 allocations per Append (event slice copy + queue slot). 1 allocation per batch on decode (events slice). The encoder buffer grows dynamically but is reused across writes.

## Test Coverage

```
coverage: 91.3% of statements
```

### Test Suite

| Category | Count | Description |
|---|---|---|
| Unit tests | ~90 | encoding, state, stats, hooks, queue, writer, storage, mmap |
| Integration tests | ~45 | WAL lifecycle, append, flush/sync, replay, iterator, recovery, meta, compression |
| Stress tests | 6 | concurrent append, large payloads, repeated open/close, backpressure |
| Fuzz tests | 4 | decode, append+replay, corruption recovery |
| Examples | 6 | GoDoc examples for all major APIs |
| Benchmarks | 12 | write path, read path, encoding, recovery |

All tests pass with `-race` detector enabled.

Uncovered code (~8.7%) consists of OS-level error paths in platform-specific syscalls (Windows mmap `CreateFileMapping` failure, `MapViewOfFile` failure, `flock` edge cases) and `Open` error branches that require simulating filesystem failures.

## File Structure

```
uewal/
├── doc.go              # Package documentation
├── errors.go           # Sentinel errors
├── event.go            # LSN, Event (with Meta), Batch types
├── state.go            # State machine (INIT → RUNNING → DRAINING → CLOSED)
├── stats.go            # Lock-free statistics (atomic counters, CompressedBytes)
├── hooks.go            # Observability hooks with panic recovery
├── options.go          # Functional options, SyncMode, BackpressureMode, Compressor, Indexer
├── storage.go          # Storage interface + FileStorage implementation
├── encoding.go         # Batch frame format v2, encoder
├── queue.go            # Bounded write queue with backpressure
├── append.go           # LSN counter, appendEvents logic
├── writer.go           # Single writer goroutine, group commit, barrier, indexer
├── replay.go           # Iterator, Replay callback, batch-based decoding
├── wal.go              # WAL orchestrator (Open, Shutdown, Close, Flush, Sync)
├── mmap.go             # mmapReader abstraction
├── mmap_unix.go        # mmap via syscall.Mmap (Linux, macOS)
├── mmap_windows.go     # mmap via CreateFileMapping/MapViewOfFile
├── mmap_fallback.go    # ReadAt-based fallback for custom Storage
├── flock.go            # fileLock type definition
├── flock_unix.go       # flock(2) advisory locking
├── flock_windows.go    # LockFileEx/UnlockFileEx
├── internal/crc/crc.go # CRC-32C (Castagnoli) with hardware acceleration
├── *_test.go           # Unit, integration, stress, fuzz, bench, example tests
└── go.mod              # Module: github.com/aasyanov/uewal
```

## Deviations from Specification

| Item | Specification | Implementation | Rationale |
|---|---|---|---|
| `Iterator` return | `*Iterator` | `(*Iterator, error)` | Distinguishes init errors from empty data |
| `OnRotate` hook | Declared | Removed | No rotation logic implemented; dead code eliminated |
| `Rotations` stat | Declared | Removed | Same as above |
| `WithMaxFileSize` | Declared | Removed | Same as above |
| `Flush` semantics | "Send buffer to writer" | Barrier-based drain wait | Stronger guarantee: caller knows data is written |
| Record format | Per-record CRC | Batch-framed CRC | True batch atomicity, lower overhead |

## License

MIT
