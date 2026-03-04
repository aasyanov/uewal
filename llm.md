# UEWAL — LLM Reference

> Module: `github.com/aasyanov/uewal` | Go 1.21+ | Zero dependencies

## CRITICAL RULES

1. Config via **functional options** to `Open` — never mutate structs
2. Errors are **sentinel values** (`==` / `errors.Is`) — never `fmt.Errorf`
3. `Append` returns **assigned** LSN; `LastLSN()` returns **persisted** LSN — they differ
4. `Replay`/`Iterator` Payload and Meta are **mmap'd** — copy if data must outlive callback
5. **Single-writer goroutine** — all writes serialized through write queue
6. **Single-process** — flock prevents concurrent access

---

## API

```go
// Lifecycle
w, err := uewal.Open(path, opts...)
err := w.Shutdown(ctx)              // graceful: drain → flush → sync → close
err := w.Close()                    // immediate: discard pending

// Write
lsn, err := w.Append(uewal.Event{Payload: data, Meta: meta})
lsn, err := w.AppendBatch(batch)    // batch via NewBatch(n) + Add/AddWithMeta

// Durability
err := w.Flush()                    // wait for writer to write pending batches
err := w.Sync()                     // fsync — Flush+Sync = full durability

// Read (zero-copy payload via mmap)
err := w.Replay(fromLSN, func(e uewal.Event) error { ... })
it, err := w.Iterator(fromLSN)      // defer it.Close()

// Observe
lsn := w.LastLSN()                  // persisted, not assigned
s := w.Stats()                      // lock-free atomic snapshot
```

## OPTIONS

```go
WithSyncMode(SyncNever|SyncBatch|SyncInterval)  // default: SyncNever
WithSyncInterval(d)           // default: 100ms
WithBackpressure(BlockMode|DropMode|ErrorMode)   // default: BlockMode
WithQueueSize(n)              // default: 4096
WithBufferSize(n)             // default: 64KiB
WithStorage(s Storage)        // default: FileStorage (os.File + flock)
WithCompressor(c Compressor)  // Compress/Decompress interface
WithIndex(i Indexer)          // OnAppend(lsn, meta, offset) from writer goroutine
WithHooks(h Hooks)            // 11 optional panic-safe callbacks
```

## ERRORS

```go
ErrClosed, ErrDraining, ErrNotRunning, ErrCorrupted, ErrQueueFull,
ErrFileLocked, ErrInvalidLSN, ErrShortWrite, ErrInvalidRecord,
ErrCRCMismatch, ErrInvalidState, ErrEmptyBatch, ErrCompressorRequired
```

## TYPES

```go
type LSN = uint64
type Event struct { LSN LSN; Meta []byte; Payload []byte }
type Batch struct { Events []Event }  // NewBatch(n), Add, AddWithMeta, Reset, Len
type State int32  // StateInit → StateRunning → StateDraining → StateClosed
```

---

## INTERNALS

### Pipeline

```
Append → atomic LSN → writeQueue → writer goroutine → Storage
                          ▲               │
                     Flush barrier    encode → compress → writeAll → maybeSync → indexer
```

### Writer loop

```
dequeue → processBatch → drainAdditional (group commit) → flushBuffer → close barriers
```

- Group commit: drains ALL available batches into one write syscall
- `maybeSync`: SyncBatch=every write, SyncInterval=on ticker (only if dirty), SyncNever=skip
- `notifyIndexer`: re-decodes just-written buffer (always succeeds)

### Recovery (Open)

1. mmap file → sequential `decodeBatchFrame` scan → track `lastValid` offset
2. On CRC error: `Truncate(lastValid)` — entire corrupted batch discarded
3. Restore LSN counter from last valid event
4. Idempotent: crash during recovery → next Open reaches same result

### Concurrency

- Append: any goroutine (atomic LSN + queue cond)
- Writer: single goroutine (no mutex in encode/write)
- Replay/Iterator: any goroutine (mmap snapshot at call time)
- Stats: any goroutine (atomic loads)
- FileStorage: mutex-protected for concurrent Read+Write

### Batch frame format (v2)

Header 24B: Magic("UWAL") + Version(2) + Flags + RecordCount + FirstLSN + BatchSize.
Per-record: PayloadLen(4B) + MetaLen(2B) + Meta + Payload.
Trailer: CRC32C(4B) covering header+records.
Compressed flag in Flags bit 0; CRC covers compressed bytes.

---

## MISTAKES TO AVOID

1. **Holding mmap'd data** — copy Payload/Meta if needed beyond callback
2. **Assuming Append=durable** — use Flush()+Sync() for durability
3. **Iterator after Close** — returns ErrClosed
4. **Blocking Indexer** — stalls entire write pipeline
5. **Not closing Iterator** — leaks mmap mapping
6. **Multi-process access** — flock prevents it; use one WAL per process
7. **Nil Compressor + compressed data** — ErrCompressorRequired
8. **Ignoring Flush() error** — surfaces writer's last error

## NOT IN SCOPE

No replication, no queries, no compaction, no log rotation (v0.2.0), no multi-process.
