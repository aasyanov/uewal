# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] — 2026-03-17

Multi-segment WAL with rotation, retention, replication, mmap recovery, and comprehensive quality hardening.

### Added — Architecture

- **Multi-segment WAL**: automatic segment rotation by size (`WithMaxSegmentSize`), age (`WithMaxSegmentAge`), and count limit (`WithMaxSegments`).
- **Segment manifest**: binary manifest file for fast recovery — replaces full scan of all `.wal` files. Manifest is updated atomically (tmp + rename) on rotation, deletion, and shutdown.
- **Sparse index**: per-segment LSN-to-offset index (`.idx` files) with O(log n) binary search via `findByLSN` / `findByTimestamp`. Parallel loading at recovery.
- **Segment retention**: `WithRetentionSize`, `WithRetentionAge`, `DeleteBefore(lsn)`, `DeleteOlderThan(ts)` for automatic and manual segment cleanup.
- **Segment preallocation**: `WithPreallocate(true)` for contiguous on-disk allocation, reducing fragmentation.
- **Mmap-based recovery**: active segment is scanned via memory-mapped I/O instead of `os.ReadFile`, eliminating a full-file heap allocation during `Open`. Recovery memory footprint is now independent of payload size.
- **Warm page cache**: sealed segments are mmap-pre-warmed at `Open` concurrently with sparse index loading, stabilizing first-replay latency.

### Added — Public API

- `Write(batch)` / `WriteUnsafe(batch)` — replaces `Append`/`AppendBatch`. All writes go through `Batch`.
- `Batch.Append(payload, key, meta, opts...)` / `Batch.AppendUnsafe(...)` — per-record `key` and `meta` fields, with `WithTimestamp` and `WithNoCompress` record options.
- `Batch.AppendWithTimestamp(payload, key, meta, ts)` / `Batch.AppendUnsafeWithTimestamp(...)` — direct timestamp API without closure allocation.
- `Batch.MarkNoCompress()` — sets no-compress flag on the entire batch without per-record closure allocations.
- `Follow(from LSN)` — live tail-follow iterator with blocking `Next()`, automatic segment crossing, and wake-on-write.
- `Snapshot(fn)` — consistent read snapshot with `SnapshotController` for iteration, checkpoint, and compaction during concurrent writes.
- `SnapshotController.Iterator()` / `IteratorFrom(lsn)` / `Segments()` / `Checkpoint(lsn)` / `CheckpointOlderThan(ts)` / `Compact()`.
- `Rotate()` — manual segment rotation.
- `Segments()` — returns `[]SegmentInfo` for all segments.
- `WaitDurable(lsn)` — blocks until the given LSN is fsynced; uses coalesced `durableNotifier`. Correctly triggers one-shot sync for all sync modes.
- `ReplayRange(from, to, fn)` — bounded replay within an LSN range.
- `ReplayBatches(from, fn)` — batch-level replay callback.
- `DeleteBefore(lsn)` / `DeleteOlderThan(ts)` — explicit retention trimming.
- `Dir()` — returns the WAL directory path.
- `FirstLSN()` / `LastLSN()` — first and last LSN accessors.
- `State()` — returns the current lifecycle state.
- `OpenSegment(firstLSN)` — opens a sealed segment for raw reading (segment shipping).
- `ImportBatch(frame)` — imports a raw batch frame from a primary (replication).
- `ImportSegment(path)` — imports a sealed segment file from a primary.
- `Event.Key` — per-event routing key (zero-cost when nil).
- `Event.Timestamp` — per-event nanosecond timestamp.
- `IndexInfo` struct with `LSN`, `Timestamp`, `Key`, `Meta`, `Offset`, `Segment` fields.
- `SegmentInfo` struct with `Path`, `FirstLSN`, `LastLSN`, `FirstTimestamp`, `LastTimestamp`, `Size`, `CreatedAt`, `Sealed`.
- `RecoveryInfo` struct with `SegmentCount`, `TruncatedBytes`, `Corrupted`.
- `StorageFactory` function type via `WithStorageFactory`.
- `WithStartLSN(lsn)` — initial LSN for fresh WALs.
- `ScratchCompressor` interface extending `Compressor` with `CompressTo(dst, src)` / `DecompressTo(dst, src)` for buffer reuse on the hot path.

### Added — Sync Modes

- `SyncCount` — fsync after every N batches via `WithSyncCount(n)`.
- `SyncSize` — fsync after every N bytes written via `WithSyncSize(n)`.

### Added — Optimizations

- **Cache-line padding** on `lsnCounter` and `queueSlot` to prevent false sharing.
- **Tiered payload buffer pool** (`payloadPools` with 6 size classes: 64B–4KB) to reduce allocation pressure on the copy path.
- **Binary search** for segment lookup in `acquireSegments` — O(log k) instead of O(k) linear scan.
- **`encoder.encodeBatchHint`**: pre-sized encoding buffer avoids repeated grow in steady state.
- **`resolveStorageFastPath`**: avoids interface dispatch on every write via method-value caching.
- **Coalesced fsync** via `durableNotifier` — multiple `WaitDurable` callers share a single fsync.
- **Batch frame v1** with per-record timestamp support: uniform-timestamp optimization (8B batch timestamp vs 8B per record) when all records share the same timestamp.
- **Mmap page cache warming** at `Open` for sealed segments — eliminates cold page fault latency on first replay.
- **Scratch-buffer compression**: `ScratchCompressor` avoids per-call allocations on the compression hot path.
- **Zero-allocation custom errors** (`syncErr`) in the sync path.

### Changed

- **Wire format**: batch header is 28 bytes (v1); per-record overhead is 8–16 bytes depending on timestamp mode. Fields: `Magic(4) Version(1) Flags(1) Count(2) FirstLSN(8) Timestamp(8) BatchSize(4)`. Per-record: `[Timestamp(8)] KeyLen(2) MetaLen(2) PayloadLen(4) Key Meta Payload`. Payload-only fast path: zeroed `KeyLen+MetaLen` as single uint32.
- **API**: `Append(events...)` / `AppendBatch(batch)` replaced by `Write(batch)` / `WriteUnsafe(batch)`. `batch.Add`/`AddWithMeta` replaced by `batch.Append(payload, key, meta, opts...)`.
- **Indexer interface**: `OnAppend(lsn LSN, meta []byte, offset int64)` → `OnAppend(info IndexInfo)`.
- **Hooks**: revised signatures — `AfterAppend(firstLSN, lastLSN LSN, count int)`, `AfterWrite(bytes int, elapsed time.Duration)`, `AfterSync(bytes int, elapsed time.Duration)`, `OnCorruption(segmentPath string, offset int64)`. Added `OnRecovery(RecoveryInfo)`, `OnRotation(SegmentInfo)`, `OnDelete(SegmentInfo)`.
- **Stats**: expanded to 19 fields — added `RotationCount`, `RetentionDeleted`, `RetentionBytes`, `LastSyncNano`, `TotalSize`, `ActiveSegmentSize`, `SegmentCount`.
- **Errors**: expanded from 13 to 25+ sentinel errors covering segments, manifest, replication, mmap, sync, directory operations.
- All file operations use named constants (`defaultFileMode`, `defaultDirMode`, `lockFileName`, `walExt`, `idxExt`, `segmentNameFmt`, `manifestTmpExt`).
- Extracted pooling logic to `pool.go`, durability to `durable.go`, test helpers to `helpers_test.go`.

### Fixed

- **106 linter issues** resolved across `errcheck`, `govet` (shadow), `gocritic`, `revive`, `staticcheck`, `unused`, `unparam`, `goconst`.
- **Deadlock in `writeQueue`**: `close()` now correctly signals `dequeueAllInto` via `q.notify` channel after setting the closed flag.
- **`flushAfterStop` residual data loss**: writer now drains all remaining items from the queue after the consumer loop exits.
- **`ImportBatch` LSN handling**: imported frames now correctly advance the WAL's LSN counter to prevent LSN overlap with subsequent writes.
- GoDoc comments for all exported symbols (16 `With*` functions, all public methods, interfaces).
- Test naming normalized to `TestType_Scenario` convention.
- `api_test.go` renamed to `integration_test.go`.

### Test Suite

- 317 test functions, 5 fuzz targets, 136 benchmarks (128 in bench_test.go across 31 categories + 8 CRC), 15 examples.
- Coverage: 90.8% of statements.
- All tests pass with `-race` detector, 0 linter issues (`golangci-lint` with 11 linters).

## [0.2.0] — 2026-03-10

Performance optimizations, wire format update, test suite cleanup.

### Changed

- **Wire format**: batch magic bytes renamed from `UWAL` to `EWAL` (breaking change — no backward compatibility with v0.1.0 files).
- Append hot path: event slices are now pooled via `sync.Pool`, eliminating per-Append heap allocations.
- LSN assignment: single `atomic.Add` per batch instead of per-event, reducing contention under parallel writes.
- Writer loop: `dequeueAllInto` combines dequeue and drain into a single mutex acquisition, lowering lock contention.
- Recovery: `scanBatchHeader` validates batch frames without decoding individual records, significantly faster on large WAL files.
- Iterator: `decodeBatchFrameInto` reuses a decode buffer across `Next()` calls, reducing read-path allocations.

### Fixed

- Test organization: removed duplicate fuzz target (`FuzzDecodeBatchFrame`), moved misplaced tests, removed redundant closed-state tests, replaced vague assertions with precise state-specific ones.
- GoDoc comments updated across all modified files.

### Added

- `TestWithIndex`, `TestCompressionStats`, state-specific replay/iterator tests, `flushAfterStop` branch coverage, `TestMmapReaderFallbackWithCustomStorage`.
- Shared `helpers_test.go` with `memStorage`.
- Test suite: 155 tests, 3 fuzz targets, 14 benchmarks, 6 examples.
- Coverage: 92.1% of statements.

## [0.1.0] — 2026-03-03

Initial public release.

### Added

- Core WAL engine with single-writer goroutine and lock-free LSN assignment.
- `Open`, `Append`, `AppendBatch`, `Flush`, `Sync`, `Shutdown`, `Close` API.
- `Replay(from LSN, fn)` streaming replay with mmap zero-copy reads.
- `Iterator(from LSN)` sequential read interface with mmap backend.
- Batch-framed wire format (v2) with single CRC-32C per batch for true batch atomicity.
- `Event.Meta` field for opaque per-event metadata.
- `Batch` type with `Add`, `AddWithMeta`, `Reset` for poolable batch construction.
- Pluggable `Compressor` interface (`Compress`/`Decompress`) via `WithCompressor`.
- Pluggable `Indexer` interface (`OnAppend`) via `WithIndex`.
- Exported `Storage` interface with `FileStorage` implementation.
- File locking via flock (Unix) / LockFileEx (Windows).
- Platform-specific mmap: `mmap(2)` on Unix, `MapViewOfFile` on Windows.
- Fallback `ReadAt`-based reader for custom `Storage` implementations.
- Three durability modes: `SyncNever`, `SyncBatch`, `SyncInterval`.
- Three backpressure modes: `BlockMode`, `DropMode`, `ErrorMode`.
- Group commit: writer drains all pending batches into a single write syscall.
- Automatic crash recovery: truncate to last valid batch boundary on `Open`.
- Lifecycle state machine: INIT → RUNNING → DRAINING → CLOSED with atomic CAS transitions.
- 11 observability hooks with panic recovery (`Hooks` struct via `WithHooks`).
- Lock-free atomic `Stats` with 13 runtime metrics.
- 13 sentinel errors comparable with `==` and `errors.Is`.
- Functional options pattern for all configuration.
- Comprehensive test suite: 113 tests, 4 fuzz targets, 14 benchmarks.
- Coverage: 91.3% of statements.
- All tests pass with `-race` detector.
- GitHub Actions CI: lint, test (matrix: 2 OS × 3 Go versions), fuzz, benchmark.
- GoDoc documentation for all exported symbols.
- Zero external dependencies (stdlib only).

[Unreleased]: https://github.com/aasyanov/uewal/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/aasyanov/uewal/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/aasyanov/uewal/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/aasyanov/uewal/releases/tag/v0.1.0
