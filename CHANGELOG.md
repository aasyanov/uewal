# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] — 2026-03-16

Multi-segment WAL with rotation, retention, replication, and comprehensive quality hardening.

### Added — Architecture

- **Multi-segment WAL**: automatic segment rotation by size (`WithMaxSegmentSize`), age (`WithMaxSegmentAge`), and count limit (`WithMaxSegments`).
- **Segment manifest**: binary manifest file for fast recovery — replaces full scan of all `.wal` files. Manifest is updated atomically on rotation, deletion, and shutdown.
- **Sparse index**: per-segment LSN-to-offset index (`.idx` files) with O(log n) binary search via `findByLSN` / `findByTimestamp`. Pre-allocated capacity via `WithSparseIndexCapacity`.
- **Segment retention**: `WithRetentionSize`, `WithRetentionAge`, `DeleteBefore(lsn)` for automatic and manual segment cleanup.
- **Segment preallocation**: `WithPreallocate(true)` for contiguous on-disk allocation, reducing fragmentation.

### Added — Public API

- `Write(batch)` / `WriteUnsafe(batch)` — replaces `Append`/`AppendBatch`. All writes go through `Batch`.
- `Batch.Append(payload, key, meta, opts...)` / `Batch.AppendUnsafe(...)` — per-record `key` and `meta` fields, with `WithTimestamp` and `WithNoCompress` options.
- `Follow(from LSN)` — live tail-follow iterator with blocking `Next()`, automatic segment crossing, and wake-on-write.
- `Snapshot(fn)` — consistent read snapshot with `SnapshotController` for iteration, checkpoint, and compaction during concurrent writes.
- `Rotate()` — manual segment rotation.
- `Segments()` — returns `[]SegmentInfo` for all segments.
- `WaitDurable(lsn)` — blocks until the given LSN is fsynced; uses coalesced `durableNotifier`.
- `ReplayRange(from, to, fn)` — bounded replay within an LSN range.
- `ReplayBatches(from, fn)` — batch-level replay callback.
- `DeleteBefore(lsn)` — explicit retention trimming.
- `Dir()` — returns the WAL directory path.
- `FirstLSN()` / `LastLSN()` — first and last LSN accessors.
- `OpenSegment(firstLSN)` — opens a sealed segment for raw reading (segment shipping).
- `ImportBatch(frame)` — imports a raw batch frame from a primary (replication).
- `ImportSegment(path)` — imports a sealed segment file from a primary.
- `Event.Key` — per-event routing key (zero-cost when nil).
- `Event.Timestamp` — per-event nanosecond timestamp.
- `IndexInfo` struct passed to `Indexer.OnAppend` callback.
- `SegmentInfo` struct for segment metadata.
- `StorageFactory` function type via `WithStorageFactory`.
- `WithStartLSN(lsn)` — initial LSN for fresh WALs.

### Added — Optimizations

- **Cache-line padding** on `lsnCounter` to prevent false sharing.
- **Tiered payload buffer pool** (`payloadPools` with 6 size classes: 64B–4KB) to reduce allocation pressure on the copy path.
- **Binary search** for segment lookup in `acquireSegments` — O(log k) instead of O(k) linear scan.
- **`encoder.encodeBatchHint`**: pre-sized encoding buffer avoids repeated grow in steady state.
- **`resolveStorageFastPath`**: avoids interface dispatch on every write via method-value caching.
- **Coalesced fsync** via `durableNotifier` — multiple `WaitDurable` callers share a single fsync.
- **Batch frame v1** with per-record timestamp support: uniform-timestamp optimization (8B batch timestamp vs 8B per record) when all records share the same timestamp.

### Changed

- **Wire format**: batch header is now 28 bytes (v1, was 24 bytes v2); per-record overhead is 8–16 bytes depending on timestamp mode (was 6 bytes). Added `KeyLen` (2 bytes) and optional per-record timestamp (8 bytes) fields.
- **API**: `Append(events...)` / `AppendBatch(batch)` replaced by `Write(batch)` / `WriteUnsafe(batch)`. `batch.Add`/`AddWithMeta` replaced by `batch.Append(payload, key, meta, opts...)`.
- All file operations use named constants (`defaultFileMode`, `defaultDirMode`, `lockFileName`, `walExt`, `idxExt`, `segmentNameFmt`, `manifestTmpExt`).
- All batch header field access uses named offset constants (`batchOffMagic`, `batchOffVersion`, `batchOffFlags`, `batchOffCount`, `batchOffLSN`, `batchOffTS`, `batchOffSize`).
- `io.SeekStart` / `io.SeekEnd` replace magic `0` / `2` in all Seek calls.
- Extracted pooling logic to `pool.go`, durability to `durable.go`, test helpers to `helpers_test.go`.

### Fixed

- **106 linter issues** resolved across `errcheck`, `govet` (shadow), `gocritic` (stringXbytes, preferStringWriter, exitAfterDefer), `revive` (exported godoc), `staticcheck` (unused values), `unused` (dead fields/methods), `unparam`, `goconst`.
- `errcheck`: explicit error handling for `writeSparseIndex`, `writeManifest`, `Truncate`, `Seek`, `closeActive`, `Sync`.
- Shadow variables in 46 test locations converted from `:=` to `=`.
- GoDoc comments added for all exported symbols (16 `With*` functions, all public methods, interfaces).
- Test naming normalized to `TestType_Scenario` convention across 75+ test functions.
- `api_test.go` renamed to `integration_test.go`.
- Benchmark suite audited: removed 43 duplicate/non-informative benchmarks (CRC, trivial loops, option variants that don't affect hot path, redundant matrix), fixed incorrect hook signatures, added Follow/Snapshot/Import benchmarks. 100 benchmarks remain across 31 categories.

### Test Suite

- 286 test functions (up from 155), 5 fuzz targets, 100 benchmarks, 6 examples.
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

- Test organization: removed duplicate fuzz target (`FuzzDecodeBatchFrame`), moved misplaced tests (`TestAddWithMeta` to `append_test.go`, `TestReadAllFallbackWithCustomStorage` to `mmap_test.go`), removed redundant `TestFlushOnClosedWAL` (covered by `TestOperationsOnClosedWAL`), replaced vague closed-state tests with precise state-specific assertions.
- GoDoc comments updated across all modified files to accurately reflect the optimized implementations.

### Added

- `TestWithIndex`: validates `WithIndex` option and `Indexer.OnAppend` callback at WAL level.
- `TestCompressionStats`: validates `CompressedBytes` stat counter with a compressor that reduces output size.
- `TestReplayOnInitState`, `TestIteratorOnInitState`: verify `ErrNotRunning` for INIT state.
- `TestReplayOnClosedWALReturnsError`, `TestIteratorOnClosedWALReturnsError`: verify `ErrClosed` for CLOSED state.
- `TestWriterFlushAfterStopWithResidual`, `TestWriterFlushAfterStopPropagatesWriteErr`, `TestWriterFlushAfterStopReturnsLastErr`: cover all `flushAfterStop` branches.
- `TestWriterProcessBatchCompression`: validates compression stats in the writer.
- `TestMmapReaderFallbackWithCustomStorage`: validates `readAllFallback` path.
- Shared `helpers_test.go` with `memStorage` for test DRY.
- Test suite: 155 tests, 3 fuzz targets, 14 benchmarks, 6 examples.
- Test coverage: 92.1% of statements (up from 90.7%).

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
- Test coverage: 91.3% of statements.
- All tests pass with `-race` detector.
- GitHub Actions CI: lint, test (matrix: 2 OS × 3 Go versions), fuzz, benchmark.
- GoDoc documentation for all exported symbols.
- Zero external dependencies (stdlib only).

[Unreleased]: https://github.com/aasyanov/uewal/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/aasyanov/uewal/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/aasyanov/uewal/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/aasyanov/uewal/releases/tag/v0.1.0
