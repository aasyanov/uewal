# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/aasyanov/uewal/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/aasyanov/uewal/releases/tag/v0.1.0
