// Package uewal implements a high-performance segmented write-ahead log engine.
//
// UEWAL is a building block for event sourcing, message brokers, embedded
// database WALs, replication logs, CDC pipelines, and durable queues. It
// provides batch-first writes, zero-copy mmap replay, crash-safe recovery,
// and extensible indexing — all behind a single [WAL] object.
//
// # Architecture
//
// UEWAL operates as an always-segmented log with a single writer goroutine:
//
//	user code ──► WAL (public API)
//	               ├── segment manager   rotation, retention, manifest
//	               ├── segment writer    append pipeline, encoding, fsync
//	               └── replay engine     mmap, iterator, sparse index
//
// # Quick Start
//
//	w, err := uewal.Open("/var/data/mywal",
//	    uewal.WithSyncMode(uewal.SyncBatch),
//	    uewal.WithMaxSegmentSize(256 << 20),
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer w.Shutdown(context.Background())
//
//	// Single event
//	batch := uewal.NewBatch(1)
//	batch.Append([]byte("hello"), []byte("user-123"), []byte("created"))
//	lsn, err := w.Write(batch)
//
//	// Batch (atomic, one CRC)
//	batch = uewal.NewBatch(100)
//	for i := 0; i < 100; i++ {
//	    batch.Append(payload, nil, nil)
//	}
//	lsn, err = w.Write(batch)
//
//	// Replay
//	err = w.Replay(0, func(ev uewal.Event) error {
//	    fmt.Printf("LSN=%d Key=%s\n", ev.LSN, ev.Key)
//	    return nil
//	})
//
// # Wire Format
//
// Each batch is encoded as a self-contained frame with a 28-byte header,
// variable-length records, and a CRC-32C trailer. Records carry optional
// Key, Meta, and Payload fields with 8 or 16 bytes of overhead depending
// on whether per-record timestamps are used.
//
// # Durability
//
// Five sync modes control the trade-off between throughput and durability.
// All modes except [SyncBatch] have a data loss window — a period during
// which acknowledged writes may be lost on crash:
//
//   - [SyncNever]: no explicit fsync (max throughput, highest data loss risk)
//   - [SyncBatch]: fsync after every batch (no data loss window)
//   - [SyncInterval]: fsync at time intervals (loss window = interval)
//   - [SyncCount]: fsync every N batches (loss window = N-1 batches)
//   - [SyncSize]: fsync every N bytes (loss window = N-1 bytes)
//
// # Extensibility
//
// The WAL supports pluggable [Compressor], [Indexer], and [Hooks] interfaces
// for compression, custom indexing, and observability without modifying core
// code. The [Storage] interface allows custom persistence backends.
//
// # Crash Recovery
//
// On [Open], the WAL scans existing batch frames, validates CRC checksums,
// and truncates any partially written data. Recovery is fast: only batch
// headers are scanned (no per-record decoding).
package uewal
