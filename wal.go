package uewal

import (
	"context"
	"sync"
	"time"
)

// WAL is the main write-ahead log structure.
//
// Create instances exclusively via [Open]. All methods are safe for
// concurrent use from multiple goroutines.
//
// A WAL progresses through four lifecycle states:
//
//	INIT → RUNNING → DRAINING → CLOSED
//
// Once closed, a WAL instance cannot be reopened. To continue using
// the same file, call [Open] again to create a new instance.
type WAL struct {
	cfg     config
	storage Storage
	queue   *writeQueue
	writer  *writer
	sm      stateMachine
	stats   statsCollector
	lsn     lsnCounter
	hooks   hooksRunner
	path    string

	shutdownOnce sync.Once
	closeOnce    sync.Once
	shutdownDone chan error
}

// Open creates or opens a WAL at the given path.
//
// If no custom [Storage] is provided via [WithStorage], a [FileStorage]
// is created at the given path. When a custom Storage is used, path is
// stored for informational purposes only.
//
// On open, the WAL scans existing records to recover the last valid LSN.
// If corruption is detected, the file is truncated to the last valid
// boundary. After recovery, the writer goroutine is started and the WAL
// transitions to [StateRunning].
//
// Returns an error if storage cannot be opened, the file is locked by
// another instance, or recovery fails.
func Open(path string, opts ...Option) (*WAL, error) {
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}

	var s Storage
	if cfg.storage != nil {
		s = cfg.storage
	} else {
		fs, err := NewFileStorage(path)
		if err != nil {
			return nil, err
		}
		s = fs
	}

	w := &WAL{
		cfg:          cfg,
		storage:      s,
		path:         path,
		hooks:        hooksRunner{h: cfg.hooks},
		shutdownDone: make(chan error, 1),
	}

	if err := w.recoverLSN(); err != nil {
		s.Close()
		return nil, err
	}

	startOffset, _ := s.Size()
	w.queue = newWriteQueue(cfg.queueSize)
	w.writer = newWriter(s, w.queue, cfg, &w.stats, &w.hooks, startOffset)

	if !w.sm.transition(StateInit, StateRunning) {
		s.Close()
		return nil, ErrInvalidState
	}

	w.writer.start()
	w.hooks.onStart()

	return w, nil
}

// recoverLSN scans existing batch frames to find the last valid LSN.
// If corruption is detected, the file is truncated to the last valid
// batch boundary.
//
// Uses header-only scanning: the batch header contains FirstLSN and
// RecordCount, so lastLSN = FirstLSN + RecordCount - 1 without
// decoding individual records.
func (w *WAL) recoverLSN() error {
	size, err := w.storage.Size()
	if err != nil {
		return err
	}
	if size == 0 {
		return nil
	}

	reader, err := newMmapReader(w.storage, size)
	if err != nil {
		return err
	}

	data := reader.bytes()
	off := 0
	var lastLSN LSN
	lastValid := 0
	corrupted := false

	for off < len(data) {
		firstLSN, count, next, decErr := scanBatchHeader(data, off)
		if decErr != nil {
			corrupted = true
			break
		}
		if count > 0 {
			batchLastLSN := firstLSN + uint64(count) - 1
			if batchLastLSN > lastLSN {
				lastLSN = batchLastLSN
			}
		}
		lastValid = next
		off = next
	}

	reader.close()

	if corrupted {
		w.stats.addCorruption()
		w.hooks.onCorruption(int64(lastValid))
		if truncErr := w.storage.Truncate(int64(lastValid)); truncErr != nil {
			return truncErr
		}
	}

	w.lsn.store(lastLSN)
	w.stats.storeLSN(lastLSN)
	return nil
}

// Append writes one or more events to the WAL.
//
// Each event is assigned a unique, monotonically increasing [LSN].
// Returns the LSN of the last event written. The events are enqueued
// to the writer goroutine according to the configured [BackpressureMode].
//
// The caller's slice is not mutated: events are copied before LSN assignment.
//
// Returns [ErrDraining] if the WAL is shutting down, [ErrClosed] if closed,
// [ErrEmptyBatch] if no events are provided, or [ErrQueueFull] in ErrorMode.
func (w *WAL) Append(events ...Event) (LSN, error) {
	return w.appendEvents(events)
}

// AppendBatch writes a batch of events to the WAL.
//
// Events are assigned contiguous LSNs and sent to the writer as one unit.
// See [Batch] documentation for atomicity guarantees.
func (w *WAL) AppendBatch(batch Batch) (LSN, error) {
	return w.appendEvents(batch.Events)
}

// Flush blocks until the writer goroutine has processed all currently
// queued batches and written them to storage.
//
// Flush does NOT call fsync — it only guarantees that data has been handed
// to the operating system via write(). To ensure durability (surviving
// power failure), call [WAL.Sync] after Flush.
//
// Returns any write error encountered by the writer goroutine.
func (w *WAL) Flush() error {
	if err := w.sm.mustBeRunning(); err != nil {
		return err
	}
	barrier := make(chan struct{})
	wb := writeBatch{barrier: barrier}
	if !w.queue.enqueue(wb) {
		return ErrClosed
	}
	<-barrier
	return w.writer.writeErr()
}

// Sync ensures all written data is durable on the underlying storage by
// calling [Storage.Sync] (fsync). This is independent of the writer
// goroutine and the configured [SyncMode].
//
// For a complete durability guarantee, call Flush followed by Sync:
//
//	w.Flush()  // wait for writer to process pending batches
//	w.Sync()   // fsync to disk
func (w *WAL) Sync() error {
	if err := w.sm.mustBeRunning(); err != nil {
		return err
	}
	return w.storage.Sync()
}

// Replay reads all events with LSN ≥ from, calling fn for each.
//
// Uses mmap for zero-copy access when [FileStorage] is used. The Event
// passed to fn has a Payload that references mapped memory; if the data
// must outlive the callback, the caller must copy it.
//
// If corruption is detected, Replay stops at the last valid record
// boundary, truncates the file, and returns nil (unless fn returned
// an error first).
//
// Replay is allowed in any state except [StateInit].
func (w *WAL) Replay(from LSN, fn func(Event) error) error {
	switch w.sm.load() {
	case StateInit:
		return ErrNotRunning
	case StateClosed:
		return ErrClosed
	}
	return replayCallback(w.storage, from, fn, w.cfg.compressor, &w.stats, &w.hooks)
}

// Iterator returns a sequential read iterator starting from the given LSN.
//
// The caller must call [Iterator.Close] when done to release mmap resources.
// Returns an error to distinguish initialization failures (e.g., storage
// errors) from an empty WAL.
//
// Allowed in [StateRunning] and [StateDraining].
func (w *WAL) Iterator(from LSN) (*Iterator, error) {
	switch w.sm.load() {
	case StateInit:
		return nil, ErrNotRunning
	case StateClosed:
		return nil, ErrClosed
	}
	return newIterator(w.storage, from, w.cfg.compressor)
}

// LastLSN returns the most recently persisted LSN as seen by the writer
// goroutine. This may lag behind the LSN returned by [WAL.Append], which
// reflects assignment (enqueue), not persistence (write completion).
func (w *WAL) LastLSN() LSN {
	return w.stats.loadLSN()
}

// Stats returns a point-in-time snapshot of WAL statistics.
//
// Safe to call in any state, including after [WAL.Close] or [WAL.Shutdown].
// When the WAL is closed, FileSize and QueueSize are reported as zero.
func (w *WAL) Stats() Stats {
	st := w.sm.load()
	var fileSize int64
	var queueSize int
	if st != StateClosed {
		fileSize, _ = w.storage.Size()
		queueSize = w.queue.size()
	}
	return w.stats.snapshot(queueSize, fileSize, st)
}

// Shutdown performs a graceful shutdown of the WAL:
//
//  1. Transitions from RUNNING to DRAINING (new appends are rejected).
//  2. Closes the write queue and waits for the writer goroutine to drain
//     all remaining batches.
//  3. Flushes any residual encoder data to storage.
//  4. Calls fsync if SyncMode is not SyncNever.
//  5. Closes the storage.
//  6. Transitions to CLOSED.
//
// Shutdown respects the provided context for cancellation and timeouts.
// If the context is canceled before shutdown completes, ctx.Err() is
// returned, but the background shutdown goroutine continues to completion.
// A subsequent call to Shutdown (with a valid context) returns the result.
//
// Idempotent: calling Shutdown on an already-closed WAL returns nil.
func (w *WAL) Shutdown(ctx context.Context) error {
	if w.sm.load() == StateClosed {
		return nil
	}

	w.shutdownOnce.Do(func() {
		if !w.sm.transition(StateRunning, StateDraining) {
			if w.sm.load() == StateClosed || w.sm.load() == StateDraining {
				w.shutdownDone <- nil
				return
			}
			w.shutdownDone <- ErrInvalidState
			return
		}

		start := time.Now()
		w.hooks.onShutdownStart()

		go func() {
			w.writer.stop()

			firstErr := w.writer.flushAfterStop()
			if w.cfg.syncMode != SyncNever {
				if err := w.storage.Sync(); err != nil && firstErr == nil {
					firstErr = err
				}
			}
			if err := w.storage.Close(); err != nil && firstErr == nil {
				firstErr = err
			}

			w.sm.transition(StateDraining, StateClosed)
			w.hooks.onShutdownDone(time.Since(start))
			w.shutdownDone <- firstErr
		}()
	})

	select {
	case err := <-w.shutdownDone:
		w.shutdownDone <- err
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close performs an immediate close without draining the write queue.
//
// Any batches still in the queue are discarded. The writer goroutine is
// stopped via queue closure. This is suitable for abnormal termination;
// for graceful shutdown with data preservation, use [WAL.Shutdown].
//
// Idempotent: calling Close on an already-closed WAL returns nil.
func (w *WAL) Close() error {
	if w.sm.load() == StateClosed {
		return nil
	}

	var closeErr error
	w.closeOnce.Do(func() {
		if w.sm.load() == StateRunning {
			w.sm.transition(StateRunning, StateDraining)
		}

		w.queue.close()
		w.writer.wg.Wait()

		closeErr = w.storage.Close()
		w.sm.transition(StateDraining, StateClosed)
	})
	return closeErr
}
