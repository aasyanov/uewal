package uewal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Write writes a batch atomically. Copies records internally so the batch
// can be safely reused (Reset, Append) immediately after Write returns.
func (w *WAL) Write(batch *Batch) (LSN, error) {
	if batch == nil || len(batch.records) == 0 {
		return 0, ErrEmptyBatch
	}
	recs, pool := getRecordSlice(len(batch.records))
	copy(recs, batch.records)
	return w.appendRecords(recs, pool, batch.noCompress, batch.tsUniform)
}

// WriteUnsafe writes a batch atomically. Zero-copy: the batch's records are
// passed directly to the writer goroutine. The batch must NOT be reused
// (Reset, Append) until [WAL.Flush] completes. Faster for fire-and-forget.
func (w *WAL) WriteUnsafe(batch *Batch) (LSN, error) {
	if batch == nil || len(batch.records) == 0 {
		return 0, ErrEmptyBatch
	}
	return w.appendRecords(batch.records, nil, batch.noCompress, batch.tsUniform)
}

// WAL is the main write-ahead log structure.
// Create via [Open]. All methods are safe for concurrent use.
//
// Lifecycle: INIT → RUNNING → DRAINING → CLOSED
type WAL struct {
	cfg     config
	mgr     *segmentManager
	queue   *writeQueue
	writer  *writer
	sm      stateMachine
	stats   statsCollector
	lsn     lsnCounter
	hooks   hooksRunner
	durable durableNotifier

	dir       string
	lockFile  *os.File
	durableMu sync.Mutex

	shutdownOnce sync.Once
	closeOnce    sync.Once
	shutdownDone chan error
}

// Open creates or opens a segmented WAL in the given directory.
// Creates the directory if it does not exist. Acquires a LOCK file
// to prevent concurrent access.
func Open(dir string, opts ...Option) (*WAL, error) {
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}

	if err := os.MkdirAll(dir, defaultDirMode); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCreateDir, err)
	}

	lockPath := filepath.Join(dir, lockFileName)
	lockF, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, defaultFileMode)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrLockFile, err)
	}
	if _, err := lockFile(lockF); err != nil {
		lockF.Close()
		return nil, ErrDirectoryLocked
	}

	w := &WAL{
		cfg:          cfg,
		dir:          dir,
		lockFile:     lockF,
		hooks:        hooksRunner{h: cfg.hooks},
		shutdownDone: make(chan error, 1),
	}

	mgr, firstLSN, lastLSN, mgrErr := openSegmentManager(dir, cfg, &w.hooks, &w.stats)
	if mgrErr != nil {
		unlockFile(fileLock{f: lockF})
		lockF.Close()
		return nil, mgrErr
	}
	w.mgr = mgr

	w.lsn.store(lastLSN)
	if lastLSN > 0 {
		w.stats.storeLSN(lastLSN)
	}
	if firstLSN > 0 {
		w.stats.storeFirstLSN(firstLSN)
	}

	if cfg.startLSN > 0 && lastLSN == 0 {
		w.lsn.store(cfg.startLSN - 1)
	}

	w.queue = newWriteQueue(cfg.queueSize)
	w.writer = newWriter(mgr, w.queue, cfg, &w.stats, &w.hooks, &w.durable)

	if !w.sm.transition(StateInit, StateRunning) {
		_ = mgr.closeActive()
		unlockFile(fileLock{f: lockF})
		lockF.Close()
		return nil, ErrInvalidState
	}

	w.writer.start()
	w.hooks.onStart()

	return w, nil
}


// Flush blocks until all pending writes are persisted.
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

// Sync fsyncs the active segment's storage.
func (w *WAL) Sync() error {
	if err := w.sm.mustBeRunning(); err != nil {
		return err
	}
	active := w.mgr.active()
	if active.storage != nil {
		return active.storage.Sync()
	}
	return nil
}

// Replay iterates events with LSN >= from across all segments.
func (w *WAL) Replay(from LSN, fn func(Event) error) error {
	switch w.sm.load() {
	case StateInit:
		return ErrNotRunning
	case StateClosed:
		return ErrClosed
	}
	return replaySegments(w.mgr, from, fn, w.cfg.compressor)
}

// Iterator returns a cross-segment iterator starting from the given LSN.
func (w *WAL) Iterator(from LSN) (*Iterator, error) {
	switch w.sm.load() {
	case StateInit:
		return nil, ErrNotRunning
	case StateClosed:
		return nil, ErrClosed
	}
	return newCrossSegmentIterator(w.mgr, from, w.cfg.compressor)
}

// Rotate manually triggers segment rotation.
// The rotation executes inside the writer goroutine to avoid races.
func (w *WAL) Rotate() error {
	if err := w.sm.mustBeRunning(); err != nil {
		return err
	}
	barrier := make(chan struct{})
	wb := writeBatch{barrier: barrier, rotate: true}
	if !w.queue.enqueue(wb) {
		return ErrClosed
	}
	<-barrier
	return w.writer.writeErr()
}

// Segments returns information about all current segments.
func (w *WAL) Segments() []SegmentInfo {
	return w.mgr.segmentsSnapshot()
}

// WaitDurable blocks until the given LSN has been fsync'd to disk.
// Uses coalesced fsync: multiple concurrent callers share one fsync.
// Works with any SyncMode; in SyncNever, triggers a one-shot fsync.
func (w *WAL) WaitDurable(lsn LSN) error {
	if err := w.sm.mustBeRunning(); err != nil {
		return err
	}
	if w.durable.syncedTo.Load() >= lsn {
		return nil
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if w.cfg.syncMode == SyncNever || w.cfg.syncMode == SyncInterval {
		if err := w.durableSync(); err != nil {
			return err
		}
	}
	w.durable.wait(lsn)
	return nil
}

// ReplayRange iterates events with from <= LSN <= to.
func (w *WAL) ReplayRange(from, to LSN, fn func(Event) error) error {
	if to < from {
		return ErrLSNOutOfRange
	}
	switch w.sm.load() {
	case StateInit:
		return ErrNotRunning
	case StateClosed:
		return ErrClosed
	}
	return replaySegments(w.mgr, from, func(ev Event) error {
		if ev.LSN > to {
			return errStopReplay
		}
		return fn(ev)
	}, w.cfg.compressor)
}

// ReplayBatches iterates batch frames, calling fn with all events of each batch.
// Useful for replication receivers and batch-aware consumers.
func (w *WAL) ReplayBatches(from LSN, fn func(batch []Event) error) error {
	switch w.sm.load() {
	case StateInit:
		return ErrNotRunning
	case StateClosed:
		return ErrClosed
	}
	return replayBatchesSegments(w.mgr, from, fn, w.cfg.compressor)
}

// DeleteBefore removes all sealed segments whose LastLSN < lsn.
// The active segment is never deleted. Segments with active iterators
// are skipped (will be retried on next call).
func (w *WAL) DeleteBefore(lsn LSN) error {
	if err := w.sm.mustBeRunning(); err != nil {
		return err
	}
	w.mgr.deleteBefore(lsn, w.hooks)
	w.mgr.persistManifest(w.lsn.current())
	return nil
}

// DeleteOlderThan removes all sealed segments whose LastTimestamp < ts (UnixNano).
// The active segment is never deleted. Segments with active iterators are skipped.
func (w *WAL) DeleteOlderThan(ts int64) error {
	if err := w.sm.mustBeRunning(); err != nil {
		return err
	}
	w.mgr.deleteOlderThan(ts, w.hooks)
	w.mgr.persistManifest(w.lsn.current())
	return nil
}

// Snapshot executes fn with a SnapshotController that provides read access
// and compaction control. Writes continue concurrently. The callback can
// iterate events, set a checkpoint, and compact old segments.
func (w *WAL) Snapshot(fn func(ctrl *SnapshotController) error) error {
	switch w.sm.load() {
	case StateInit:
		return ErrNotRunning
	case StateClosed:
		return ErrClosed
	}
	ctrl := &SnapshotController{w: w}
	return fn(ctrl)
}

// FirstLSN returns the lowest LSN in the WAL.
func (w *WAL) FirstLSN() LSN { return w.stats.firstLSN.Load() }
// LastLSN returns the highest LSN written.
func (w *WAL) LastLSN() LSN { return w.stats.loadLSN() }

// Dir returns the WAL directory path.
func (w *WAL) Dir() string { return w.dir }

// State returns the current lifecycle state.
func (w *WAL) State() State { return w.sm.load() }

// Stats returns a snapshot of WAL statistics.
func (w *WAL) Stats() Stats {
	st := w.sm.load()
	var totalSize, activeSize int64
	var queueSize, segCount int
	if st != StateClosed {
		totalSize = w.mgr.totalSize()
		active := w.mgr.active()
		activeSize = active.writeOff.Load()
		queueSize = w.queue.size()
		segCount = w.mgr.segmentCount()
	}
	return w.stats.snapshot(queueSize, totalSize, activeSize, segCount, st)
}

// Shutdown gracefully drains and closes the WAL.
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

			// Final sync before waking durable waiters.
			active := w.mgr.active()
			if active.storage != nil {
				if err := active.storage.Sync(); err != nil && firstErr == nil {
					firstErr = err
				}
			}
			w.durable.wakeAll()

			lastLSN := w.lsn.current()
			active = w.mgr.active()
			active.storeLastLSN(lastLSN)
			active.storeSize(w.writer.writeOffset)
			w.mgr.persistManifest(lastLSN)

			if err := w.mgr.closeActive(); err != nil && firstErr == nil {
				firstErr = err
			}

			w.releaseLock()

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

// Close immediately stops the WAL without draining.
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
		w.writer.shutdown()

		lastLSN := w.lsn.current()
		active := w.mgr.active()

		if active.storage != nil {
			_ = active.storage.Sync()
		}
		w.durable.wakeAll()
		active.storeLastLSN(lastLSN)
		active.storeSize(w.writer.writeOffset)
		w.mgr.persistManifest(lastLSN)

		closeErr = w.mgr.closeActive()
		w.releaseLock()
		w.sm.transition(StateDraining, StateClosed)
	})
	return closeErr
}

func (w *WAL) releaseLock() {
	if w.lockFile != nil {
		unlockFile(fileLock{f: w.lockFile})
		w.lockFile.Close()
		w.lockFile = nil
	}
}
