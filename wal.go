package uewal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

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

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("uewal: create dir: %w", err)
	}

	lockPath := filepath.Join(dir, "LOCK")
	lockF, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("uewal: open lock file: %w", err)
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
		mgr.closeActive()
		unlockFile(fileLock{f: lockF})
		lockF.Close()
		return nil, ErrInvalidState
	}

	w.writer.start()
	w.hooks.onStart()

	return w, nil
}

// Append writes a single event. Always copies payload.
func (w *WAL) Append(payload []byte, opts ...RecordOption) (LSN, error) {
	return w.singleAppend(payload, opts...)
}

// AppendBatch writes a batch atomically. One CRC per batch.
func (w *WAL) AppendBatch(batch *Batch) (LSN, error) {
	if batch == nil || len(batch.records) == 0 {
		return 0, ErrEmptyBatch
	}
	return w.appendRecords(batch.records, nil, batch.noCompress)
}

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
		w.durableSync()
	}
	w.durable.wait(lsn)
	return nil
}

// durableSync performs a mutex-protected fsync + advance.
// Multiple concurrent callers coalesce: the first does the fsync,
// subsequent callers find syncedTo already advanced and skip.
func (w *WAL) durableSync() {
	w.durableMu.Lock()
	defer w.durableMu.Unlock()

	currentLSN := w.lsn.current()
	if w.durable.syncedTo.Load() >= currentLSN {
		return
	}
	active := w.mgr.active()
	if active.storage != nil {
		if err := active.storage.Sync(); err != nil {
			return
		}
	}
	w.durable.advance(currentLSN)
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

func (w *WAL) FirstLSN() LSN { return w.stats.firstLSN.Load() }
func (w *WAL) LastLSN() LSN  { return w.stats.loadLSN() }

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
			active.lastLSN = lastLSN
			active.size = w.writer.writeOffset
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
			active.storage.Sync()
		}
		w.durable.wakeAll()
		active.lastLSN = lastLSN
		active.size = w.writer.writeOffset
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
