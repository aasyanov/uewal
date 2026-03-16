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
	cfg   config
	mgr   *segmentManager
	queue *writeQueue
	writer *writer
	sm    stateMachine
	stats statsCollector
	lsn   lsnCounter
	hooks hooksRunner

	dir      string
	lockFile *os.File

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
	w.writer = newWriter(mgr, w.queue, cfg, &w.stats, &w.hooks)

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
func (w *WAL) Rotate() error {
	if err := w.sm.mustBeRunning(); err != nil {
		return err
	}
	barrier := make(chan struct{})
	wb := writeBatch{barrier: barrier}
	if !w.queue.enqueue(wb) {
		return ErrClosed
	}
	<-barrier

	lastLSN := w.lsn.current()
	_, err := w.mgr.rotate(lastLSN, w.writer.writeOffset)
	if err != nil {
		return err
	}
	w.writer.storage = w.mgr.active().storage
	w.writer.writeOffset = 0
	w.writer.segmentPath = w.mgr.active().path
	w.writer.segmentLSN = w.mgr.active().firstLSN
	w.writer.segCreatedAt = w.mgr.active().createdAt
	return nil
}

// Segments returns information about all current segments.
func (w *WAL) Segments() []SegmentInfo {
	return w.mgr.segmentsSnapshot()
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

			if w.cfg.syncMode != SyncNever {
				active := w.mgr.active()
				if active.storage != nil {
					if err := active.storage.Sync(); err != nil && firstErr == nil {
						firstErr = err
					}
				}
			}

			lastLSN := w.lsn.current()
			active := w.mgr.active()
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

		lastLSN := w.lsn.current()
		active := w.mgr.active()
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
