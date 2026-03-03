package uewal

import (
	"sync"
	"time"
)

// writer is the single background goroutine responsible for persisting
// events to storage. It implements group commit by draining all immediately
// available batches from the queue into one write syscall.
//
// The writer is started by [WAL.Open] and stopped during [WAL.Shutdown]
// or [WAL.Close]. It is not safe for concurrent use — only one instance
// runs per WAL.
type writer struct {
	storage  Storage
	queue    *writeQueue
	enc      *encoder
	cfg      config
	stats    *statsCollector
	hooks    *hooksRunner
	syncTick *time.Ticker
	done     chan struct{}
	wg       sync.WaitGroup

	drainBuf         []writeBatch
	pendingSyncBytes uint64
	writeOffset      int64
	lastErr          error
}

// newWriter creates a writer bound to the given storage and queue.
// If SyncInterval mode is active, a ticker is started immediately.
func newWriter(s Storage, q *writeQueue, cfg config, stats *statsCollector, hooks *hooksRunner, startOffset int64) *writer {
	w := &writer{
		storage:     s,
		queue:       q,
		enc:         newEncoder(cfg.bufferSize),
		cfg:         cfg,
		stats:       stats,
		hooks:       hooks,
		done:        make(chan struct{}),
		drainBuf:    make([]writeBatch, 0, cfg.queueSize),
		writeOffset: startOffset,
	}
	if cfg.syncMode == SyncInterval {
		w.syncTick = time.NewTicker(cfg.syncInterval)
	}
	return w
}

// start launches the writer goroutine.
func (w *writer) start() {
	w.wg.Add(1)
	go w.loop()
}

// loop is the main writer loop. It dequeues batches, performs group commit
// by draining additional batches, flushes the encoder buffer to storage,
// and signals any flush barriers.
func (w *writer) loop() {
	defer w.wg.Done()
	defer func() {
		if w.syncTick != nil {
			w.syncTick.Stop()
		}
	}()

	for {
		batch, ok := w.queue.dequeue()
		if !ok {
			return
		}
		w.processBatch(batch)
		w.processAdditionalBatches()
		w.flushBuffer()
		if batch.barrier != nil {
			close(batch.barrier)
		}
		for i := range w.drainBuf {
			if w.drainBuf[i].barrier != nil {
				close(w.drainBuf[i].barrier)
			}
			w.drainBuf[i] = writeBatch{}
		}
	}
}

// processBatch encodes a single writeBatch into the encoder buffer
// and updates statistics. Barrier-only batches (no events) are skipped.
func (w *writer) processBatch(b writeBatch) {
	if len(b.events) == 0 {
		return
	}
	sizeBefore := len(w.enc.bytes())
	if err := w.enc.encodeBatch(b.events, b.lsnStart, w.cfg.compressor); err != nil {
		w.lastErr = err
		return
	}
	encoded := len(w.enc.bytes()) - sizeBefore
	uncompressed := batchFrameSize(b.events)
	if w.cfg.compressor != nil && uncompressed > encoded {
		w.stats.addCompressed(uint64(uncompressed - encoded))
	}
	w.stats.addEvents(uint64(len(b.events)))
	w.stats.addBatches(1)
	w.stats.storeLSN(b.lsnEnd)
	w.hooks.afterAppend(b.lsnEnd, len(b.events))
}

// processAdditionalBatches drains all immediately available batches
// to enable group commit. Uses a pre-allocated buffer.
// Barriers in drained batches are closed by the caller (loop) after flush.
func (w *writer) processAdditionalBatches() {
	w.drainBuf = w.queue.drainAllInto(w.drainBuf[:0])
	for i := range w.drainBuf {
		w.processBatch(w.drainBuf[i])
	}
}

// flushBuffer writes the encoded buffer to storage and optionally syncs
// according to the configured SyncMode. After a successful write, it
// notifies the [Indexer] (if configured) for each pending event.
func (w *writer) flushBuffer() {
	buf := w.enc.bytes()
	if len(buf) == 0 {
		return
	}

	w.hooks.beforeWrite(len(buf))

	writeOffset := w.writeOffset
	n, err := w.writeAll(buf)
	w.hooks.afterWrite(n)

	if err != nil {
		w.lastErr = err
		w.enc.reset()
		return
	}

	w.stats.addBytes(uint64(n))
	w.writeOffset += int64(n)
	w.enc.reset()

	if w.cfg.indexer != nil {
		w.notifyIndexer(buf, writeOffset)
	}

	w.maybeSync(uint64(n))
}

// writeAll ensures the full buffer is written to storage, retrying on
// short writes. Returns [ErrShortWrite] if Write returns n=0 without
// an error (preventing an infinite loop).
func (w *writer) writeAll(buf []byte) (int, error) {
	total := 0
	for len(buf) > 0 {
		n, err := w.storage.Write(buf)
		total += n
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, ErrShortWrite
		}
		buf = buf[n:]
	}
	return total, nil
}

// maybeSync calls fsync according to the configured SyncMode.
func (w *writer) maybeSync(written uint64) {
	switch w.cfg.syncMode {
	case SyncBatch:
		w.doSync(written)
	case SyncInterval:
		w.pendingSyncBytes += written
		select {
		case <-w.syncTick.C:
			w.doSync(w.pendingSyncBytes)
			w.pendingSyncBytes = 0
		default:
		}
	}
}

// doSync performs an fsync, fires hooks, and updates stats.
func (w *writer) doSync(written uint64) {
	w.hooks.beforeSync()
	start := time.Now()
	err := w.storage.Sync()
	elapsed := time.Since(start)
	w.hooks.afterSync(elapsed)
	if err == nil {
		w.stats.addSynced(written)
		w.stats.addSync()
	}
}

// flushAfterStop writes any remaining encoder data to storage.
// Must only be called after the writer goroutine has exited (via stop).
// Returns the last write error encountered during the loop, if any.
func (w *writer) flushAfterStop() error {
	buf := w.enc.bytes()
	if len(buf) > 0 {
		n, err := w.writeAll(buf)
		if err != nil {
			return err
		}
		w.stats.addBytes(uint64(n))
		w.enc.reset()
	}
	return w.lastErr
}

// stop signals the writer to finish by closing the queue, waits for
// the writer goroutine to exit, and closes the done channel.
func (w *writer) stop() {
	w.queue.close()
	w.wg.Wait()
	close(w.done)
}

// notifyIndexer walks batch frames in buf and calls Indexer.OnAppend
// for each event. Panics are recovered via safeCall.
func (w *writer) notifyIndexer(buf []byte, baseOffset int64) {
	off := 0
	for off < len(buf) {
		events, next, err := decodeBatchFrame(buf, off, w.cfg.compressor)
		if err != nil {
			break
		}
		frameOff := baseOffset + int64(off)
		for i := range events {
			safeCall(func() {
				w.cfg.indexer.OnAppend(events[i].LSN, events[i].Meta, frameOff)
			})
		}
		off = next
	}
}

// writeErr returns the last write error encountered by the writer, if any.
// Used by [WAL.Flush] to surface errors to the caller.
func (w *writer) writeErr() error {
	return w.lastErr
}
