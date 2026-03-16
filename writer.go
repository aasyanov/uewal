package uewal

import (
	"sync"
	"time"
)

// pendingSparseEntry tracks batch metadata during encoding for
// post-write sparse index updates.
type pendingSparseEntry struct {
	firstLSN  LSN
	timestamp int64
	bufOffset int // offset within encoder buffer
}

// writer is the single background goroutine that persists records to storage.
// Implements group commit by draining all available batches per write cycle.
type writer struct {
	mgr     *segmentManager
	storage Storage
	queue   *writeQueue
	enc     *encoder
	cfg     config
	stats   *statsCollector
	hooks   *hooksRunner

	syncTick *time.Ticker
	done     chan struct{}
	wg       sync.WaitGroup

	drainBuf         []writeBatch
	pendingSyncBytes uint64
	writeOffset      int64
	lastErr          error

	segmentPath   string
	segmentLSN    LSN
	segCreatedAt  int64
	pendingSparse []pendingSparseEntry
	lastLSN       LSN // tracks the last LSN written in current cycle
}

func newWriter(mgr *segmentManager, q *writeQueue, cfg config, stats *statsCollector, hooks *hooksRunner) *writer {
	active := mgr.active()
	w := &writer{
		mgr:          mgr,
		storage:      active.storage,
		queue:        q,
		enc:          newEncoder(cfg.bufferSize),
		cfg:          cfg,
		stats:        stats,
		hooks:        hooks,
		done:         make(chan struct{}),
		drainBuf:     make([]writeBatch, 0, cfg.queueSize),
		writeOffset:  active.writeOff.Load(),
		segmentPath:  active.path,
		segmentLSN:   active.firstLSN,
		segCreatedAt: active.createdAt,
	}
	if cfg.syncMode == SyncInterval {
		w.syncTick = time.NewTicker(cfg.syncInterval)
	}
	return w
}

func (w *writer) start() {
	w.wg.Add(1)
	go w.loop()
}

func (w *writer) loop() {
	defer w.wg.Done()
	defer func() {
		if w.syncTick != nil {
			w.syncTick.Stop()
		}
	}()

	for {
		var ok bool
		w.drainBuf, ok = w.queue.dequeueAllInto(w.drainBuf[:0])
		if !ok {
			return
		}
		for i := range w.drainBuf {
			if w.pendingRotation() {
				w.flushBuffer()
			}
			w.processBatch(w.drainBuf[i])
		}
		w.flushBuffer()
		for i := range w.drainBuf {
			if w.drainBuf[i].barrier != nil {
				close(w.drainBuf[i].barrier)
			}
			w.drainBuf[i] = writeBatch{}
		}
	}
}

// pendingRotation returns true if the accumulated buffer would push the
// current segment past MaxSegmentSize. Checked before encoding each batch
// so that rotation happens at batch boundaries within a group commit.
func (w *writer) pendingRotation() bool {
	if w.enc.len() == 0 {
		return false
	}
	currentSize := w.writeOffset + int64(w.enc.len())
	if w.cfg.maxSegmentSize > 0 && currentSize >= w.cfg.maxSegmentSize {
		return true
	}
	return false
}

func (w *writer) processBatch(b writeBatch) {
	if len(b.records) == 0 {
		return
	}

	noCompress := b.noCompress

	w.pendingSparse = append(w.pendingSparse, pendingSparseEntry{
		firstLSN:  b.lsnStart,
		timestamp: b.records[0].timestamp,
		bufOffset: w.enc.len(),
	})

	if err := w.enc.encodeBatch(b.records, b.lsnStart, w.cfg.compressor, noCompress); err != nil {
		w.lastErr = err
		w.pendingSparse = w.pendingSparse[:len(w.pendingSparse)-1]
		w.returnRecords(b)
		return
	}

	n := len(b.records)
	w.returnRecords(b)

	w.stats.addEvents(uint64(n))
	w.stats.addBatches(1)
	w.stats.storeLSN(b.lsnEnd)
	w.lastLSN = b.lsnEnd

	w.hooks.afterAppend(b.lsnStart, b.lsnEnd, n)
}

func (w *writer) returnRecords(b writeBatch) {
	if b.recordPool != nil {
		putRecordSlice(b.recordPool, b.records)
	}
}

func (w *writer) flushBuffer() {
	buf := w.enc.bytes()
	if len(buf) == 0 {
		w.pendingSparse = w.pendingSparse[:0]
		return
	}

	baseOffset := w.writeOffset

	w.hooks.beforeWrite(len(buf))
	start := time.Now()
	n, err := w.writeAll(buf)
	elapsed := time.Since(start)
	w.hooks.afterWrite(n, elapsed)

	if err != nil {
		w.lastErr = err
		w.enc.reset()
		w.pendingSparse = w.pendingSparse[:0]
		return
	}

	w.stats.addBytes(uint64(n))
	w.writeOffset += int64(n)

	active := w.mgr.active()
	active.writeOff.Store(w.writeOffset)

	for _, pe := range w.pendingSparse {
		active.sparse.append(sparseEntry{
			FirstLSN:  pe.firstLSN,
			Offset:    baseOffset + int64(pe.bufOffset),
			Timestamp: pe.timestamp,
		})
		if active.firstTS == 0 {
			active.firstTS = pe.timestamp
		}
		active.lastTS = pe.timestamp
	}
	w.pendingSparse = w.pendingSparse[:0]

	w.enc.reset()

	if w.cfg.indexer != nil {
		w.notifyIndexer(buf, baseOffset)
	}

	w.maybeSync(uint64(n))

	if w.shouldRotate() {
		w.doRotate()
	}
}

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

func (w *writer) doSync(written uint64) {
	w.hooks.beforeSync()
	start := time.Now()
	err := w.storage.Sync()
	elapsed := time.Since(start)
	w.hooks.afterSync(int(written), elapsed)
	if err == nil {
		w.stats.addSynced(written)
		w.stats.addSync()
	}
}

func (w *writer) shouldRotate() bool {
	if w.cfg.maxSegmentSize > 0 && w.writeOffset >= w.cfg.maxSegmentSize {
		return true
	}
	if w.cfg.maxSegmentAge > 0 && w.segCreatedAt > 0 {
		age := time.Since(time.Unix(0, w.segCreatedAt))
		if age >= w.cfg.maxSegmentAge {
			return true
		}
	}
	return false
}

func (w *writer) doRotate() {
	newSeg, err := w.mgr.rotate(w.lastLSN, w.writeOffset)
	if err != nil {
		w.lastErr = err
		return
	}
	w.storage = newSeg.storage
	w.writeOffset = 0
	w.segmentPath = newSeg.path
	w.segmentLSN = newSeg.firstLSN
	w.segCreatedAt = newSeg.createdAt
}

func (w *writer) stop() {
	w.queue.close()
	w.wg.Wait()
	close(w.done)
}

func (w *writer) flushAfterStop() error {
	buf := w.enc.bytes()
	if len(buf) > 0 {
		n, err := w.writeAll(buf)
		if err != nil {
			return err
		}
		w.stats.addBytes(uint64(n))
		w.writeOffset += int64(n)

		active := w.mgr.active()
		active.writeOff.Store(w.writeOffset)
		active.lastLSN = w.lastLSN

		w.enc.reset()
	}
	return w.lastErr
}

func (w *writer) writeErr() error {
	return w.lastErr
}

// notifyIndexer calls Indexer.OnAppend for each event in the encoded buffer.
func (w *writer) notifyIndexer(buf []byte, baseOffset int64) {
	off := 0
	var decodeBuf []Event
	for off < len(buf) {
		frameStart := off
		decodeBuf = decodeBuf[:0]
		events, next, err := decodeBatchFrameInto(buf, off, w.cfg.compressor, decodeBuf)
		if err != nil {
			break
		}
		decodeBuf = events
		frameOff := baseOffset + int64(frameStart)
		for i := range events {
			info := IndexInfo{
				LSN:       events[i].LSN,
				Timestamp: events[i].Timestamp,
				Key:       events[i].Key,
				Meta:      events[i].Meta,
				Offset:    frameOff,
				Segment:   w.segmentLSN,
			}
			safeCall(func() { w.cfg.indexer.OnAppend(info) })
		}
		off = next
	}
}
