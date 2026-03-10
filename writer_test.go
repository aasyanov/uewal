package uewal

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"
)

func TestWriterBasicWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "writer.wal")
	s, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}

	q := newWriteQueue(16)
	var stats statsCollector
	hooks := &hooksRunner{}
	cfg := defaultConfig()

	w := newWriter(s, q, cfg, &stats, hooks, 0)
	w.start()

	events := []Event{{LSN: 1, Payload: []byte("hello")}}
	q.enqueue(writeBatch{events: events, lsnStart: 1, lsnEnd: 1})

	time.Sleep(50 * time.Millisecond)

	q.close()
	w.wg.Wait()

	size, _ := s.Size()
	if size == 0 {
		t.Fatal("expected data written to storage")
	}

	buf := make([]byte, size)
	s.ReadAt(buf, 0)
	decoded, _, decErr := decodeBatchFrame(buf, 0, nil)
	if decErr != nil {
		t.Fatalf("decode error: %v", decErr)
	}
	if len(decoded) != 1 {
		t.Fatalf("got %d events, want 1", len(decoded))
	}
	if decoded[0].LSN != 1 || !bytes.Equal(decoded[0].Payload, []byte("hello")) {
		t.Fatalf("unexpected event: %+v", decoded[0])
	}

	s.Close()
}

func TestWriterGroupCommit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "group.wal")
	s, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}

	q := newWriteQueue(16)
	var stats statsCollector
	hooks := &hooksRunner{}
	cfg := defaultConfig()

	for i := 0; i < 5; i++ {
		q.enqueue(writeBatch{
			events:   []Event{{LSN: LSN(i + 1), Payload: []byte("event")}},
			lsnStart: LSN(i + 1),
			lsnEnd:   LSN(i + 1),
		})
	}

	w := newWriter(s, q, cfg, &stats, hooks, 0)
	w.start()

	time.Sleep(100 * time.Millisecond)
	q.close()
	w.wg.Wait()

	if stats.eventsWritten.Load() != 5 {
		t.Fatalf("events=%d, want 5", stats.eventsWritten.Load())
	}
	if stats.lastLSN.Load() != 5 {
		t.Fatalf("lastLSN=%d, want 5", stats.lastLSN.Load())
	}

	s.Close()
}

func TestWriterSyncBatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.wal")
	s, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}

	q := newWriteQueue(16)
	var stats statsCollector
	hooks := &hooksRunner{}
	cfg := defaultConfig()
	cfg.syncMode = SyncBatch

	w := newWriter(s, q, cfg, &stats, hooks, 0)
	w.start()

	q.enqueue(writeBatch{
		events:   []Event{{LSN: 1, Payload: []byte("durable")}},
		lsnStart: 1,
		lsnEnd:   1,
	})

	time.Sleep(50 * time.Millisecond)
	q.close()
	w.wg.Wait()

	if stats.syncCount.Load() == 0 {
		t.Fatal("expected at least one sync in SyncBatch mode")
	}

	s.Close()
}

func TestWriterDrain(t *testing.T) {
	path := filepath.Join(t.TempDir(), "drain.wal")
	s, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}

	q := newWriteQueue(64)
	var stats statsCollector
	hooks := &hooksRunner{}
	cfg := defaultConfig()

	w := newWriter(s, q, cfg, &stats, hooks, 0)
	w.start()

	for i := 0; i < 50; i++ {
		q.enqueue(writeBatch{
			events:   []Event{{LSN: LSN(i + 1), Payload: []byte("drain")}},
			lsnStart: LSN(i + 1),
			lsnEnd:   LSN(i + 1),
		})
	}

	w.stop()

	if stats.eventsWritten.Load() != 50 {
		t.Fatalf("events=%d, want 50", stats.eventsWritten.Load())
	}

	s.Close()
}

type shortWriteStorage struct {
	Storage
	maxPerWrite int
}

func (s *shortWriteStorage) Write(p []byte) (int, error) {
	n := len(p)
	if n > s.maxPerWrite {
		n = s.maxPerWrite
	}
	return s.Storage.Write(p[:n])
}

func TestWriterShortWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "short_write.wal")
	fs, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}

	sw := &shortWriteStorage{Storage: fs, maxPerWrite: 5}

	q := newWriteQueue(16)
	var stats statsCollector
	hooks := &hooksRunner{}
	cfg := defaultConfig()

	w := newWriter(sw, q, cfg, &stats, hooks, 0)
	w.start()

	payload := []byte("this is a longer payload that exceeds 5 bytes")
	q.enqueue(writeBatch{
		events:   []Event{{LSN: 1, Payload: payload}},
		lsnStart: 1,
		lsnEnd:   1,
	})

	time.Sleep(100 * time.Millisecond)
	q.close()
	w.wg.Wait()

	size, _ := fs.Size()
	if size == 0 {
		t.Fatal("expected data written despite short writes")
	}

	buf := make([]byte, size)
	fs.ReadAt(buf, 0)
	decoded, _, decErr := decodeBatchFrame(buf, 0, nil)
	if decErr != nil {
		t.Fatalf("decode error: %v", decErr)
	}
	if len(decoded) != 1 || !bytes.Equal(decoded[0].Payload, payload) {
		t.Fatalf("unexpected: %+v", decoded)
	}

	fs.Close()
}

type failingStorage struct {
	Storage
	failAfter int
	writes    int
}

func (s *failingStorage) Write(p []byte) (int, error) {
	s.writes++
	if s.writes > s.failAfter {
		return 0, ErrShortWrite
	}
	return s.Storage.Write(p)
}

func TestWriterWriteErrPropagation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fail_write.wal")
	fs, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}

	fw := &failingStorage{Storage: fs, failAfter: 1}

	q := newWriteQueue(16)
	var stats statsCollector
	hooks := &hooksRunner{}
	cfg := defaultConfig()

	w := newWriter(fw, q, cfg, &stats, hooks, 0)
	w.start()

	q.enqueue(writeBatch{
		events:   []Event{{LSN: 1, Payload: []byte("ok")}},
		lsnStart: 1,
		lsnEnd:   1,
	})
	time.Sleep(50 * time.Millisecond)

	q.enqueue(writeBatch{
		events:   []Event{{LSN: 2, Payload: []byte("fail")}},
		lsnStart: 2,
		lsnEnd:   2,
	})
	time.Sleep(50 * time.Millisecond)

	q.close()
	w.wg.Wait()

	flushErr := w.flushAfterStop()
	writeErr := w.writeErr()

	if flushErr == nil && writeErr == nil {
		t.Fatal("expected write error to be captured")
	}

	fs.Close()
}

func TestWriterBarrierBatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "barrier.wal")
	s, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}

	q := newWriteQueue(16)
	var stats statsCollector
	hooks := &hooksRunner{}
	cfg := defaultConfig()

	w := newWriter(s, q, cfg, &stats, hooks, 0)
	w.start()

	q.enqueue(writeBatch{
		events:   []Event{{LSN: 1, Payload: []byte("real")}},
		lsnStart: 1,
		lsnEnd:   1,
	})

	barrier := make(chan struct{})
	q.enqueue(writeBatch{barrier: barrier})

	<-barrier

	if stats.eventsWritten.Load() != 1 {
		t.Fatalf("events=%d, want 1", stats.eventsWritten.Load())
	}
	if stats.batchesWritten.Load() != 1 {
		t.Fatalf("batches=%d, want 1", stats.batchesWritten.Load())
	}

	q.close()
	w.wg.Wait()
	s.Close()
}

func TestWriterBarrierAfterFlush(t *testing.T) {
	path := filepath.Join(t.TempDir(), "barrier_flush.wal")
	s, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}

	q := newWriteQueue(16)
	var stats statsCollector
	hooks := &hooksRunner{}
	cfg := defaultConfig()

	w := newWriter(s, q, cfg, &stats, hooks, 0)
	w.start()

	for i := 1; i <= 5; i++ {
		q.enqueue(writeBatch{
			events:   []Event{{LSN: LSN(i), Payload: []byte("data")}},
			lsnStart: LSN(i),
			lsnEnd:   LSN(i),
		})
	}
	barrier := make(chan struct{})
	q.enqueue(writeBatch{barrier: barrier})
	<-barrier

	if stats.eventsWritten.Load() != 5 {
		t.Fatalf("events=%d after barrier, want 5", stats.eventsWritten.Load())
	}

	size, _ := s.Size()
	if size == 0 {
		t.Fatal("no data written to storage after barrier")
	}

	q.close()
	w.wg.Wait()
	s.Close()
}

func TestWriterSyncInterval(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync_interval.wal")
	s, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}

	q := newWriteQueue(16)
	var stats statsCollector
	hooks := &hooksRunner{}
	cfg := defaultConfig()
	cfg.syncMode = SyncInterval
	cfg.syncInterval = 10 * time.Millisecond

	w := newWriter(s, q, cfg, &stats, hooks, 0)
	w.start()

	for i := 0; i < 20; i++ {
		q.enqueue(writeBatch{
			events:   []Event{{LSN: LSN(i + 1), Payload: []byte("interval")}},
			lsnStart: LSN(i + 1),
			lsnEnd:   LSN(i + 1),
		})
		time.Sleep(5 * time.Millisecond)
	}

	time.Sleep(50 * time.Millisecond)
	q.close()
	w.wg.Wait()

	if stats.syncCount.Load() == 0 {
		t.Fatal("expected at least one sync in SyncInterval mode")
	}

	s.Close()
}

func TestWriterWithIndexer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "indexer.wal")
	s, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}

	q := newWriteQueue(16)
	var stats statsCollector
	hooks := &hooksRunner{}

	var indexed []indexEntry
	idx := &testIndexer{entries: &indexed}

	cfg := defaultConfig()
	cfg.indexer = idx

	w := newWriter(s, q, cfg, &stats, hooks, 0)
	w.start()

	q.enqueue(writeBatch{
		events: []Event{
			{LSN: 1, Payload: []byte("a"), Meta: []byte("meta1")},
			{LSN: 2, Payload: []byte("b")},
		},
		lsnStart: 1,
		lsnEnd:   2,
	})

	time.Sleep(50 * time.Millisecond)
	q.close()
	w.wg.Wait()

	if len(indexed) != 2 {
		t.Fatalf("indexed %d entries, want 2", len(indexed))
	}
	if indexed[0].lsn != 1 || string(indexed[0].meta) != "meta1" {
		t.Fatalf("indexed[0]=%+v", indexed[0])
	}
	if indexed[1].lsn != 2 || indexed[1].meta != nil {
		t.Fatalf("indexed[1]=%+v", indexed[1])
	}

	s.Close()
}

type indexEntry struct {
	lsn    LSN
	meta   []byte
	offset int64
}

type testIndexer struct {
	entries *[]indexEntry
}

func (idx *testIndexer) OnAppend(lsn LSN, meta []byte, offset int64) {
	*idx.entries = append(*idx.entries, indexEntry{lsn: lsn, meta: meta, offset: offset})
}

func TestWriterIndexerMultipleBatches(t *testing.T) {
	path := filepath.Join(t.TempDir(), "indexer_multi.wal")
	s, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}

	q := newWriteQueue(16)
	var stats statsCollector
	hooks := &hooksRunner{}

	var indexed []indexEntry
	idx := &testIndexer{entries: &indexed}

	cfg := defaultConfig()
	cfg.indexer = idx

	w := newWriter(s, q, cfg, &stats, hooks, 0)
	w.start()

	for batch := 0; batch < 5; batch++ {
		lsn := LSN(batch*3 + 1)
		q.enqueue(writeBatch{
			events: []Event{
				{LSN: lsn, Payload: []byte("x")},
				{LSN: lsn + 1, Payload: []byte("y"), Meta: []byte("m")},
				{LSN: lsn + 2, Payload: []byte("z")},
			},
			lsnStart: lsn,
			lsnEnd:   lsn + 2,
		})
	}

	time.Sleep(100 * time.Millisecond)
	q.close()
	w.wg.Wait()

	if len(indexed) != 15 {
		t.Fatalf("indexed %d entries, want 15", len(indexed))
	}
	for i, entry := range indexed {
		if entry.lsn != LSN(i+1) {
			t.Fatalf("indexed[%d].lsn=%d, want %d", i, entry.lsn, i+1)
		}
	}

	s.Close()
}

func TestWriterFlushAfterStopWithResidual(t *testing.T) {
	path := filepath.Join(t.TempDir(), "flush_residual.wal")
	s, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}

	q := newWriteQueue(16)
	var stats statsCollector
	hooks := &hooksRunner{}
	cfg := defaultConfig()

	w := newWriter(s, q, cfg, &stats, hooks, 0)
	w.start()
	w.stop()

	w.enc.encodeBatch([]Event{{LSN: 1, Payload: []byte("residual")}}, 1, nil)

	bytesBefore := stats.bytesWritten.Load()
	if ferr := w.flushAfterStop(); ferr != nil {
		t.Fatalf("flushAfterStop: %v", ferr)
	}

	if stats.bytesWritten.Load() <= bytesBefore {
		t.Fatal("flushAfterStop should have written residual bytes")
	}

	size, _ := s.Size()
	if size == 0 {
		t.Fatal("expected data after flushAfterStop")
	}
	s.Close()
}

func TestWriterFlushAfterStopPropagatesWriteErr(t *testing.T) {
	path := filepath.Join(t.TempDir(), "flush_err.wal")
	fs, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}

	fw := &failingStorage{Storage: fs, failAfter: 100}

	q := newWriteQueue(16)
	var stats statsCollector
	hooks := &hooksRunner{}
	cfg := defaultConfig()

	w := newWriter(fw, q, cfg, &stats, hooks, 0)
	w.start()
	w.stop()

	fw.failAfter = 0
	w.enc.encodeBatch([]Event{{LSN: 1, Payload: []byte("fail")}}, 1, nil)

	if ferr := w.flushAfterStop(); ferr == nil {
		t.Fatal("expected flushAfterStop to return write error")
	}
	fs.Close()
}

func TestWriterFlushAfterStopReturnsLastErr(t *testing.T) {
	path := filepath.Join(t.TempDir(), "flush_last.wal")
	fs, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}

	fw := &failingStorage{Storage: fs, failAfter: 0}

	q := newWriteQueue(16)
	var stats statsCollector
	hooks := &hooksRunner{}
	cfg := defaultConfig()

	w := newWriter(fw, q, cfg, &stats, hooks, 0)
	w.start()

	q.enqueue(writeBatch{
		events:   []Event{{LSN: 1, Payload: []byte("fail")}},
		lsnStart: 1,
		lsnEnd:   1,
	})
	time.Sleep(50 * time.Millisecond)

	w.stop()

	if ferr := w.flushAfterStop(); ferr == nil {
		t.Fatal("expected flushAfterStop to propagate lastErr")
	}
	fs.Close()
}

func TestWriterProcessBatchCompression(t *testing.T) {
	path := filepath.Join(t.TempDir(), "comp_batch.wal")
	s, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}

	q := newWriteQueue(16)
	var stats statsCollector
	hooks := &hooksRunner{}
	cfg := defaultConfig()
	cfg.compressor = &shrinkingCompressor{}

	w := newWriter(s, q, cfg, &stats, hooks, 0)
	w.start()

	q.enqueue(writeBatch{
		events:   []Event{{LSN: 1, Payload: []byte("compress me please")}},
		lsnStart: 1,
		lsnEnd:   1,
	})
	time.Sleep(50 * time.Millisecond)

	q.close()
	w.wg.Wait()

	snap := stats.snapshot(0, 0, StateRunning)
	if snap.CompressedBytes == 0 {
		t.Fatal("expected CompressedBytes > 0")
	}

	s.Close()
}
