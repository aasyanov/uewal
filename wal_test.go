package uewal

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestOpenClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "oc.wal")
	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	s := w.Stats()
	if s.State != StateRunning {
		t.Fatalf("state=%v, want RUNNING", s.State)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s = w.Stats()
	if s.State != StateClosed {
		t.Fatalf("state after close=%v, want CLOSED", s.State)
	}
}

func TestShutdownGraceful(t *testing.T) {
	w := openTestWAL(t, WithSyncMode(SyncBatch))

	for i := 0; i < 100; i++ {
		w.Append(Event{Payload: []byte("shutdown-test")})
	}

	err := w.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	s := w.Stats()
	if s.State != StateClosed {
		t.Fatalf("state=%v, want CLOSED", s.State)
	}
}

func TestShutdownIdempotent(t *testing.T) {
	w := openTestWAL(t)
	w.Shutdown(context.Background())
	err := w.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
}

func TestCloseIdempotent(t *testing.T) {
	w := openTestWAL(t)
	w.Close()
	err := w.Close()
	if err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestShutdownWithTimeout(t *testing.T) {
	w := openTestWAL(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := w.Shutdown(ctx)
	if err != nil {
		t.Fatalf("Shutdown with timeout: %v", err)
	}
}

func TestShutdownHooks(t *testing.T) {
	var started, shutdownStarted, shutdownDone bool

	w := openTestWAL(t, WithHooks(Hooks{
		OnStart:         func() { started = true },
		OnShutdownStart: func() { shutdownStarted = true },
		OnShutdownDone:  func(time.Duration) { shutdownDone = true },
	}))

	if !started {
		t.Fatal("OnStart not called")
	}

	w.Shutdown(context.Background())

	if !shutdownStarted {
		t.Fatal("OnShutdownStart not called")
	}
	if !shutdownDone {
		t.Fatal("OnShutdownDone not called")
	}
}

func TestStatsAPI(t *testing.T) {
	w := openTestWAL(t, WithSyncMode(SyncBatch))

	w.Append(Event{Payload: []byte("stats1")})
	w.Append(Event{Payload: []byte("stats2")})
	time.Sleep(100 * time.Millisecond)

	s := w.Stats()
	if s.State != StateRunning {
		t.Fatalf("state=%v, want RUNNING", s.State)
	}
	if s.EventsWritten < 2 {
		t.Fatalf("EventsWritten=%d, want >= 2", s.EventsWritten)
	}
	if s.BytesWritten == 0 {
		t.Fatal("BytesWritten should be > 0")
	}

	w.Shutdown(context.Background())
}

func TestLastLSN(t *testing.T) {
	w := openTestWAL(t)

	if w.LastLSN() != 0 {
		t.Fatalf("initial LastLSN=%d, want 0", w.LastLSN())
	}

	w.Append(Event{Payload: []byte("a")})
	w.Append(Event{Payload: []byte("b")})
	time.Sleep(100 * time.Millisecond)

	if w.LastLSN() < 2 {
		t.Fatalf("LastLSN=%d, want >= 2", w.LastLSN())
	}

	w.Shutdown(context.Background())
}

func TestOpenWithCustomStorage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "custom.wal")
	s, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}

	w, err := Open("", WithStorage(s))
	if err != nil {
		t.Fatalf("Open with custom storage: %v", err)
	}

	lsn, err := w.Append(Event{Payload: []byte("custom")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if lsn != 1 {
		t.Fatalf("LSN=%d, want 1", lsn)
	}

	w.Shutdown(context.Background())
}

func TestFlushWaitsForWriterDrain(t *testing.T) {
	path := filepath.Join(t.TempDir(), "flush_drain.wal")
	w, err := Open(path, WithSyncMode(SyncNever))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Shutdown(context.Background())

	const total = 50
	for i := 0; i < total; i++ {
		w.Append(Event{Payload: []byte("flush-drain")})
	}

	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// After Flush returns, all events MUST have been written to storage.
	s := w.Stats()
	if s.EventsWritten < total {
		t.Fatalf("EventsWritten=%d after Flush, want >= %d", s.EventsWritten, total)
	}
	if s.FileSize == 0 {
		t.Fatal("FileSize should be > 0 after Flush")
	}
}

func TestFlushDoesNotFsync(t *testing.T) {
	var synced bool
	w := openTestWAL(t, WithSyncMode(SyncNever), WithHooks(Hooks{
		BeforeSync: func() { synced = true },
	}))
	defer w.Shutdown(context.Background())

	w.Append(Event{Payload: []byte("no-fsync")})
	w.Flush()

	if synced {
		t.Fatal("Flush should NOT trigger fsync (SyncNever mode)")
	}
}

func TestSyncCallsFsync(t *testing.T) {
	w := openTestWAL(t, WithSyncMode(SyncNever))
	defer w.Shutdown(context.Background())

	w.Append(Event{Payload: []byte("fsync-test")})

	// Sync calls storage.Sync() directly, ensuring durability.
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
}

func TestFlushThenSync(t *testing.T) {
	w := openTestWAL(t, WithSyncMode(SyncNever))
	defer w.Shutdown(context.Background())

	w.Append(Event{Payload: []byte("a")})
	w.Append(Event{Payload: []byte("b")})

	// Flush: wait for writer to process everything.
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// Sync: make it durable.
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	s := w.Stats()
	if s.EventsWritten < 2 {
		t.Fatalf("EventsWritten=%d, want >= 2", s.EventsWritten)
	}
}

func TestFlushConcurrent(t *testing.T) {
	w := openTestWAL(t, WithQueueSize(256))
	defer w.Shutdown(context.Background())

	for i := 0; i < 20; i++ {
		w.Append(Event{Payload: []byte("concurrent-flush")})
	}

	const goroutines = 10
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			errs <- w.Flush()
		}()
	}

	for i := 0; i < goroutines; i++ {
		if err := <-errs; err != nil {
			t.Errorf("concurrent Flush: %v", err)
		}
	}
}

func TestOperationsOnClosedWAL(t *testing.T) {
	w := openTestWAL(t)
	w.Shutdown(context.Background())

	if _, err := w.Append(Event{Payload: []byte("x")}); err == nil {
		t.Fatal("Append on closed WAL should fail")
	}
	if err := w.Flush(); err == nil {
		t.Fatal("Flush on closed WAL should fail")
	}
	if err := w.Sync(); err == nil {
		t.Fatal("Sync on closed WAL should fail")
	}

	// Stats should still work
	s := w.Stats()
	if s.State != StateClosed {
		t.Fatalf("state=%v, want CLOSED", s.State)
	}
}

func TestFileLockingViaOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lock.wal")
	w1, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w1.Shutdown(context.Background())

	_, err = Open(path)
	if err == nil {
		t.Fatal("second Open should fail with file lock error")
	}
}

func TestReopenAfterShutdown(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reopen.wal")

	w1, err := Open(path, WithSyncMode(SyncBatch))
	if err != nil {
		t.Fatal(err)
	}
	w1.Append(Event{Payload: []byte("round1")})
	w1.Shutdown(context.Background())

	w2, err := Open(path, WithSyncMode(SyncBatch))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}

	if w2.LastLSN() != 1 {
		t.Fatalf("recovered LastLSN=%d, want 1", w2.LastLSN())
	}

	lsn, _ := w2.Append(Event{Payload: []byte("round2")})
	if lsn != 2 {
		t.Fatalf("new LSN=%d, want 2", lsn)
	}

	w2.Shutdown(context.Background())
}

func TestStatsAfterCloseNoPanic(t *testing.T) {
	w := openTestWAL(t)
	w.Append(Event{Payload: []byte("x")})
	w.Shutdown(context.Background())

	// Must not panic even though storage is closed.
	s := w.Stats()
	if s.State != StateClosed {
		t.Fatalf("state=%v, want CLOSED", s.State)
	}
	if s.FileSize != 0 {
		t.Fatalf("FileSize=%d after close, want 0", s.FileSize)
	}
}

func TestShutdownConcurrentCallers(t *testing.T) {
	w := openTestWAL(t)

	for i := 0; i < 10; i++ {
		w.Append(Event{Payload: []byte("concurrent-shutdown")})
	}

	const goroutines = 10
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			errs <- w.Shutdown(context.Background())
		}()
	}

	for i := 0; i < goroutines; i++ {
		if err := <-errs; err != nil {
			t.Errorf("Shutdown goroutine returned error: %v", err)
		}
	}

	if w.sm.load() != StateClosed {
		t.Fatalf("state=%v, want CLOSED", w.sm.load())
	}
}

func TestShutdownThenClose(t *testing.T) {
	w := openTestWAL(t)
	w.Append(Event{Payload: []byte("x")})

	if err := w.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	// Close after Shutdown must be idempotent and safe.
	if err := w.Close(); err != nil {
		t.Fatalf("Close after Shutdown: %v", err)
	}
}

func TestCloseThenShutdown(t *testing.T) {
	w := openTestWAL(t)
	w.Append(Event{Payload: []byte("x")})

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Shutdown after Close must be idempotent and safe.
	if err := w.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown after Close: %v", err)
	}
}

func TestShutdownContextCancel(t *testing.T) {
	w := openTestWAL(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := w.Shutdown(ctx)
	// Either nil (completed before cancel) or context.Canceled.
	if err != nil && err != context.Canceled {
		t.Fatalf("Shutdown with canceled ctx: got %v", err)
	}

	// WAL should still be cleanable even if ctx canceled.
	// Subsequent Shutdown with real context must succeed.
	err = w.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("Shutdown after cancel: %v", err)
	}
}

func TestShutdownDrainsAllEvents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "drain_all.wal")
	w, err := Open(path, WithSyncMode(SyncBatch))
	if err != nil {
		t.Fatal(err)
	}

	const total = 200
	for i := 0; i < total; i++ {
		w.Append(Event{Payload: []byte("drain-check")})
	}

	w.Shutdown(context.Background())

	w2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Shutdown(context.Background())

	var count int
	w2.Replay(0, func(Event) error {
		count++
		return nil
	})
	if count != total {
		t.Fatalf("replayed %d events, want %d", count, total)
	}
}

func TestShutdownIdempotentConcurrentWithAppend(t *testing.T) {
	w := openTestWAL(t, WithQueueSize(128))

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			_, err := w.Append(Event{Payload: []byte("racing")})
			if err != nil {
				return
			}
		}
	}()

	time.Sleep(time.Millisecond)
	w.Shutdown(context.Background())
	<-done

	if w.sm.load() != StateClosed {
		t.Fatalf("state=%v, want CLOSED", w.sm.load())
	}
}

func TestWithIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.wal")

	var indexed []struct {
		lsn    LSN
		offset int64
	}
	idx := &testIndexerFunc{fn: func(lsn LSN, _ []byte, off int64) {
		indexed = append(indexed, struct {
			lsn    LSN
			offset int64
		}{lsn, off})
	}}

	w, err := Open(path, WithSyncMode(SyncBatch), WithIndex(idx))
	if err != nil {
		t.Fatal(err)
	}

	w.Append(Event{Payload: []byte("first")})
	w.Append(Event{Payload: []byte("second")})
	w.Flush()

	w.Shutdown(context.Background())

	if len(indexed) != 2 {
		t.Fatalf("indexed %d, want 2", len(indexed))
	}
	if indexed[0].lsn != 1 || indexed[1].lsn != 2 {
		t.Fatalf("LSNs: %d, %d", indexed[0].lsn, indexed[1].lsn)
	}
}

type testIndexerFunc struct {
	fn func(LSN, []byte, int64)
}

func (idx *testIndexerFunc) OnAppend(lsn LSN, meta []byte, offset int64) {
	idx.fn(lsn, meta, offset)
}

func TestCompressionStats(t *testing.T) {
	comp := &shrinkingCompressor{}
	path := filepath.Join(t.TempDir(), "comp_stats.wal")

	w, err := Open(path, WithSyncMode(SyncBatch), WithCompressor(comp))
	if err != nil {
		t.Fatal(err)
	}

	w.Append(Event{Payload: []byte("compressible data payload here")})
	w.Flush()

	s := w.Stats()
	if s.CompressedBytes == 0 {
		t.Fatal("CompressedBytes should be > 0 with shrinking compressor")
	}

	w.Shutdown(context.Background())
}

// shrinkingCompressor always halves the input, exercising the
// addCompressed stats branch that requires encoded < uncompressed.
type shrinkingCompressor struct{}

func (c *shrinkingCompressor) Compress(src []byte) ([]byte, error) {
	n := len(src) / 2
	if n == 0 {
		n = 1
	}
	return src[:n], nil
}

func (c *shrinkingCompressor) Decompress(src []byte) ([]byte, error) {
	return src, nil
}

func TestReplayOnInitState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "init.wal")
	s, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	w := &WAL{storage: s}

	err = w.Replay(0, func(Event) error { return nil })
	if err != ErrNotRunning {
		t.Fatalf("Replay on INIT: got %v, want ErrNotRunning", err)
	}
}

func TestIteratorOnInitState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "init_iter.wal")
	s, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	w := &WAL{storage: s}

	_, err = w.Iterator(0)
	if err != ErrNotRunning {
		t.Fatalf("Iterator on INIT: got %v, want ErrNotRunning", err)
	}
}

func TestReplayOnClosedWALReturnsError(t *testing.T) {
	w := openTestWAL(t, WithSyncMode(SyncBatch))
	w.Append(Event{Payload: []byte("data")})
	w.Shutdown(context.Background())

	err := w.Replay(0, func(Event) error { return nil })
	if err != ErrClosed {
		t.Fatalf("Replay on CLOSED: got %v, want ErrClosed", err)
	}
}

func TestIteratorOnClosedWALReturnsError(t *testing.T) {
	w := openTestWAL(t, WithSyncMode(SyncBatch))
	w.Append(Event{Payload: []byte("data")})
	w.Shutdown(context.Background())

	_, err := w.Iterator(0)
	if err != ErrClosed {
		t.Fatalf("Iterator on CLOSED: got %v, want ErrClosed", err)
	}
}
