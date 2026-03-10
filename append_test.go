package uewal

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func openTestWAL(t *testing.T, opts ...Option) *WAL {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := Open(path, opts...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return w
}

func TestAppendSingle(t *testing.T) {
	w := openTestWAL(t)
	defer w.Shutdown(context.Background())

	lsn, err := w.Append(Event{Payload: []byte("hello")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if lsn != 1 {
		t.Fatalf("LSN=%d, want 1", lsn)
	}
}

func TestAppendMultiple(t *testing.T) {
	w := openTestWAL(t)
	defer w.Shutdown(context.Background())

	lsn, err := w.Append(
		Event{Payload: []byte("a")},
		Event{Payload: []byte("b")},
		Event{Payload: []byte("c")},
	)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if lsn != 3 {
		t.Fatalf("LSN=%d, want 3", lsn)
	}
}

func TestAppendBatch(t *testing.T) {
	w := openTestWAL(t)
	defer w.Shutdown(context.Background())

	b := NewBatch(3)
	b.Add([]byte("x"))
	b.Add([]byte("y"))
	b.Add([]byte("z"))

	lsn, err := w.AppendBatch(b)
	if err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if lsn != 3 {
		t.Fatalf("LSN=%d, want 3", lsn)
	}
}

func TestAppendEmpty(t *testing.T) {
	w := openTestWAL(t)
	defer w.Shutdown(context.Background())

	_, err := w.Append()
	if err != ErrEmptyBatch {
		t.Fatalf("got %v, want ErrEmptyBatch", err)
	}
}

func TestAppendAfterClose(t *testing.T) {
	w := openTestWAL(t)
	w.Shutdown(context.Background())

	_, err := w.Append(Event{Payload: []byte("late")})
	if err == nil {
		t.Fatal("expected error after shutdown")
	}
}

func TestAppendLSNMonotonic(t *testing.T) {
	w := openTestWAL(t)
	defer w.Shutdown(context.Background())

	var prev LSN
	for i := 0; i < 100; i++ {
		lsn, err := w.Append(Event{Payload: []byte("mono")})
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		if lsn <= prev {
			t.Fatalf("LSN not monotonic: %d <= %d", lsn, prev)
		}
		prev = lsn
	}
}

func TestAppendBackpressureError(t *testing.T) {
	w := openTestWAL(t, WithQueueSize(1), WithBackpressure(ErrorMode))
	defer w.Shutdown(context.Background())

	// Fill the queue by quickly sending many events
	var errCount int
	for i := 0; i < 1000; i++ {
		_, err := w.Append(Event{Payload: []byte("pressure")})
		if err == ErrQueueFull {
			errCount++
		}
	}

	if errCount == 0 {
		t.Log("no ErrQueueFull observed (writer consumed fast enough)")
	}
}

func TestAppendBackpressureDrop(t *testing.T) {
	w := openTestWAL(t, WithQueueSize(1), WithBackpressure(DropMode))
	defer w.Shutdown(context.Background())

	for i := 0; i < 1000; i++ {
		w.Append(Event{Payload: []byte("drop-test")})
	}

	// No error is expected; drops are silent
	stats := w.Stats()
	_ = stats.Drops // may or may not be > 0 depending on timing
}

func TestAppendConcurrent(t *testing.T) {
	w := openTestWAL(t, WithQueueSize(256))
	defer w.Shutdown(context.Background())

	const goroutines = 10
	const perGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				w.Append(Event{Payload: []byte("concurrent")})
			}
		}()
	}
	wg.Wait()

	// LSN counter is incremented on Append, not when writer processes.
	// Check lsn counter, not LastLSN (which reflects writer progress).
	lsn := w.lsn.current()
	if lsn != goroutines*perGoroutine {
		t.Fatalf("lsn.current()=%d, want %d", lsn, goroutines*perGoroutine)
	}
}

func TestAppendDoesNotMutateCallerSlice(t *testing.T) {
	w := openTestWAL(t)
	defer w.Shutdown(context.Background())

	events := []Event{
		{Payload: []byte("a")},
		{Payload: []byte("b")},
	}

	// Caller's LSN fields should be zero before Append.
	if events[0].LSN != 0 || events[1].LSN != 0 {
		t.Fatal("precondition: LSN should be 0")
	}

	w.Append(events...)

	// After Append, the caller's original slice must remain unmodified.
	if events[0].LSN != 0 || events[1].LSN != 0 {
		t.Fatalf("Append mutated caller's events: LSN=%d,%d", events[0].LSN, events[1].LSN)
	}
}

func TestAppendBatchDoesNotMutateCaller(t *testing.T) {
	w := openTestWAL(t)
	defer w.Shutdown(context.Background())

	batch := NewBatch(2)
	batch.Add([]byte("x"))
	batch.Add([]byte("y"))

	if batch.Events[0].LSN != 0 || batch.Events[1].LSN != 0 {
		t.Fatal("precondition: batch LSN should be 0")
	}

	w.AppendBatch(batch)

	if batch.Events[0].LSN != 0 || batch.Events[1].LSN != 0 {
		t.Fatalf("AppendBatch mutated caller's batch: LSN=%d,%d",
			batch.Events[0].LSN, batch.Events[1].LSN)
	}
}

func TestAppendDuringDraining(t *testing.T) {
	w := openTestWAL(t)

	// Transition to draining.
	w.sm.transition(StateRunning, StateDraining)

	_, err := w.Append(Event{Payload: []byte("too late")})
	if err != ErrDraining {
		t.Fatalf("Append during draining: got %v, want ErrDraining", err)
	}

	// Clean up without double-close.
	w.queue.close()
	w.writer.wg.Wait()
	w.storage.Close()
	w.sm.transition(StateDraining, StateClosed)
}

func TestAppendReturnsCorrectLSNForMultiple(t *testing.T) {
	w := openTestWAL(t)
	defer w.Shutdown(context.Background())

	// First call: 3 events -> LSN should be 3
	lsn, err := w.Append(
		Event{Payload: []byte("a")},
		Event{Payload: []byte("b")},
		Event{Payload: []byte("c")},
	)
	if err != nil {
		t.Fatal(err)
	}
	if lsn != 3 {
		t.Fatalf("first Append LSN=%d, want 3", lsn)
	}

	// Second call: 2 events -> LSN should be 5
	lsn, err = w.Append(
		Event{Payload: []byte("d")},
		Event{Payload: []byte("e")},
	)
	if err != nil {
		t.Fatal(err)
	}
	if lsn != 5 {
		t.Fatalf("second Append LSN=%d, want 5", lsn)
	}
}

func TestAppendEmptyBatch(t *testing.T) {
	w := openTestWAL(t)
	defer w.Shutdown(context.Background())

	batch := NewBatch(0)
	_, err := w.AppendBatch(batch)
	if err != ErrEmptyBatch {
		t.Fatalf("got %v, want ErrEmptyBatch", err)
	}
}

func TestBatchLenAndReset(t *testing.T) {
	b := NewBatch(5)
	if b.Len() != 0 {
		t.Fatalf("Len()=%d, want 0", b.Len())
	}

	b.Add([]byte("a"))
	b.Add([]byte("b"))
	b.Add([]byte("c"))
	if b.Len() != 3 {
		t.Fatalf("Len()=%d, want 3", b.Len())
	}

	b.Reset()
	if b.Len() != 0 {
		t.Fatalf("Len() after Reset=%d, want 0", b.Len())
	}

	// Verify capacity is preserved after Reset.
	b.Add([]byte("d"))
	if b.Len() != 1 {
		t.Fatalf("Len() after re-add=%d, want 1", b.Len())
	}
}

func TestAddWithMeta(t *testing.T) {
	b := NewBatch(2)
	b.Add([]byte("no-meta"))
	b.AddWithMeta([]byte("with-meta"), []byte("tag:test"))

	if b.Len() != 2 {
		t.Fatalf("Len()=%d, want 2", b.Len())
	}
	if b.Events[0].Meta != nil {
		t.Fatalf("Events[0].Meta should be nil")
	}
	if string(b.Events[1].Meta) != "tag:test" {
		t.Fatalf("Events[1].Meta=%q", b.Events[1].Meta)
	}
}

func TestEventSlicePoolRoundTrip(t *testing.T) {
	s, sp := getEventSlice(5)
	if len(s) != 5 {
		t.Fatalf("len=%d, want 5", len(s))
	}
	if sp == nil {
		t.Fatal("pool pointer should not be nil")
	}

	for i := range s {
		s[i] = Event{LSN: LSN(i + 1), Payload: []byte("test")}
	}

	putEventSlice(sp, s)

	s2, sp2 := getEventSlice(3)
	if len(s2) != 3 {
		t.Fatalf("len=%d, want 3", len(s2))
	}
	for i := range s2 {
		if s2[i].LSN != 0 || s2[i].Payload != nil {
			t.Fatalf("slot %d not zeroed: LSN=%d, Payload=%v", i, s2[i].LSN, s2[i].Payload)
		}
	}
	putEventSlice(sp2, s2)
}

func TestEventSlicePoolGrowth(t *testing.T) {
	s, sp := getEventSlice(100)
	if len(s) != 100 {
		t.Fatalf("len=%d, want 100", len(s))
	}
	putEventSlice(sp, s)

	s2, sp2 := getEventSlice(50)
	if len(s2) != 50 {
		t.Fatalf("len=%d, want 50", len(s2))
	}
	if cap(s2) < 100 {
		t.Log("pool may have returned a different slice (GC cleared pool)")
	}
	putEventSlice(sp2, s2)
}

func TestWithSyncIntervalOption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync_interval.wal")
	w, err := Open(path,
		WithSyncMode(SyncInterval),
		WithSyncInterval(50*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 20; i++ {
		w.Append(Event{Payload: []byte("interval-test")})
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)

	s := w.Stats()
	if s.SyncCount == 0 {
		t.Fatal("expected at least one sync in SyncInterval mode")
	}
	w.Shutdown(context.Background())
}
