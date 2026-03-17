package uewal

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWaitDurable_Basic(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	lsn, _ := writeOne(w, []byte("durable-data"), nil, nil)
	if err := w.WaitDurable(lsn); err != nil {
		t.Fatal(err)
	}

	w.Shutdown(context.Background())
}

func TestWaitDurable_AlreadySynced(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, WithSyncMode(SyncBatch))
	if err != nil {
		t.Fatal(err)
	}

	lsn, _ := writeOne(w, []byte("data"), nil, nil)
	w.Flush()
	time.Sleep(20 * time.Millisecond)

	start := time.Now()
	w.WaitDurable(lsn)
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("WaitDurable should return instantly for already-synced LSN")
	}

	w.Shutdown(context.Background())
}

func TestWaitDurable_Coalesced(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var completed atomic.Int32

	for i := 0; i < 5; i++ {
		lsn, _ := writeOne(w, []byte("coalesced"), nil, nil)
		wg.Add(1)
		go func(l LSN) {
			defer wg.Done()
			w.WaitDurable(l)
			completed.Add(1)
		}(lsn)
	}

	wg.Wait()
	if completed.Load() != 5 {
		t.Fatalf("expected 5 completed, got %d", completed.Load())
	}

	w.Shutdown(context.Background())
}

func TestWaitDurable_SyncBatch(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, WithSyncMode(SyncBatch))
	if err != nil {
		t.Fatal(err)
	}

	lsn, _ := writeOne(w, []byte("sync-batch"), nil, nil)
	if err := w.WaitDurable(lsn); err != nil {
		t.Fatal(err)
	}

	w.Shutdown(context.Background())
}

func TestDurableNotifier_WaitAndAdvance(t *testing.T) {
	dn := &durableNotifier{}

	dn.advance(10)
	dn.wait(5) // should return immediately

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		dn.wait(20)
	}()

	time.Sleep(10 * time.Millisecond)
	dn.mu.Lock()
	n := len(dn.waiters)
	dn.mu.Unlock()
	if n == 0 {
		t.Fatal("should have pending waiters")
	}

	dn.advance(25)
	wg.Wait()

	dn.mu.Lock()
	n = len(dn.waiters)
	dn.mu.Unlock()
	if n != 0 {
		t.Fatal("should have no pending waiters")
	}
}

func TestDurableNotifier_WakeAll(t *testing.T) {
	dn := &durableNotifier{}

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(lsn LSN) {
			defer wg.Done()
			dn.wait(lsn)
		}(LSN(100 + i))
	}

	time.Sleep(10 * time.Millisecond)
	dn.wakeAll()
	wg.Wait()
}

func TestDurableNotifier_AdvanceNoop(t *testing.T) {
	dn := &durableNotifier{}
	dn.advance(10)

	// Advancing to same or lower LSN should be a noop.
	dn.advance(5)
	if dn.syncedTo.Load() != 10 {
		t.Fatalf("advance(5) should not decrease syncedTo; got %d", dn.syncedTo.Load())
	}
	dn.advance(10)
	if dn.syncedTo.Load() != 10 {
		t.Fatalf("advance(10) should be idempotent; got %d", dn.syncedTo.Load())
	}
}

func TestWaitDurable_ShutdownUnblocks(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := writeOne(w, []byte("data"), nil, nil); err != nil {
		t.Fatal(err)
	}
	w.Flush()

	done := make(chan struct{})
	go func() {
		w.WaitDurable(9999)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	w.Shutdown(context.Background())

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("WaitDurable did not unblock after Shutdown")
	}
}

func TestWaitDurable_SyncNever(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, WithSyncMode(SyncNever))
	if err != nil {
		t.Fatal(err)
	}

	lsn, _ := writeOne(w, []byte("sync-never"), nil, nil)
	if err := w.WaitDurable(lsn); err != nil {
		t.Fatal(err)
	}

	if w.durable.syncedTo.Load() < lsn {
		t.Fatalf("syncedTo %d should be >= %d", w.durable.syncedTo.Load(), lsn)
	}

	w.Shutdown(context.Background())
}

func TestWaitDurable_SyncInterval(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, WithSyncMode(SyncInterval), WithSyncInterval(time.Second))
	if err != nil {
		t.Fatal(err)
	}

	lsn, _ := writeOne(w, []byte("sync-interval"), nil, nil)
	start := time.Now()
	if err := w.WaitDurable(lsn); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)

	// WaitDurable triggers immediate durableSync, not waiting for interval tick.
	if elapsed > 500*time.Millisecond {
		t.Fatalf("WaitDurable took %v, should be near-instant (not waiting for interval)", elapsed)
	}

	w.Shutdown(context.Background())
}
