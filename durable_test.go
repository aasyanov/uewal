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

	lsn, _ := w.Append([]byte("durable-data"))
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

	lsn, _ := w.Append([]byte("data"))
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
		lsn, _ := w.Append([]byte("coalesced"))
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

	lsn, _ := w.Append([]byte("sync-batch"))
	if err := w.WaitDurable(lsn); err != nil {
		t.Fatal(err)
	}

	w.Shutdown(context.Background())
}

func TestDurableNotifier_Unit(t *testing.T) {
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
	if !dn.pending() {
		t.Fatal("should have pending waiters")
	}

	dn.advance(25)
	wg.Wait()

	if dn.pending() {
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
