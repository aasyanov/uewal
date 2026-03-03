package uewal

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func TestStressConcurrentAppend(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	path := filepath.Join(t.TempDir(), "stress_append.wal")
	w, err := Open(path, WithSyncMode(SyncNever), WithQueueSize(4096))
	if err != nil {
		t.Fatal(err)
	}

	const goroutines = 16
	const perGoroutine = 5000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				payload := fmt.Appendf(nil, "g%d-e%d", id, i)
				_, err := w.Append(Event{Payload: payload})
				if err != nil {
					t.Errorf("Append error: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	expected := uint64(goroutines * perGoroutine)
	if w.lsn.current() != expected {
		t.Fatalf("lsn=%d, want %d", w.lsn.current(), expected)
	}

	w.Shutdown(context.Background())
}

func TestStressConcurrentAppendAndReplay(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	path := filepath.Join(t.TempDir(), "stress_rw.wal")

	// Phase 1: Write
	w1, err := Open(path, WithSyncMode(SyncBatch), WithQueueSize(4096))
	if err != nil {
		t.Fatal(err)
	}

	const total = 10000
	var wg sync.WaitGroup
	wg.Add(4)
	for g := 0; g < 4; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < total/4; i++ {
				payload := fmt.Appendf(nil, "event-%d-%d", id, i)
				w1.Append(Event{Payload: payload})
			}
		}(g)
	}
	wg.Wait()
	w1.Shutdown(context.Background())

	// Phase 2: Replay
	w2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	var count int
	err = w2.Replay(0, func(e Event) error {
		count++
		if e.LSN == 0 {
			return fmt.Errorf("event with LSN=0")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Replay error: %v", err)
	}
	if count != total {
		t.Fatalf("replayed %d events, want %d", count, total)
	}

	w2.Shutdown(context.Background())
}

func TestStressLargePayloads(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	path := filepath.Join(t.TempDir(), "stress_large.wal")
	w, err := Open(path,
		WithSyncMode(SyncNever),
		WithQueueSize(256),
		WithBufferSize(1024*1024),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Write 100 events of 64KB each (6.4MB total)
	bigPayload := make([]byte, 64*1024)
	for i := range bigPayload {
		bigPayload[i] = byte(i % 256)
	}

	const numEvents = 100
	for i := 0; i < numEvents; i++ {
		if _, appendErr := w.Append(Event{Payload: bigPayload}); appendErr != nil {
			t.Fatalf("Append %d: %v", i, appendErr)
		}
	}

	w.Shutdown(context.Background())

	// Verify replay
	w2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	w2.Replay(0, func(e Event) error {
		count++
		if len(e.Payload) != len(bigPayload) {
			t.Fatalf("payload size=%d, want %d", len(e.Payload), len(bigPayload))
		}
		return nil
	})

	if count != numEvents {
		t.Fatalf("replayed %d, want %d", count, numEvents)
	}

	w2.Shutdown(context.Background())
}

func TestStressRepeatedOpenClose(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	path := filepath.Join(t.TempDir(), "stress_oc.wal")

	for cycle := 0; cycle < 20; cycle++ {
		w, err := Open(path, WithSyncMode(SyncBatch))
		if err != nil {
			t.Fatalf("cycle %d: Open: %v", cycle, err)
		}

		expectedLSN := LSN(cycle*10 + 10)
		for i := 0; i < 10; i++ {
			w.Append(Event{Payload: []byte(fmt.Sprintf("cycle%d-ev%d", cycle, i))})
		}

		w.Shutdown(context.Background())

		// Verify
		w2, err := Open(path)
		if err != nil {
			t.Fatalf("cycle %d: reopen: %v", cycle, err)
		}
		if w2.LastLSN() != expectedLSN {
			t.Fatalf("cycle %d: LastLSN=%d, want %d", cycle, w2.LastLSN(), expectedLSN)
		}
		w2.Shutdown(context.Background())
	}
}

func TestStressBackpressureDrop(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	path := filepath.Join(t.TempDir(), "stress_drop.wal")
	w, err := Open(path,
		WithSyncMode(SyncBatch),
		WithQueueSize(8),
		WithBackpressure(DropMode),
	)
	if err != nil {
		t.Fatal(err)
	}

	const goroutines = 8
	const perGoroutine = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				w.Append(Event{Payload: []byte("drop-stress")})
			}
		}()
	}
	wg.Wait()

	stats := w.Stats()
	t.Logf("events written=%d, drops=%d", stats.EventsWritten, stats.Drops)

	w.Shutdown(context.Background())
}

func TestStressBackpressureError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	path := filepath.Join(t.TempDir(), "stress_error.wal")
	w, err := Open(path,
		WithSyncMode(SyncNever),
		WithQueueSize(8),
		WithBackpressure(ErrorMode),
	)
	if err != nil {
		t.Fatal(err)
	}

	var errCount, okCount int
	for i := 0; i < 10000; i++ {
		_, appendErr := w.Append(Event{Payload: []byte("error-stress")})
		switch appendErr {
		case ErrQueueFull:
			errCount++
		case nil:
			okCount++
		default:
			t.Fatalf("unexpected error: %v", appendErr)
		}
	}

	t.Logf("ok=%d, ErrQueueFull=%d", okCount, errCount)
	if okCount == 0 {
		t.Fatal("expected at least some successful appends")
	}

	w.Shutdown(context.Background())
}
