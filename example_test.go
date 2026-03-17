package uewal_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aasyanov/uewal"
)

// ─────────────────────────────────────────────────────────────
// 1. Lifecycle
// ─────────────────────────────────────────────────────────────

//nolint:gocritic // log.Fatal is acceptable in examples
func ExampleOpen() {
	dir, _ := os.MkdirTemp("", "uewal-example-*")
	defer os.RemoveAll(dir)

	w, err := uewal.Open(dir,
		uewal.WithSyncMode(uewal.SyncNever),
		uewal.WithMaxSegmentSize(64<<20),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer w.Shutdown(context.Background())

	fmt.Println("WAL opened")
	// Output: WAL opened
}

// ─────────────────────────────────────────────────────────────
// 2. Write — single event
// ─────────────────────────────────────────────────────────────

//nolint:gocritic // log.Fatal is acceptable in examples
func ExampleWAL_Write() {
	dir, _ := os.MkdirTemp("", "uewal-example-*")
	defer os.RemoveAll(dir)

	w, _ := uewal.Open(dir)
	defer w.Shutdown(context.Background())

	batch := uewal.NewBatch(1)
	batch.Append([]byte("user_created"), []byte("user-123"), []byte("aggregate:user"))
	lsn, err := w.Write(batch)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("LSN=%d\n", lsn)
	// Output: LSN=1
}

// ─────────────────────────────────────────────────────────────
// 3. Write — batch of events
// ─────────────────────────────────────────────────────────────

//nolint:gocritic // log.Fatal is acceptable in examples
func ExampleWAL_Write_batch() {
	dir, _ := os.MkdirTemp("", "uewal-example-*")
	defer os.RemoveAll(dir)

	w, _ := uewal.Open(dir)
	defer w.Shutdown(context.Background())

	batch := uewal.NewBatch(3)
	batch.Append([]byte("event-1"), nil, nil)
	batch.Append([]byte("event-2"), nil, nil)
	batch.Append([]byte("event-3"), nil, nil)
	lsn, err := w.Write(batch)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("last LSN=%d, batch size=%d\n", lsn, batch.Len())
	// Output: last LSN=3, batch size=3
}

// ─────────────────────────────────────────────────────────────
// 4. WriteUnsafe — zero-copy with explicit timestamp
// ─────────────────────────────────────────────────────────────

//nolint:gocritic // log.Fatal is acceptable in examples
func ExampleWAL_WriteUnsafe() {
	dir, _ := os.MkdirTemp("", "uewal-example-*")
	defer os.RemoveAll(dir)

	w, _ := uewal.Open(dir)
	defer w.Shutdown(context.Background())

	ts := time.Date(2026, 3, 17, 12, 0, 0, 0, time.UTC).UnixNano()
	batch := uewal.NewBatch(2)
	batch.AppendUnsafeWithTimestamp([]byte("sensor-reading-1"), []byte("sensor-42"), nil, ts)
	batch.AppendUnsafeWithTimestamp([]byte("sensor-reading-2"), []byte("sensor-42"), nil, ts+1)
	lsn, err := w.WriteUnsafe(batch)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("last LSN=%d\n", lsn)
	// Output: last LSN=2
}

// ─────────────────────────────────────────────────────────────
// 5. Batch reuse — zero-alloc hot path
// ─────────────────────────────────────────────────────────────

//nolint:gocritic // log.Fatal is acceptable in examples
func ExampleBatch_Reset() {
	dir, _ := os.MkdirTemp("", "uewal-example-*")
	defer os.RemoveAll(dir)

	w, _ := uewal.Open(dir)
	defer w.Shutdown(context.Background())

	batch := uewal.NewBatch(10)
	for i := 0; i < 3; i++ {
		batch.Reset()
		batch.Append([]byte(fmt.Sprintf("tick-%d", i)), nil, nil)
		w.Write(batch)
	}
	w.Flush()

	fmt.Printf("last LSN=%d\n", w.LastLSN())
	// Output: last LSN=3
}

// ─────────────────────────────────────────────────────────────
// 6. MarkNoCompress — skip compression for a batch
// ─────────────────────────────────────────────────────────────

//nolint:gocritic // log.Fatal is acceptable in examples
func ExampleBatch_MarkNoCompress() {
	dir, _ := os.MkdirTemp("", "uewal-example-*")
	defer os.RemoveAll(dir)

	w, _ := uewal.Open(dir)
	defer w.Shutdown(context.Background())

	batch := uewal.NewBatch(2)
	batch.MarkNoCompress()
	batch.Append([]byte("already-compressed-data"), nil, nil)
	batch.Append([]byte("binary-blob"), nil, nil)
	lsn, err := w.Write(batch)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("last LSN=%d\n", lsn)
	// Output: last LSN=2
}

// ─────────────────────────────────────────────────────────────
// 7. Replay — callback-based read
// ─────────────────────────────────────────────────────────────

func ExampleWAL_Replay() {
	dir, _ := os.MkdirTemp("", "uewal-example-*")
	defer os.RemoveAll(dir)

	w, _ := uewal.Open(dir)

	batch := uewal.NewBatch(3)
	batch.Append([]byte("alpha"), nil, nil)
	batch.Append([]byte("beta"), nil, nil)
	batch.Append([]byte("gamma"), nil, nil)
	w.Write(batch)
	w.Flush()

	var count int
	w.Replay(0, func(ev uewal.Event) error {
		count++
		return nil
	})

	fmt.Printf("replayed %d events\n", count)
	w.Shutdown(context.Background())
	// Output: replayed 3 events
}

// ─────────────────────────────────────────────────────────────
// 8. ReplayRange — bounded LSN range
// ─────────────────────────────────────────────────────────────

func ExampleWAL_ReplayRange() {
	dir, _ := os.MkdirTemp("", "uewal-example-*")
	defer os.RemoveAll(dir)

	w, _ := uewal.Open(dir)
	defer w.Shutdown(context.Background())

	b := uewal.NewBatch(1)
	for i := 0; i < 10; i++ {
		b.Reset()
		b.Append([]byte(fmt.Sprintf("ev-%d", i)), nil, nil)
		w.Write(b)
	}
	w.Flush()

	var payloads []string
	w.ReplayRange(3, 5, func(ev uewal.Event) error {
		payloads = append(payloads, string(ev.Payload))
		return nil
	})

	fmt.Printf("range [3..5]: %v\n", payloads)
	// Output: range [3..5]: [ev-2 ev-3 ev-4]
}

// ─────────────────────────────────────────────────────────────
// 9. Iterator — pull-based read
// ─────────────────────────────────────────────────────────────

//nolint:gocritic // log.Fatal is acceptable in examples
func ExampleWAL_Iterator() {
	dir, _ := os.MkdirTemp("", "uewal-example-*")
	defer os.RemoveAll(dir)

	w, _ := uewal.Open(dir)
	defer w.Shutdown(context.Background())

	b := uewal.NewBatch(1)
	for i := 0; i < 5; i++ {
		b.Reset()
		b.Append([]byte(fmt.Sprintf("item-%d", i)), nil, nil)
		w.Write(b)
	}
	w.Flush()

	it, err := w.Iterator(3)
	if err != nil {
		log.Fatal(err)
	}
	var count int
	for it.Next() {
		count++
	}
	it.Close()

	fmt.Printf("iterated %d events from LSN 3\n", count)
	// Output: iterated 3 events from LSN 3
}

// ─────────────────────────────────────────────────────────────
// 10. Follow — live tail-follow iterator
// ─────────────────────────────────────────────────────────────

//nolint:gocritic // log.Fatal is acceptable in examples
func ExampleWAL_Follow() {
	dir, _ := os.MkdirTemp("", "uewal-example-*")
	defer os.RemoveAll(dir)

	w, _ := uewal.Open(dir)
	defer w.Shutdown(context.Background())

	b := uewal.NewBatch(1)
	b.Append([]byte("event-1"), nil, nil)
	w.Write(b)
	b.Reset()
	b.Append([]byte("event-2"), nil, nil)
	w.Write(b)
	w.Flush()

	it, err := w.Follow(0)
	if err != nil {
		log.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		var count int
		for it.Next() {
			count++
			if count == 3 {
				break
			}
		}
		fmt.Printf("followed %d events\n", count)
		close(done)
	}()

	time.Sleep(10 * time.Millisecond)
	b.Reset()
	b.Append([]byte("event-3"), nil, nil)
	w.Write(b)
	w.Flush()

	<-done
	it.Close()
	// Output: followed 3 events
}

// ─────────────────────────────────────────────────────────────
// 11. WaitDurable — wait for fsync
// ─────────────────────────────────────────────────────────────

//nolint:gocritic // log.Fatal is acceptable in examples
func ExampleWAL_WaitDurable() {
	dir, _ := os.MkdirTemp("", "uewal-example-*")
	defer os.RemoveAll(dir)

	w, _ := uewal.Open(dir, uewal.WithSyncMode(uewal.SyncNever))
	defer w.Shutdown(context.Background())

	batch := uewal.NewBatch(1)
	batch.Append([]byte("critical-event"), nil, nil)
	lsn, _ := w.Write(batch)

	if err := w.WaitDurable(lsn); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("LSN %d is durable\n", lsn)
	// Output: LSN 1 is durable
}

// ─────────────────────────────────────────────────────────────
// 12. Rotation & Segments
// ─────────────────────────────────────────────────────────────

func ExampleWAL_Rotate() {
	dir, _ := os.MkdirTemp("", "uewal-example-*")
	defer os.RemoveAll(dir)

	w, _ := uewal.Open(dir)
	defer w.Shutdown(context.Background())

	b := uewal.NewBatch(1)
	b.Append([]byte("before-rotate"), nil, nil)
	w.Write(b)
	w.Flush()

	w.Rotate()

	b.Reset()
	b.Append([]byte("after-rotate"), nil, nil)
	w.Write(b)
	w.Flush()

	segments := w.Segments()
	fmt.Printf("%d segments\n", len(segments))
	// Output: 2 segments
}

// ─────────────────────────────────────────────────────────────
// 13. Stats
// ─────────────────────────────────────────────────────────────

func ExampleWAL_Stats() {
	dir, _ := os.MkdirTemp("", "uewal-example-*")
	defer os.RemoveAll(dir)

	w, _ := uewal.Open(dir)
	defer w.Shutdown(context.Background())

	b := uewal.NewBatch(1)
	for i := 0; i < 5; i++ {
		b.Reset()
		b.Append([]byte("data"), nil, nil)
		w.Write(b)
	}
	w.Flush()

	s := w.Stats()
	fmt.Printf("events=%d batches=%d\n", s.EventsWritten, s.BatchesWritten)
	// Output: events=5 batches=5
}

// ─────────────────────────────────────────────────────────────
// 14. Snapshot — consistent read during writes
// ─────────────────────────────────────────────────────────────

//nolint:gocritic // log.Fatal is acceptable in examples
func ExampleWAL_Snapshot() {
	dir, _ := os.MkdirTemp("", "uewal-example-*")
	defer os.RemoveAll(dir)

	w, _ := uewal.Open(dir, uewal.WithMaxSegmentSize(500))

	b := uewal.NewBatch(1)
	for i := 0; i < 20; i++ {
		b.Reset()
		b.Append([]byte(fmt.Sprintf("event-%02d", i)), nil, nil)
		w.Write(b)
	}
	w.Flush()

	err := w.Snapshot(func(ctrl *uewal.SnapshotController) error {
		var lastLSN uewal.LSN
		it, err := ctrl.Iterator()
		if err != nil {
			return err
		}
		for it.Next() {
			lastLSN = it.Event().LSN
		}
		it.Close()

		ctrl.Checkpoint(lastLSN)
		fmt.Printf("checkpoint at LSN=%d\n", lastLSN)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	w.Shutdown(context.Background())
	// Output: checkpoint at LSN=20
}

// ─────────────────────────────────────────────────────────────
// 15. Hooks — observability callbacks
// ─────────────────────────────────────────────────────────────

func ExampleWithHooks() {
	dir, _ := os.MkdirTemp("", "uewal-example-*")
	defer os.RemoveAll(dir)

	var rotations int
	w, _ := uewal.Open(dir,
		uewal.WithMaxSegmentSize(256),
		uewal.WithHooks(uewal.Hooks{
			OnRotation: func(_ uewal.SegmentInfo) { rotations++ },
		}),
	)

	b := uewal.NewBatch(1)
	for i := 0; i < 50; i++ {
		b.Reset()
		b.Append([]byte(fmt.Sprintf("event-%03d", i)), nil, nil)
		w.Write(b)
	}
	w.Shutdown(context.Background())

	fmt.Printf("rotations > 0: %v\n", rotations > 0)
	// Output: rotations > 0: true
}
