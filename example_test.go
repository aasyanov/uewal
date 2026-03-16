package uewal_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aasyanov/uewal"
)

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
