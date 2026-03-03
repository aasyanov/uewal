package uewal_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/aasyanov/uewal"
)

func ExampleOpen() {
	dir, _ := os.MkdirTemp("", "uewal-example-*")
	defer os.RemoveAll(dir)

	w, err := uewal.Open(filepath.Join(dir, "example.wal"))
	if err != nil {
		panic(err)
	}
	defer w.Shutdown(context.Background())

	fmt.Println("WAL opened successfully")
	// Output: WAL opened successfully
}

func ExampleWAL_Append() {
	dir, _ := os.MkdirTemp("", "uewal-append-*")
	defer os.RemoveAll(dir)

	w, _ := uewal.Open(filepath.Join(dir, "append.wal"),
		uewal.WithSyncMode(uewal.SyncBatch),
	)
	defer w.Shutdown(context.Background())

	lsn, err := w.Append(uewal.Event{Payload: []byte("hello world")})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Written at LSN=%d\n", lsn)
	// Output: Written at LSN=1
}

func ExampleWAL_AppendBatch() {
	dir, _ := os.MkdirTemp("", "uewal-batch-*")
	defer os.RemoveAll(dir)

	w, _ := uewal.Open(filepath.Join(dir, "batch.wal"))
	defer w.Shutdown(context.Background())

	batch := uewal.NewBatch(3)
	batch.Add([]byte("event-1"))
	batch.Add([]byte("event-2"))
	batch.Add([]byte("event-3"))

	lsn, err := w.AppendBatch(batch)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Batch written, last LSN=%d\n", lsn)
	// Output: Batch written, last LSN=3
}

func ExampleWAL_Replay() {
	dir, _ := os.MkdirTemp("", "uewal-replay-*")
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "replay.wal")

	// Write some events
	w, _ := uewal.Open(path, uewal.WithSyncMode(uewal.SyncBatch))
	w.Append(uewal.Event{Payload: []byte("alpha")})
	w.Append(uewal.Event{Payload: []byte("beta")})
	w.Append(uewal.Event{Payload: []byte("gamma")})
	w.Shutdown(context.Background())

	// Replay all events
	w2, _ := uewal.Open(path)
	defer w2.Shutdown(context.Background())

	w2.Replay(0, func(e uewal.Event) error {
		cp := make([]byte, len(e.Payload))
		copy(cp, e.Payload)
		fmt.Printf("LSN=%d payload=%s\n", e.LSN, cp)
		return nil
	})
	// Output:
	// LSN=1 payload=alpha
	// LSN=2 payload=beta
	// LSN=3 payload=gamma
}

func ExampleWithHooks() {
	dir, _ := os.MkdirTemp("", "uewal-hooks-*")
	defer os.RemoveAll(dir)

	hooks := uewal.Hooks{
		OnStart: func() {
			fmt.Println("WAL started")
		},
		OnShutdownDone: func(d time.Duration) {
			fmt.Println("WAL shutdown complete")
		},
	}

	w, _ := uewal.Open(filepath.Join(dir, "hooks.wal"),
		uewal.WithHooks(hooks),
	)
	w.Shutdown(context.Background())
	// Output:
	// WAL started
	// WAL shutdown complete
}

func ExampleWithStorage() {
	dir, _ := os.MkdirTemp("", "uewal-storage-*")
	defer os.RemoveAll(dir)

	// Create a custom FileStorage
	storage, err := uewal.NewFileStorage(filepath.Join(dir, "custom.wal"))
	if err != nil {
		panic(err)
	}

	w, err := uewal.Open("", uewal.WithStorage(storage))
	if err != nil {
		panic(err)
	}
	defer w.Shutdown(context.Background())

	fmt.Println("WAL with custom storage opened")
	// Output: WAL with custom storage opened
}
