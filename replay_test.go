package uewal

import (
	"context"
	"errors"
	"testing"
)

func TestReplaySegments_StopEarly(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Shutdown(context.Background())

	for i := 0; i < 10; i++ {
		_, err := w.Append([]byte("event"))
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	var count int
	err = w.Replay(1, func(ev Event) error {
		count++
		if count >= 5 {
			return errStopReplay
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Replay should return nil on errStopReplay, got %v", err)
	}
	if count != 5 {
		t.Fatalf("callback should process %d events, got %d", 5, count)
	}
}

func TestReplaySegments_CallbackError(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Shutdown(context.Background())

	_, err = w.Append([]byte("event"))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	customErr := errors.New("custom error")
	err = w.Replay(1, func(ev Event) error {
		return customErr
	})
	if err != customErr {
		t.Fatalf("Replay should propagate callback error, got %v", err)
	}
}

func TestReplayBatchesSegments_BatchCallback(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Shutdown(context.Background())

	batch1 := NewBatch(3)
	batch1.Append([]byte("a"))
	batch1.Append([]byte("b"))
	batch1.Append([]byte("c"))
	_, err = w.AppendBatch(batch1)
	if err != nil {
		t.Fatal(err)
	}

	batch2 := NewBatch(3)
	batch2.Append([]byte("d"))
	batch2.Append([]byte("e"))
	batch2.Append([]byte("f"))
	_, err = w.AppendBatch(batch2)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	var batchCount int
	var batchSizes []int
	err = w.ReplayBatches(1, func(events []Event) error {
		batchCount++
		batchSizes = append(batchSizes, len(events))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if batchCount != 2 {
		t.Fatalf("callback should be called %d times, got %d", 2, batchCount)
	}
	if len(batchSizes) != 2 || batchSizes[0] != 3 || batchSizes[1] != 3 {
		t.Fatalf("expected 3 events per batch, got %v", batchSizes)
	}
}
