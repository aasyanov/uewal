package uewal

import (
	"context"
	"errors"
	"os"
	"runtime"
	"testing"
)

func TestIterator_CloseIdempotent(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Shutdown(context.Background())

	_, err = writeOne(w, []byte("event"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Flush(); err != nil {
		t.Fatal(err)
	}

	it, err := w.Iterator(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("second Close should be idempotent: %v", err)
	}
}

func TestIterator_EmptyWAL(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Shutdown(context.Background())

	it, err := w.Iterator(1)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	if it.Next() {
		t.Fatal("Next() on empty WAL should return false immediately")
	}
}

func TestIterator_FromLSN_SparseAcceleration(t *testing.T) {
	dir := t.TempDir()
	// Small MaxSegmentSize to force rotation after ~100 events
	w, err := Open(dir, WithMaxSegmentSize(4096))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Shutdown(context.Background())

	payload := make([]byte, 32)
	for i := 0; i < 100; i++ {
		_, err = writeOne(w, payload, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = w.Rotate(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		_, err = writeOne(w, payload, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = w.Flush(); err != nil {
		t.Fatal(err)
	}

	// Iterator from LSN 150 should use sparse index and start at 150
	it, err := w.Iterator(150)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	if !it.Next() {
		t.Fatal("expected at least one event")
	}
	if ev := it.Event(); ev.LSN != 150 {
		t.Fatalf("first event LSN = %d, want 150 (sparse acceleration)", ev.LSN)
	}
}

func TestIterator_MmapError(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("on Windows, segment files may be locked; mmap error test skipped")
	}
	dir := t.TempDir()

	// Write data, rotate to create a sealed segment, then shut down.
	w, err := Open(dir, WithMaxSegmentSize(1))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		writeOne(w, []byte("data"), nil, nil)
	}
	_ = w.Flush()
	w.Shutdown(context.Background())

	// Find and delete a sealed segment file.
	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	segs := w2.Segments()
	var removed bool
	for _, seg := range segs {
		if seg.Sealed {
			if rmErr := os.Remove(seg.Path); rmErr == nil {
				removed = true
				break
			}
		}
	}
	if !removed {
		t.Skip("could not remove a sealed segment to trigger mmap error")
	}

	// Iterator or Replay should surface the mmap error.
	it, itErr := w2.Iterator(0)
	if itErr != nil {
		// Error surfaced at creation — acceptable.
		if !errors.Is(itErr, ErrMmap) {
			t.Fatalf("expected ErrMmap, got %v", itErr)
		}
		return
	}
	defer it.Close()

	for it.Next() {
	}
	if it.Err() == nil {
		// mmap error must be surfaced somewhere.
		t.Fatal("expected mmap error to propagate")
	}
}
