package uewal

import (
	"context"
	"errors"
	"path/filepath"
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
	// Build a sealed segment that points to a non-existent file.
	// warmCache is deliberately NOT called, so mmapAcquire will hit disk
	// and fail, exercising the ErrMmap wrapping in advanceSegment.
	seg := &segment{
		path:     filepath.Join(t.TempDir(), "gone.wal"),
		firstLSN: 1,
	}
	seg.sealedAt.Store(true)
	seg.storeSize(4096)
	seg.storeLastLSN(10)

	it := &Iterator{
		segments:  []*segment{seg},
		segIdx:    -1,
		fromLSN:   0,
		decodeBuf: make([]Event, 0, 8),
	}

	if it.Next() {
		t.Fatal("Next() should return false for missing segment file")
	}
	if it.Err() == nil {
		t.Fatal("expected mmap error to propagate via Err()")
	}
	if !errors.Is(it.Err(), ErrMmap) {
		t.Fatalf("expected ErrMmap, got %v", it.Err())
	}
}
