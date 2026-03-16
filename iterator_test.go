package uewal

import (
	"context"
	"os"
	"path/filepath"
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
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Shutdown(context.Background())

	// Create a segment via ImportSegment
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "seg.wal")
	primary, err := Open(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	_, err = writeOne(primary, []byte("x"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = primary.Flush(); err != nil {
		t.Fatal(err)
	}
	primary.Shutdown(context.Background())

	data, err := os.ReadFile(filepath.Join(tmpDir, "00000000000000000001.wal"))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(tmpFile, data, 0644); err != nil {
		t.Fatal(err)
	}
	if err = w.ImportSegment(tmpFile); err != nil {
		t.Fatal(err)
	}

	// Remove segment file so mmap will fail on open (works on Unix)
	segs := w.Segments()
	if len(segs) == 0 {
		t.Fatal("no segments after import")
	}
	if err = os.Remove(segs[0].Path); err != nil {
		t.Skipf("cannot remove segment to trigger mmap error: %v", err)
	}

	it, err := w.Iterator(1)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	// Next() will try to mmap the missing file and fail
	it.Next()
	if it.Err() == nil {
		t.Fatal("expected error from mmap to propagate via Err()")
	}
}
