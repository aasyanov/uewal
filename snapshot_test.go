package uewal

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestSnapshot_Basic(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		w.Append([]byte(fmt.Sprintf("snap-%d", i)))
	}
	w.Flush()

	err = w.Snapshot(func(ctrl *SnapshotController) error {
		count := 0
		it, err := ctrl.Iterator()
		if err != nil {
			return err
		}
		defer it.Close()
		for it.Next() {
			count++
		}
		if count != 10 {
			t.Fatalf("snapshot iterator: %d events, want 10", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	w.Shutdown(context.Background())
}

func TestSnapshot_IteratorFrom(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		w.Append([]byte("data"))
	}
	w.Flush()

	err = w.Snapshot(func(ctrl *SnapshotController) error {
		it, err := ctrl.IteratorFrom(5)
		if err != nil {
			return err
		}
		defer it.Close()
		count := 0
		for it.Next() {
			count++
		}
		if count != 6 {
			t.Fatalf("from LSN 5: %d events, want 6", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	w.Shutdown(context.Background())
}

func TestSnapshot_Compact(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, WithMaxSegmentSize(256))
	if err != nil {
		t.Fatal(err)
	}

	payload := make([]byte, 100)
	for i := 0; i < 30; i++ {
		w.Append(payload)
	}
	w.Flush()
	time.Sleep(50 * time.Millisecond)

	segsBefore := w.Segments()
	if len(segsBefore) < 2 {
		t.Fatalf("expected multiple segments, got %d", len(segsBefore))
	}

	err = w.Snapshot(func(ctrl *SnapshotController) error {
		ctrl.Checkpoint(20)
		return ctrl.Compact()
	})
	if err != nil {
		t.Fatal(err)
	}

	segsAfter := w.Segments()
	if len(segsAfter) >= len(segsBefore) {
		t.Fatalf("compact should have removed segments: before=%d, after=%d",
			len(segsBefore), len(segsAfter))
	}

	w.Shutdown(context.Background())
}

func TestSnapshot_Segments(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	w.Append([]byte("data"))
	w.Flush()

	err = w.Snapshot(func(ctrl *SnapshotController) error {
		segs := ctrl.Segments()
		if len(segs) != 1 {
			t.Fatalf("expected 1 segment, got %d", len(segs))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	w.Shutdown(context.Background())
}

func TestSnapshot_NoCheckpoint(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, WithMaxSegmentSize(256))
	if err != nil {
		t.Fatal(err)
	}

	payload := make([]byte, 100)
	for i := 0; i < 20; i++ {
		w.Append(payload)
	}
	w.Flush()
	time.Sleep(50 * time.Millisecond)

	segsBefore := len(w.Segments())

	w.Snapshot(func(ctrl *SnapshotController) error {
		return ctrl.Compact()
	})

	segsAfter := len(w.Segments())
	if segsAfter != segsBefore {
		t.Fatalf("compact without checkpoint should be noop: before=%d, after=%d",
			segsBefore, segsAfter)
	}

	w.Shutdown(context.Background())
}
