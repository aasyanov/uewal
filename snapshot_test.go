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
		if _, err = writeOne(w, []byte(fmt.Sprintf("snap-%d", i)), nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	w.Flush()

	err = w.Snapshot(func(ctrl *SnapshotController) error {
		count := 0
		it, e := ctrl.Iterator()
		if e != nil {
			return e
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
		if _, err = writeOne(w, []byte("data"), nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	w.Flush()

	err = w.Snapshot(func(ctrl *SnapshotController) error {
		it, e := ctrl.IteratorFrom(5)
		if e != nil {
			return e
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
		if _, err = writeOne(w, payload, nil, nil); err != nil {
			t.Fatal(err)
		}
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

	if _, err = writeOne(w, []byte("data"), nil, nil); err != nil {
		t.Fatal(err)
	}
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
		if _, err = writeOne(w, payload, nil, nil); err != nil {
			t.Fatal(err)
		}
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

func TestSnapshot_ConcurrentAppend(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, WithMaxSegmentSize(256))
	if err != nil {
		t.Fatal(err)
	}

	payload := make([]byte, 80)
	for i := 0; i < 10; i++ {
		if _, err = writeOne(w, payload, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	w.Flush()
	time.Sleep(30 * time.Millisecond)

	err = w.Snapshot(func(ctrl *SnapshotController) error {
		snapshotSegs := ctrl.Segments()

		for i := 0; i < 5; i++ {
			if _, err = writeOne(w, payload, nil, nil); err != nil {
				return err
			}
		}
		w.Flush()

		var it *Iterator
		it, err = ctrl.Iterator()
		if err != nil {
			return err
		}
		defer it.Close()
		count := 0
		for it.Next() {
			count++
		}

		if len(snapshotSegs) == 0 {
			t.Fatal("snapshot should see segments")
		}
		if count == 0 {
			t.Fatal("snapshot iterator should see events")
		}
		ctrl.Checkpoint(5)
		return ctrl.Compact()
	})
	if err != nil {
		t.Fatal(err)
	}

	w.Shutdown(context.Background())
}

func TestSnapshot_CompactPreservesActiveData(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, WithMaxSegmentSize(256))
	if err != nil {
		t.Fatal(err)
	}

	payload := make([]byte, 80)
	for i := 0; i < 20; i++ {
		if _, err = writeOne(w, payload, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	w.Flush()
	time.Sleep(30 * time.Millisecond)

	lastLSN := w.LastLSN()

	err = w.Snapshot(func(ctrl *SnapshotController) error {
		ctrl.Checkpoint(lastLSN)
		return ctrl.Compact()
	})
	if err != nil {
		t.Fatal(err)
	}

	// After compaction, new appends must work.
	var lsn LSN
	lsn, err = writeOne(w, []byte("after-compact"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if lsn <= lastLSN {
		t.Fatalf("LSN %d should be > %d after compact", lsn, lastLSN)
	}

	w.Shutdown(context.Background())
}
