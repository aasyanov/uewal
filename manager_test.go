package uewal

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestManager_RecoverFromManifest(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		_, err = writeOne(w, []byte("data"), nil, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = w.Flush(); err != nil {
		t.Fatal(err)
	}
	lastLSN := w.LastLSN()
	w.Shutdown(context.Background())

	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Shutdown(context.Background())

	if w2.LastLSN() != lastLSN {
		t.Fatalf("recovered LastLSN = %d, want %d", w2.LastLSN(), lastLSN)
	}
	var count int
	w2.Replay(1, func(ev Event) error {
		count++
		return nil
	})
	if count != 5 {
		t.Fatalf("recovered %d events, want 5", count)
	}
}

func TestManager_RecoverByScan(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		_, err = writeOne(w, []byte("data"), nil, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = w.Flush(); err != nil {
		t.Fatal(err)
	}
	lastLSN := w.LastLSN()
	w.Shutdown(context.Background())

	manifestPath := filepath.Join(dir, manifestFile)
	if err = os.Remove(manifestPath); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Shutdown(context.Background())

	if w2.LastLSN() != lastLSN {
		t.Fatalf("recovered LastLSN = %d, want %d (fallback scan)", w2.LastLSN(), lastLSN)
	}
	var count int
	w2.Replay(1, func(ev Event) error {
		count++
		return nil
	})
	if count != 5 {
		t.Fatalf("recovered %d events, want 5", count)
	}
}

func TestManager_InsertSealed_Ordering(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Shutdown(context.Background())

	// Create segments in primary, export, import in reverse order
	primaryDir := t.TempDir()
	primary, err := Open(primaryDir, WithMaxSegmentSize(200))
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 80)
	for i := 0; i < 30; i++ {
		if _, err := writeOne(primary, payload, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	primary.Flush()
	primary.Shutdown(context.Background())

	segs := primary.Segments()
	var sealedPaths []string
	for _, s := range segs {
		if s.Sealed {
			sealedPaths = append(sealedPaths, s.Path)
		}
	}
	if len(sealedPaths) < 2 {
		t.Skipf("need >= 2 sealed segments, got %d", len(sealedPaths))
	}

	// Import in reverse order
	for i := len(sealedPaths) - 1; i >= 0; i-- {
		tmpFile := filepath.Join(t.TempDir(), "seg.wal")
		data, err := os.ReadFile(sealedPaths[i])
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(tmpFile, data, 0644); err != nil {
			t.Fatal(err)
		}
		if err := w.ImportSegment(tmpFile); err != nil {
			t.Fatal(err)
		}
	}

	replicaSegs := w.Segments()
	for i := 1; i < len(replicaSegs); i++ {
		if replicaSegs[i].FirstLSN < replicaSegs[i-1].FirstLSN && replicaSegs[i].Sealed {
			t.Fatalf("Segments() out of order: [%d].FirstLSN=%d < [%d].FirstLSN=%d",
				i, replicaSegs[i].FirstLSN, i-1, replicaSegs[i-1].FirstLSN)
		}
	}
}

func TestManager_DeleteBefore_RefCount(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, WithMaxSegmentSize(512))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Shutdown(context.Background())

	for i := 0; i < 10; i++ {
		_, err = writeOne(w, []byte("x"), nil, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = w.Rotate(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		_, err = writeOne(w, []byte("y"), nil, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = w.Flush(); err != nil {
		t.Fatal(err)
	}

	// Iterator holds ref on segments
	it, err := w.Iterator(1)
	if err != nil {
		t.Fatal(err)
	}
	_ = it.Next() // ensure we've acquired segments

	// DeleteBefore(100) would delete segment 1 (LSN 1-10) but it's in use
	w.DeleteBefore(100)
	segs := w.Segments()
	if len(segs) != 2 {
		t.Fatalf("segment should not be deleted while iterator holds ref: got %d segments", len(segs))
	}

	it.Close()

	// Now DeleteBefore can remove the old segment
	w.DeleteBefore(100)
	segs = w.Segments()
	if len(segs) != 1 {
		t.Fatalf("after iterator closed, expect 1 segment, got %d", len(segs))
	}
}
