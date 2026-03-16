package uewal

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestOpenSegment(t *testing.T) {
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

	segs := w.Segments()
	var sealedLSN LSN
	for _, s := range segs {
		if s.Sealed {
			sealedLSN = s.FirstLSN
			break
		}
	}
	if sealedLSN == 0 {
		t.Fatal("no sealed segments found")
	}

	rc, info, err := w.OpenSegment(sealedLSN)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	if info.FirstLSN != sealedLSN {
		t.Fatalf("info.FirstLSN: %d, want %d", info.FirstLSN, sealedLSN)
	}
	if !info.Sealed {
		t.Fatal("should be sealed")
	}

	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatal("segment data should not be empty")
	}

	w.Shutdown(context.Background())
}

func TestOpenSegment_NotFound(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = w.OpenSegment(99999)
	if err != ErrSegmentNotFound {
		t.Fatalf("expected ErrSegmentNotFound, got %v", err)
	}

	w.Shutdown(context.Background())
}

func TestImportBatch(t *testing.T) {
	primaryDir := t.TempDir()
	primary, err := Open(primaryDir)
	if err != nil {
		t.Fatal(err)
	}

	primary.Append([]byte("replicated-1"))
	primary.Append([]byte("replicated-2"))
	primary.Flush()

	var frames [][]byte
	primary.Replay(0, func(ev Event) error {
		return nil
	})

	primary.Flush()
	time.Sleep(20 * time.Millisecond)

	segs := primary.Segments()
	if len(segs) == 0 {
		t.Fatal("no segments")
	}

	segData, err := os.ReadFile(segs[0].Path)
	if err != nil {
		t.Fatal(err)
	}

	off := 0
	for off < len(segData) {
		info, scanErr := scanBatchFrame(segData, off)
		if scanErr != nil {
			break
		}
		frame := make([]byte, info.frameSize)
		copy(frame, segData[off:info.frameEnd])
		frames = append(frames, frame)
		off = info.frameEnd
	}

	primary.Shutdown(context.Background())

	replicaDir := t.TempDir()
	replica, err := Open(replicaDir, WithStartLSN(1))
	if err != nil {
		t.Fatal(err)
	}

	for _, frame := range frames {
		if err := replica.ImportBatch(frame); err != nil {
			t.Fatal(err)
		}
	}

	count := 0
	replica.Replay(0, func(ev Event) error {
		count++
		return nil
	})
	if count != 2 {
		t.Fatalf("replica events: %d, want 2", count)
	}

	replica.Shutdown(context.Background())
}

func TestImportBatch_InvalidMagic(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	bad := make([]byte, 64)
	bad[0] = 'X'
	if err := w.ImportBatch(bad); err != ErrImportInvalid {
		t.Fatalf("expected ErrImportInvalid, got %v", err)
	}

	w.Shutdown(context.Background())
}

func TestImportSegment(t *testing.T) {
	primaryDir := t.TempDir()
	primary, err := Open(primaryDir, WithMaxSegmentSize(256))
	if err != nil {
		t.Fatal(err)
	}

	payload := make([]byte, 100)
	for i := 0; i < 20; i++ {
		primary.Append(payload)
	}
	primary.Flush()
	time.Sleep(50 * time.Millisecond)

	segs := primary.Segments()
	var sealedPath string
	var sealedFirstLSN LSN
	for _, s := range segs {
		if s.Sealed {
			sealedPath = s.Path
			sealedFirstLSN = s.FirstLSN
			break
		}
	}
	if sealedPath == "" {
		t.Fatal("no sealed segment")
	}

	primary.Shutdown(context.Background())

	tmpFile := filepath.Join(t.TempDir(), "import.wal")
	data, _ := os.ReadFile(sealedPath)
	os.WriteFile(tmpFile, data, 0644)

	replicaDir := t.TempDir()
	replica, err := Open(replicaDir, WithStartLSN(sealedFirstLSN))
	if err != nil {
		t.Fatal(err)
	}

	if err := replica.ImportSegment(tmpFile); err != nil {
		t.Fatal(err)
	}

	replicaSegs := replica.Segments()
	found := false
	for _, s := range replicaSegs {
		if s.FirstLSN == sealedFirstLSN && s.Sealed {
			found = true
		}
	}
	if !found {
		t.Fatal("imported segment not found in replica")
	}

	replica.Shutdown(context.Background())
}

func TestImportSegment_InvalidCRC(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	tmpFile := filepath.Join(t.TempDir(), "bad.wal")
	bad := make([]byte, 100)
	bad[0] = 'E'
	bad[1] = 'W'
	bad[2] = 'A'
	bad[3] = 'L'
	os.WriteFile(tmpFile, bad, 0644)

	err = w.ImportSegment(tmpFile)
	if err != ErrImportInvalid {
		t.Fatalf("expected ErrImportInvalid, got %v", err)
	}

	w.Shutdown(context.Background())
}
