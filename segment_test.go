package uewal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAutoRotation(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, WithMaxSegmentSize(512))
	if err != nil {
		t.Fatal(err)
	}

	payload := make([]byte, 100)
	for i := 0; i < 50; i++ {
		if _, err := writeOne(w, payload, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	w.Flush()
	time.Sleep(50 * time.Millisecond)

	segs := w.Segments()
	if len(segs) < 2 {
		t.Fatalf("expected multiple segments after rotation, got %d", len(segs))
	}

	for i := 0; i < len(segs)-1; i++ {
		if !segs[i].Sealed {
			t.Fatalf("segment %d should be sealed", i)
		}
	}
	if segs[len(segs)-1].Sealed {
		t.Fatal("last segment should not be sealed")
	}

	w.Shutdown(context.Background())
}

func TestManualRotation(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	writeOne(w, []byte("seg1-data"), nil, nil)
	w.Flush()

	if err := w.Rotate(); err != nil {
		t.Fatal(err)
	}

	writeOne(w, []byte("seg2-data"), nil, nil)
	w.Flush()

	segs := w.Segments()
	if len(segs) != 2 {
		t.Fatalf("expected 2 segments, got %d", len(segs))
	}

	w.Shutdown(context.Background())
}

func TestCrossSegmentReplay(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, WithMaxSegmentSize(256))
	if err != nil {
		t.Fatal(err)
	}

	n := 30
	for i := 0; i < n; i++ {
		writeOne(w, []byte(fmt.Sprintf("event-%d", i)), nil, nil)
	}
	w.Flush()
	time.Sleep(50 * time.Millisecond)

	segs := w.Segments()
	if len(segs) < 2 {
		t.Fatalf("expected multiple segments, got %d", len(segs))
	}

	count := 0
	err = w.Replay(0, func(ev Event) error {
		expected := fmt.Sprintf("event-%d", count)
		if string(ev.Payload) != expected {
			t.Fatalf("event %d: payload %q, want %q", count, ev.Payload, expected)
		}
		if ev.LSN != uint64(count+1) {
			t.Fatalf("event %d: LSN %d, want %d", count, ev.LSN, count+1)
		}
		count++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != n {
		t.Fatalf("replayed %d events, want %d", count, n)
	}

	w.Shutdown(context.Background())
}

func TestCrossSegmentReplayFrom(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, WithMaxSegmentSize(256))
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 30; i++ {
		writeOne(w, []byte(fmt.Sprintf("ev-%d", i)), nil, nil)
	}
	w.Flush()
	time.Sleep(50 * time.Millisecond)

	count := 0
	var firstLSN LSN
	w.Replay(15, func(ev Event) error {
		if count == 0 {
			firstLSN = ev.LSN
		}
		count++
		return nil
	})
	if firstLSN != 15 {
		t.Fatalf("first LSN: %d, want 15", firstLSN)
	}
	if count != 16 {
		t.Fatalf("events from LSN 15: %d, want 16", count)
	}

	w.Shutdown(context.Background())
}

func TestCrossSegmentIterator(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, WithMaxSegmentSize(256))
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 20; i++ {
		writeOne(w, []byte(fmt.Sprintf("iter-%d", i)), nil, nil)
	}
	w.Flush()
	time.Sleep(50 * time.Millisecond)

	it, err := w.Iterator(0)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	count := 0
	for it.Next() {
		count++
	}
	if it.Err() != nil {
		t.Fatal(it.Err())
	}
	if count != 20 {
		t.Fatalf("iterator count: %d, want 20", count)
	}

	w.Shutdown(context.Background())
}

func TestSegmentRecovery(t *testing.T) {
	dir := t.TempDir()

	func() {
		w, err := Open(dir, WithMaxSegmentSize(256))
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 20; i++ {
			writeOne(w, []byte(fmt.Sprintf("data-%d", i)), nil, nil)
		}
		w.Flush()
		time.Sleep(50 * time.Millisecond)
		w.Shutdown(context.Background())
	}()

	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	w.Replay(0, func(ev Event) error {
		count++
		return nil
	})
	if count != 20 {
		t.Fatalf("recovered %d events, want 20", count)
	}

	lsn, err := writeOne(w, []byte("after-recovery"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if lsn != 21 {
		t.Fatalf("LSN after recovery: %d, want 21", lsn)
	}

	w.Shutdown(context.Background())
}

func TestRecoveryWithoutManifest(t *testing.T) {
	dir := t.TempDir()

	func() {
		w, err := Open(dir, WithMaxSegmentSize(256))
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 15; i++ {
			writeOne(w, []byte(fmt.Sprintf("data-%d", i)), nil, nil)
		}
		w.Flush()
		time.Sleep(50 * time.Millisecond)
		w.Shutdown(context.Background())
	}()

	os.Remove(filepath.Join(dir, "manifest.bin"))

	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	w.Replay(0, func(ev Event) error {
		count++
		return nil
	})
	if count != 15 {
		t.Fatalf("recovered %d events without manifest, want 15", count)
	}

	w.Shutdown(context.Background())
}

func TestRetentionByCount(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, WithMaxSegmentSize(256), WithMaxSegments(3))
	if err != nil {
		t.Fatal(err)
	}

	payload := make([]byte, 100)
	for i := 0; i < 100; i++ {
		writeOne(w, payload, nil, nil)
	}
	w.Flush()
	time.Sleep(100 * time.Millisecond)

	segs := w.Segments()
	if len(segs) > 3 {
		t.Fatalf("expected <= 3 segments after retention, got %d", len(segs))
	}

	w.Shutdown(context.Background())
}

func TestRetentionBySize(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, WithMaxSegmentSize(256), WithRetentionSize(1024))
	if err != nil {
		t.Fatal(err)
	}

	payload := make([]byte, 100)
	for i := 0; i < 80; i++ {
		writeOne(w, payload, nil, nil)
	}
	w.Flush()
	time.Sleep(100 * time.Millisecond)

	s := w.Stats()
	if s.TotalSize > 2048 {
		t.Fatalf("total size %d exceeds retention budget of 1024 significantly", s.TotalSize)
	}

	w.Shutdown(context.Background())
}

func TestSegmentNaming(t *testing.T) {
	tests := []struct {
		lsn  LSN
		name string
	}{
		{1, "00000000000000000001.wal"},
		{100001, "00000000000000100001.wal"},
		{99999999999999999, "00099999999999999999.wal"},
	}
	for _, tt := range tests {
		got := segmentName(tt.lsn)
		if got != tt.name {
			t.Errorf("segmentName(%d) = %q, want %q", tt.lsn, got, tt.name)
		}
	}
}

func TestParseSegmentLSN(t *testing.T) {
	tests := []struct {
		name string
		lsn  LSN
		ok   bool
	}{
		{"00000000000000000001.wal", 1, true},
		{"00000000000000100001.wal", 100001, true},
		{"not-a-segment.wal", 0, false},
		{"00000000000000000001.idx", 0, false},
		{"LOCK", 0, false},
		{"00000000000000000000.wal", 0, false},
	}
	for _, tt := range tests {
		lsn, ok := parseSegmentLSN(tt.name)
		if ok != tt.ok || (ok && lsn != tt.lsn) {
			t.Errorf("parseSegmentLSN(%q) = (%d, %v), want (%d, %v)", tt.name, lsn, ok, tt.lsn, tt.ok)
		}
	}
}

func TestRotationHook(t *testing.T) {
	dir := t.TempDir()

	var rotated []SegmentInfo
	w, err := Open(dir, WithMaxSegmentSize(256), WithHooks(Hooks{
		OnRotation: func(info SegmentInfo) {
			rotated = append(rotated, info)
		},
	}))
	if err != nil {
		t.Fatal(err)
	}

	payload := make([]byte, 100)
	for i := 0; i < 30; i++ {
		writeOne(w, payload, nil, nil)
	}
	w.Flush()
	time.Sleep(50 * time.Millisecond)

	if len(rotated) == 0 {
		t.Fatal("OnRotation hook not called")
	}
	for _, r := range rotated {
		if !r.Sealed {
			t.Fatal("rotated segment should be sealed")
		}
		if r.FirstLSN == 0 {
			t.Fatal("rotated segment FirstLSN should be > 0")
		}
	}

	w.Shutdown(context.Background())
}

func TestDeleteHook(t *testing.T) {
	dir := t.TempDir()

	var deleted []SegmentInfo
	w, err := Open(dir, WithMaxSegmentSize(256), WithMaxSegments(2), WithHooks(Hooks{
		OnDelete: func(info SegmentInfo) {
			deleted = append(deleted, info)
		},
	}))
	if err != nil {
		t.Fatal(err)
	}

	payload := make([]byte, 100)
	for i := 0; i < 100; i++ {
		writeOne(w, payload, nil, nil)
	}
	w.Flush()
	time.Sleep(100 * time.Millisecond)

	if len(deleted) == 0 {
		t.Fatal("OnDelete hook not called")
	}

	w.Shutdown(context.Background())
}

func TestSegmentsInfo(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	segs := w.Segments()
	if len(segs) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(segs))
	}
	if segs[0].Sealed {
		t.Fatal("initial segment should not be sealed")
	}

	w.Shutdown(context.Background())
}

func TestMultipleRecoveries(t *testing.T) {
	dir := t.TempDir()

	for round := 0; round < 3; round++ {
		w, err := Open(dir)
		if err != nil {
			t.Fatalf("round %d open: %v", round, err)
		}
		for i := 0; i < 5; i++ {
				writeOne(w, []byte(fmt.Sprintf("r%d-e%d", round, i)), nil, nil)
		}
		w.Flush()
		w.Shutdown(context.Background())
	}

	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	w.Replay(0, func(ev Event) error {
		count++
		return nil
	})
	if count != 15 {
		t.Fatalf("total events after 3 rounds: %d, want 15", count)
	}
	w.Shutdown(context.Background())
}

func TestSparseIndexIdxFile(t *testing.T) {
	dir := t.TempDir()

	func() {
		w, err := Open(dir, WithMaxSegmentSize(256))
		if err != nil {
			t.Fatal(err)
		}
		payload := make([]byte, 100)
		for i := 0; i < 20; i++ {
			writeOne(w, payload, nil, nil)
		}
		w.Flush()
		time.Sleep(50 * time.Millisecond)
		w.Shutdown(context.Background())
	}()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	idxCount := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".idx" {
			idxCount++
		}
	}

	if idxCount == 0 {
		t.Fatal("expected .idx files for sealed segments")
	}
}

func TestWithStartLSN_Segmented(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, WithStartLSN(1000))
	if err != nil {
		t.Fatal(err)
	}

	lsn, _ := writeOne(w, []byte("data"), nil, nil)
	if lsn != 1000 {
		t.Fatalf("expected LSN 1000, got %d", lsn)
	}

	segs := w.Segments()
	if len(segs) != 1 {
		t.Fatalf("segments: %d", len(segs))
	}
	if segs[0].FirstLSN != 1000 {
		t.Fatalf("segment firstLSN: %d, want 1000", segs[0].FirstLSN)
	}

	w.Shutdown(context.Background())
}
