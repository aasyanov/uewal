package uewal

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aasyanov/uewal/internal/crc"
)

func TestReplication_OpenSegment_Basic(t *testing.T) {
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

func TestReplication_ImportBatch_Basic(t *testing.T) {
	primaryDir := t.TempDir()
	primary, err := Open(primaryDir)
	if err != nil {
		t.Fatal(err)
	}

	if _, err = writeOne(primary, []byte("replicated-1"), nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err = writeOne(primary, []byte("replicated-2"), nil, nil); err != nil {
		t.Fatal(err)
	}
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

func TestReplication_ImportSegment_Basic(t *testing.T) {
	primaryDir := t.TempDir()
	primary, err := Open(primaryDir, WithMaxSegmentSize(256))
	if err != nil {
		t.Fatal(err)
	}

	payload := make([]byte, 100)
	for i := 0; i < 20; i++ {
		if _, err = writeOne(primary, payload, nil, nil); err != nil {
			t.Fatal(err)
		}
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

	if err = replica.ImportSegment(tmpFile); err != nil {
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

func TestImportBatch_ZeroCount(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Shutdown(context.Background())

	// Craft a frame with count=0 but valid magic and CRC.
	frame := make([]byte, batchOverhead)
	copy(frame[0:4], batchMagic[:])
	frame[4] = batchVersion
	// count = 0 at offset 6:8 (already zero)
	// batchSize at offset 24:28
	frame[24] = byte(batchOverhead)

	// Fix CRC
	crcOff := batchOverhead - batchTrailerLen
	checksum := crc.Checksum(frame[:crcOff])
	frame[crcOff] = byte(checksum)
	frame[crcOff+1] = byte(checksum >> 8)
	frame[crcOff+2] = byte(checksum >> 16)
	frame[crcOff+3] = byte(checksum >> 24)

	err = w.ImportBatch(frame)
	if err != ErrImportInvalid {
		t.Fatalf("expected ErrImportInvalid for count=0, got %v", err)
	}
}

func TestImportSegment_UpdatesLSN(t *testing.T) {
	primaryDir := t.TempDir()
	primary, err := Open(primaryDir, WithMaxSegmentSize(256))
	if err != nil {
		t.Fatal(err)
	}

	payload := make([]byte, 100)
	for i := 0; i < 20; i++ {
		if _, err = writeOne(primary, payload, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	primary.Flush()
	time.Sleep(50 * time.Millisecond)

	segs := primary.Segments()
	var sealedPath string
	var sealedLastLSN LSN
	for _, s := range segs {
		if s.Sealed {
			sealedPath = s.Path
			sealedLastLSN = s.LastLSN
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
	replica, err := Open(replicaDir)
	if err != nil {
		t.Fatal(err)
	}

	if err = replica.ImportSegment(tmpFile); err != nil {
		t.Fatal(err)
	}

	if replica.LastLSN() < sealedLastLSN {
		t.Fatalf("replica LSN %d should be >= imported LastLSN %d",
			replica.LastLSN(), sealedLastLSN)
	}

	// Append after import should produce LSNs beyond the imported range.
	lsn, err := writeOne(replica, []byte("after-import"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if lsn <= sealedLastLSN {
		t.Fatalf("post-import LSN %d should be > %d", lsn, sealedLastLSN)
	}

	replica.Shutdown(context.Background())
}

func TestImportSegment_SortOrder(t *testing.T) {
	primaryDir := t.TempDir()
	primary, err := Open(primaryDir, WithMaxSegmentSize(200))
	if err != nil {
		t.Fatal(err)
	}

	payload := make([]byte, 80)
	for i := 0; i < 30; i++ {
		if _, err = writeOne(primary, payload, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	primary.Flush()
	time.Sleep(50 * time.Millisecond)

	segs := primary.Segments()
	var sealedPaths []string
	for _, s := range segs {
		if s.Sealed {
			sealedPaths = append(sealedPaths, s.Path)
		}
	}
	primary.Shutdown(context.Background())

	if len(sealedPaths) < 2 {
		t.Skipf("need >= 2 sealed segments, got %d", len(sealedPaths))
	}

	replicaDir := t.TempDir()
	replica, err := Open(replicaDir)
	if err != nil {
		t.Fatal(err)
	}

	// Import in reverse order to test sort.
	for i := len(sealedPaths) - 1; i >= 0; i-- {
		tmpFile := filepath.Join(t.TempDir(), "seg.wal")
		data, _ := os.ReadFile(sealedPaths[i])
		os.WriteFile(tmpFile, data, 0644)
		if err = replica.ImportSegment(tmpFile); err != nil {
			t.Fatal(err)
		}
	}

	replicaSegs := replica.Segments()
	for i := 1; i < len(replicaSegs); i++ {
		if replicaSegs[i].FirstLSN < replicaSegs[i-1].FirstLSN && replicaSegs[i].Sealed {
			t.Fatalf("segments out of order: [%d].FirstLSN=%d < [%d].FirstLSN=%d",
				i, replicaSegs[i].FirstLSN, i-1, replicaSegs[i-1].FirstLSN)
		}
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

func TestImportBatch_Stats(t *testing.T) {
	primaryDir := t.TempDir()
	primary, err := Open(primaryDir)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		if _, err = writeOne(primary, []byte("data"), nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	primary.Flush()

	segs := primary.Segments()
	segData, err := os.ReadFile(segs[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	var frames [][]byte
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
	replica.Flush()
	time.Sleep(20 * time.Millisecond)

	s := replica.Stats()
	if s.ImportBatches != uint64(len(frames)) {
		t.Errorf("ImportBatches=%d, want %d", s.ImportBatches, len(frames))
	}
	if s.ImportBytes == 0 {
		t.Error("ImportBytes=0, want >0")
	}

	replica.Shutdown(context.Background())
}

func TestImportBatch_OnImportHook(t *testing.T) {
	primaryDir := t.TempDir()
	primary, err := Open(primaryDir)
	if err != nil {
		t.Fatal(err)
	}

	if _, err = writeOne(primary, []byte("hook-test"), nil, nil); err != nil {
		t.Fatal(err)
	}
	primary.Flush()

	segs := primary.Segments()
	segData, err := os.ReadFile(segs[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	var frames [][]byte
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

	var importCalls int
	var importFirstLSN, importLastLSN LSN
	var importBytes int

	replicaDir := t.TempDir()
	replica, err := Open(replicaDir, WithStartLSN(1), WithHooks(Hooks{
		OnImport: func(firstLSN, lastLSN LSN, bytes int) {
			importCalls++
			importFirstLSN = firstLSN
			importLastLSN = lastLSN
			importBytes = bytes
		},
	}))
	if err != nil {
		t.Fatal(err)
	}

	for _, frame := range frames {
		if err := replica.ImportBatch(frame); err != nil {
			t.Fatal(err)
		}
	}
	replica.Flush()
	time.Sleep(20 * time.Millisecond)

	if importCalls != len(frames) {
		t.Errorf("OnImport calls=%d, want %d", importCalls, len(frames))
	}
	if importFirstLSN == 0 {
		t.Error("OnImport firstLSN=0, want >0")
	}
	if importLastLSN == 0 {
		t.Error("OnImport lastLSN=0, want >0")
	}
	if importBytes == 0 {
		t.Error("OnImport bytes=0, want >0")
	}

	replica.Shutdown(context.Background())
}

func TestImportSegment_Stats(t *testing.T) {
	primaryDir := t.TempDir()
	primary, err := Open(primaryDir, WithMaxSegmentSize(256))
	if err != nil {
		t.Fatal(err)
	}

	payload := make([]byte, 100)
	for i := 0; i < 20; i++ {
		if _, err = writeOne(primary, payload, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	primary.Flush()
	time.Sleep(20 * time.Millisecond)
	primary.Shutdown(context.Background())

	segs := primary.Segments()
	var sealedPaths []string
	for _, seg := range segs {
		if seg.Sealed {
			sealedPaths = append(sealedPaths, seg.Path)
		}
	}
	if len(sealedPaths) == 0 {
		t.Skip("no sealed segments to import")
	}

	var importCalls int
	replicaDir := t.TempDir()
	replica, err := Open(replicaDir, WithStartLSN(1), WithHooks(Hooks{
		OnImport: func(LSN, LSN, int) { importCalls++ },
	}))
	if err != nil {
		t.Fatal(err)
	}

	for _, p := range sealedPaths {
		if err := replica.ImportSegment(p); err != nil {
			t.Fatal(err)
		}
	}

	s := replica.Stats()
	if s.ImportBatches == 0 {
		t.Error("ImportBatches=0 after ImportSegment, want >0")
	}
	if s.ImportBytes == 0 {
		t.Error("ImportBytes=0 after ImportSegment, want >0")
	}
	if importCalls != len(sealedPaths) {
		t.Errorf("OnImport calls=%d, want %d", importCalls, len(sealedPaths))
	}

	replica.Shutdown(context.Background())
}
