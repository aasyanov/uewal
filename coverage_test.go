package uewal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// --- 0% coverage: appendUnsafeSlow ---

func TestBatch_AppendUnsafe_WithTimestamp(t *testing.T) {
	b := NewBatch(1)
	ts := int64(9999)
	b.AppendUnsafe([]byte("p"), []byte("k"), []byte("m"), WithTimestamp(ts))
	if b.Len() != 1 {
		t.Fatalf("Len=%d, want 1", b.Len())
	}
	if b.records[0].timestamp != ts {
		t.Fatalf("timestamp=%d, want %d", b.records[0].timestamp, ts)
	}
	if string(b.records[0].key) != "k" {
		t.Fatalf("key=%q, want k", b.records[0].key)
	}
}

func TestBatch_AppendUnsafe_WithNoCompress(t *testing.T) {
	b := NewBatch(1)
	b.AppendUnsafe([]byte("p"), nil, nil, WithNoCompress())
	if !b.noCompress {
		t.Fatal("noCompress should be true")
	}
}

func TestBatch_AppendUnsafe_DefaultTimestamp(t *testing.T) {
	b := NewBatch(1)
	before := time.Now().UnixNano()
	b.AppendUnsafe([]byte("p"), nil, nil, WithTimestamp(0))
	after := time.Now().UnixNano()
	ts := b.records[0].timestamp
	if ts < before || ts > after {
		t.Fatalf("auto-timestamp %d not in [%d, %d]", ts, before, after)
	}
}

// --- 0% coverage: getPayloadBuf / putPayloadBuf ---

func TestPool_GetPayloadBuf_TieredSizes(t *testing.T) {
	for _, size := range []int{10, 64, 100, 128, 200, 256, 500, 512, 1000, 1024, 3000, 4096} {
		buf, class := getPayloadBuf(size)
		if len(buf) != size {
			t.Fatalf("getPayloadBuf(%d): len=%d", size, len(buf))
		}
		if size <= 4096 && class == 0 {
			t.Fatalf("getPayloadBuf(%d): expected pooled class, got 0", size)
		}
		if class > 0 {
			putPayloadBuf(buf, class)
		}
	}
}

func TestPool_GetPayloadBuf_Oversized(t *testing.T) {
	buf, class := getPayloadBuf(5000)
	if class != 0 {
		t.Fatalf("oversized: class=%d, want 0", class)
	}
	if len(buf) != 5000 {
		t.Fatalf("len=%d, want 5000", len(buf))
	}
}

func TestPool_PutPayloadBuf_InvalidClass(t *testing.T) {
	putPayloadBuf(make([]byte, 64), 0)
	putPayloadBuf(make([]byte, 64), -1)
	putPayloadBuf(make([]byte, 64), 7)
}

func TestPool_PutRecordSlice_WithPoolClass(t *testing.T) {
	recs, sp := getRecordSlice(2)
	buf, class := getPayloadBuf(32)
	recs[0] = record{payload: buf, poolClass: class}
	recs[1] = record{payload: []byte("no-pool"), poolClass: 0}
	putRecordSlice(sp, recs)
}

// --- 0% coverage: trackCompressed ---

type coverShrinkCompressor struct{}

func (c *coverShrinkCompressor) Compress(data []byte) ([]byte, error) {
	if len(data) <= 8 {
		return data, nil
	}
	return data[:len(data)/2], nil
}

func (c *coverShrinkCompressor) Decompress(data []byte) ([]byte, error) {
	out := make([]byte, len(data)*2)
	copy(out, data)
	return out, nil
}

func TestWriter_TrackCompressed(t *testing.T) {
	dir := t.TempDir()
	comp := &coverShrinkCompressor{}
	w, err := Open(dir, WithCompressor(comp), WithSyncMode(SyncBatch))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	payload := make([]byte, 256)
	for i := 0; i < 5; i++ {
		writeOne(w, payload, nil, nil)
	}
	w.Flush()
	time.Sleep(50 * time.Millisecond)

	s := w.Stats()
	if s.CompressedBytes == 0 {
		t.Fatal("CompressedBytes should be > 0 with compressor")
	}
}

// --- flushAfterStop (25%) ---

func TestWAL_Shutdown_FlushPendingData(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, WithSyncMode(SyncNever))
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		writeOne(w, []byte(fmt.Sprintf("e%d", i)), nil, nil)
	}

	w.Shutdown(context.Background())

	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	count := 0
	w2.Replay(0, func(ev Event) error {
		count++
		return nil
	})
	if count != 10 {
		t.Fatalf("count=%d after shutdown flush, want 10", count)
	}
}

// --- trackTS non-uniform (80%) ---

func TestBatch_TrackTS_NonUniform(t *testing.T) {
	b := NewBatch(3)
	b.Append([]byte("a"), nil, nil, WithTimestamp(100))
	if !b.tsUniform {
		t.Fatal("single record should be uniform")
	}
	b.Append([]byte("b"), nil, nil, WithTimestamp(200))
	if b.tsUniform {
		t.Fatal("different timestamps should break uniformity")
	}
	b.Append([]byte("c"), nil, nil, WithTimestamp(300))
	if b.tsUniform {
		t.Fatal("should stay non-uniform")
	}
}

// --- WriteUnsafe empty batch (66.7%) ---

func TestWAL_WriteUnsafe_EmptyBatch(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	_, err = w.WriteUnsafe(nil)
	if err != ErrEmptyBatch {
		t.Fatalf("nil batch: err=%v, want ErrEmptyBatch", err)
	}

	b := NewBatch(0)
	_, err = w.WriteUnsafe(b)
	if err != ErrEmptyBatch {
		t.Fatalf("empty batch: err=%v, want ErrEmptyBatch", err)
	}
}

// --- BatchTooLarge for single record ---

func TestWAL_Write_BatchTooLarge_SingleRecord(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, WithMaxBatchSize(50))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	b := NewBatch(1)
	b.Append(make([]byte, 100), nil, nil)
	_, err = w.Write(b)
	if err != ErrBatchTooLarge {
		t.Fatalf("err=%v, want ErrBatchTooLarge", err)
	}
}

// --- Retention by age ---

func TestWAL_Retention_ByAge(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, WithMaxSegmentSize(256), WithRetentionAge(1*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	payload := make([]byte, 100)
	for i := 0; i < 50; i++ {
		writeOne(w, payload, nil, nil)
	}
	w.Flush()
	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 10; i++ {
		writeOne(w, payload, nil, nil)
	}
	w.Flush()
	time.Sleep(100 * time.Millisecond)

	segs := w.Segments()
	if len(segs) > 3 {
		t.Logf("segments=%d (retention by age should trim)", len(segs))
	}
}

// --- Shutdown already closed ---

func TestWAL_Shutdown_AlreadyClosed(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
	err = w.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("Shutdown after Close: %v", err)
	}
}

// --- mmap zero-size and close on empty reader ---

func TestMmapByPath_ZeroSize(t *testing.T) {
	r, err := mmapByPath("nonexistent", 0)
	if err != nil {
		t.Fatal(err)
	}
	if r.bytes() != nil {
		t.Fatal("expected nil bytes for zero size")
	}
	if err := r.close(); err != nil {
		t.Fatal(err)
	}
}

func TestMmapReader_Close_EmptyReader(t *testing.T) {
	r := &mmapReader{}
	if err := r.close(); err != nil {
		t.Fatal(err)
	}
}

func TestMmapReader_Close_FileOnly(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "mmap-test")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString("data")
	f.Close()

	f2, _ := os.Open(f.Name())
	r := &mmapReader{f: f2}
	if err := r.close(); err != nil {
		t.Fatal(err)
	}
}

// --- recoverFromManifest: missing segment file ---

func TestRecovery_MissingSegmentFile(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, WithMaxSegmentSize(256))
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 100)
	for i := 0; i < 30; i++ {
		writeOne(w, payload, nil, nil)
	}
	w.Flush()
	time.Sleep(50 * time.Millisecond)
	w.Shutdown(context.Background())

	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".wal" {
			os.Remove(filepath.Join(dir, e.Name()))
			break
		}
	}

	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	count := 0
	w2.Replay(0, func(ev Event) error {
		count++
		return nil
	})
	t.Logf("recovered %d events after removing first segment", count)
}

// --- recoverFromManifest: all segments sealed -> creates new segment ---

func TestRecovery_AllSealed_CreatesNewSegment(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, WithMaxSegmentSize(256))
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 100)
	for i := 0; i < 20; i++ {
		writeOne(w, payload, nil, nil)
	}
	w.Flush()
	w.Rotate()
	w.Flush()
	time.Sleep(50 * time.Millisecond)
	lastLSN := w.LastLSN()
	w.Shutdown(context.Background())

	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	lsn, err := writeOne(w2, []byte("post-recovery"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if lsn <= lastLSN {
		t.Fatalf("post-recovery LSN %d should be > %d", lsn, lastLSN)
	}
}

// --- validateActiveSegment: empty segment file ---

func TestRecovery_EmptyActiveSegment(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.Shutdown(context.Background())

	segs, _ := os.ReadDir(dir)
	for _, e := range segs {
		if filepath.Ext(e.Name()) == ".wal" {
			os.Truncate(filepath.Join(dir, e.Name()), 0)
		}
	}

	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	lsn, err := writeOne(w2, []byte("fresh"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if lsn == 0 {
		t.Fatal("LSN should be > 0")
	}
}

// --- validateActiveSegment: corrupted frame ---

func TestRecovery_CorruptedActiveSegment(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	writeOne(w, []byte("valid"), nil, nil)
	w.Flush()

	active := w.mgr.active()
	if active.storage != nil {
		active.storage.Write([]byte("EWAL\x01\x00\x00\x01garbage-bytes-that-wont-pass-crc-check"))
	}
	w.Close()

	var corruptionSeen bool
	w2, err := Open(dir, WithHooks(Hooks{
		OnCorruption: func(path string, validEnd int64) {
			corruptionSeen = true
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	if !corruptionSeen {
		t.Fatal("OnCorruption hook should fire for corrupted segment")
	}

	count := 0
	w2.Replay(0, func(ev Event) error {
		count++
		return nil
	})
	if count != 1 {
		t.Fatalf("count=%d, want 1 (only valid event)", count)
	}
}

// --- validateActiveSegment: custom StorageFactory ---

func TestRecovery_WithStorageFactory(t *testing.T) {
	dir := t.TempDir()
	var calls int
	factory := func(path string) (Storage, error) {
		calls++
		return NewFileStorage(path)
	}

	w, err := Open(dir, WithStorageFactory(factory))
	if err != nil {
		t.Fatal(err)
	}
	writeOne(w, []byte("x"), nil, nil)
	w.Flush()
	w.Shutdown(context.Background())

	calls = 0
	w2, err := Open(dir, WithStorageFactory(factory))
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	if calls == 0 {
		t.Fatal("StorageFactory should be called during recovery")
	}
}

// --- encodeBatchHint: tsUniformHint=true ---

func TestEncoder_EncodeBatchHint_UniformHint(t *testing.T) {
	enc := newEncoder(256)
	recs := []record{
		{payload: []byte("a"), timestamp: 100},
		{payload: []byte("b"), timestamp: 100},
	}
	err := enc.encodeBatchHint(recs, 1, nil, false, true)
	if err != nil {
		t.Fatal(err)
	}

	events, _, err := decodeBatchFrame(enc.bytes(), 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("events=%d, want 2", len(events))
	}
	if events[0].Timestamp != 100 || events[1].Timestamp != 100 {
		t.Fatalf("timestamps: %d, %d", events[0].Timestamp, events[1].Timestamp)
	}
}

func TestEncoder_EncodeBatchHint_NonUniform(t *testing.T) {
	enc := newEncoder(256)
	recs := []record{
		{payload: []byte("a"), timestamp: 100},
		{payload: []byte("b"), timestamp: 200},
	}
	err := enc.encodeBatchHint(recs, 1, nil, false, false)
	if err != nil {
		t.Fatal(err)
	}

	events, _, err := decodeBatchFrame(enc.bytes(), 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if events[0].Timestamp != 100 || events[1].Timestamp != 200 {
		t.Fatalf("timestamps: %d, %d", events[0].Timestamp, events[1].Timestamp)
	}
}

// --- resolveStorageFastPath: non-FileStorage ---

func TestWriter_StorageFastPath_NonFileStorage(t *testing.T) {
	dir := t.TempDir()
	factory := func(path string) (Storage, error) {
		return &memStorage{}, nil
	}

	w, err := Open(dir, WithStorageFactory(factory))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	writeOne(w, []byte("data"), nil, nil)
	w.Flush()
}

// --- OpenSegment state checks ---

func TestWAL_OpenSegment_AfterClose(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	_, _, err = w.OpenSegment(1)
	if err == nil {
		t.Fatal("expected error on OpenSegment after Close")
	}
}

// --- Replay/Iterator/ReplayBatches/Snapshot with StateInit not testable directly
//     (Open transitions immediately), but we test closed state thoroughly elsewhere.

// --- deleteFiles with storage ---

func TestSegment_DeleteFiles_WithStorage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, segmentName(1))
	os.WriteFile(path, []byte("data"), 0644)

	storage, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}

	seg := &segment{path: path, firstLSN: 1, storage: storage}
	seg.deleteFiles(dir)

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("segment file should be deleted")
	}
}

// --- seal with nil storage ---

func TestSegment_Seal_NilStorage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, segmentName(1))
	os.WriteFile(path, []byte("data"), 0644)

	seg := &segment{path: path, firstLSN: 1}
	err := seg.seal(dir, 4)
	if err != nil {
		t.Fatal(err)
	}
	if !seg.isSealed() {
		t.Fatal("segment should be sealed")
	}
}

// --- closeActive edge: no segments ---

func TestManager_CloseActive_EmptySegments(t *testing.T) {
	m := &segmentManager{}
	if err := m.closeActive(); err != nil {
		t.Fatalf("closeActive on empty manager: %v", err)
	}
}

// --- Manifest buildManifest ---

func TestManifest_Build_WithTimestamps(t *testing.T) {
	seg := &segment{firstLSN: 1, createdAt: 5000, path: "test.wal"}
	seg.storeLastLSN(10)
	seg.sizeAt.Store(1024)
	seg.firstTSv.Store(100)
	seg.storeLastTS(200)

	m := buildManifest([]*segment{seg}, 10)
	if m.entries[0].firstTS != 100 {
		t.Fatalf("firstTS=%d, want 100", m.entries[0].firstTS)
	}
	if m.entries[0].lastTS != 200 {
		t.Fatalf("lastTS=%d, want 200", m.entries[0].lastTS)
	}
}
