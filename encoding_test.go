package uewal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
)

func TestEncodeDecode_SimpleRecords(t *testing.T) {
	recs := []record{
		{payload: []byte("hello"), timestamp: 1000},
		{payload: []byte("world"), timestamp: 1000},
	}

	buf := make([]byte, 256)
	frame, size, err := encodeBatchFrame(buf, recs, 1, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if size < batchOverhead {
		t.Fatalf("frame too small: %d", size)
	}

	// Verify header.
	if string(frame[0:4]) != "EWAL" {
		t.Fatal("magic mismatch")
	}
	if frame[4] != 1 {
		t.Fatalf("version: got %d", frame[4])
	}
	flags := frame[5]
	if flags&flagPerRecordTS != 0 {
		t.Fatal("uniform timestamps should not set per_record_ts")
	}
	count := binary.LittleEndian.Uint16(frame[6:8])
	if count != 2 {
		t.Fatalf("count: got %d", count)
	}
	firstLSN := binary.LittleEndian.Uint64(frame[8:16])
	if firstLSN != 1 {
		t.Fatalf("firstLSN: got %d", firstLSN)
	}

	events, next, err := decodeBatchFrame(frame, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if next != size {
		t.Fatalf("next=%d size=%d", next, size)
	}
	if len(events) != 2 {
		t.Fatalf("events: %d", len(events))
	}
	if string(events[0].Payload) != "hello" || events[0].LSN != 1 {
		t.Fatalf("event 0: %+v", events[0])
	}
	if string(events[1].Payload) != "world" || events[1].LSN != 2 {
		t.Fatalf("event 1: %+v", events[1])
	}
	if events[0].Timestamp != 1000 || events[1].Timestamp != 1000 {
		t.Fatal("timestamps should be uniform 1000")
	}
}

func TestEncodeDecode_PerRecordTimestamp(t *testing.T) {
	recs := []record{
		{payload: []byte("a"), timestamp: 100},
		{payload: []byte("b"), timestamp: 200},
	}

	buf := make([]byte, 256)
	frame, _, err := encodeBatchFrame(buf, recs, 10, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	if frame[5]&flagPerRecordTS == 0 {
		t.Fatal("per_record_ts flag should be set")
	}

	events, _, err := decodeBatchFrame(frame, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if events[0].Timestamp != 100 || events[1].Timestamp != 200 {
		t.Fatalf("timestamps: %d, %d", events[0].Timestamp, events[1].Timestamp)
	}
	if events[0].LSN != 10 || events[1].LSN != 11 {
		t.Fatalf("LSNs: %d, %d", events[0].LSN, events[1].LSN)
	}
}

func TestEncodeDecode_KeyAndMeta(t *testing.T) {
	recs := []record{
		{
			payload:   []byte("data"),
			key:       []byte("user-1"),
			meta:      []byte("created"),
			timestamp: 500,
		},
	}

	buf := make([]byte, 256)
	frame, _, err := encodeBatchFrame(buf, recs, 1, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	events, _, err := decodeBatchFrame(frame, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("events: %d", len(events))
	}
	ev := events[0]
	if string(ev.Key) != "user-1" {
		t.Fatalf("key: %q", ev.Key)
	}
	if string(ev.Meta) != "created" {
		t.Fatalf("meta: %q", ev.Meta)
	}
	if string(ev.Payload) != "data" {
		t.Fatalf("payload: %q", ev.Payload)
	}
}

func TestEncoding_ScanBatchFrame_Basic(t *testing.T) {
	recs := []record{
		{payload: []byte("x"), timestamp: 999},
		{payload: []byte("y"), timestamp: 999},
		{payload: []byte("z"), timestamp: 999},
	}

	buf := make([]byte, 256)
	frame, size, err := encodeBatchFrame(buf, recs, 100, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	info, err := scanBatchFrame(frame, 0)
	if err != nil {
		t.Fatal(err)
	}
	if info.firstLSN != 100 {
		t.Fatalf("firstLSN: %d", info.firstLSN)
	}
	if info.count != 3 {
		t.Fatalf("count: %d", info.count)
	}
	if info.timestamp != 999 {
		t.Fatalf("timestamp: %d", info.timestamp)
	}
	if info.frameEnd != size {
		t.Fatalf("frameEnd: %d, size: %d", info.frameEnd, size)
	}
}

func TestEncodeDecode_CRCMismatch(t *testing.T) {
	recs := []record{{payload: []byte("data"), timestamp: 1}}
	buf := make([]byte, 128)
	frame, size, err := encodeBatchFrame(buf, recs, 1, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	frame[size-5] ^= 0xFF // corrupt data before CRC

	_, _, err = decodeBatchFrame(frame, 0, nil)
	if err != ErrCRCMismatch {
		t.Fatalf("expected ErrCRCMismatch, got %v", err)
	}
}

func TestEncodeDecode_Truncated(t *testing.T) {
	recs := []record{{payload: []byte("data"), timestamp: 1}}
	buf := make([]byte, 128)
	frame, _, err := encodeBatchFrame(buf, recs, 1, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = decodeBatchFrame(frame[:10], 0, nil)
	if err != ErrInvalidRecord {
		t.Fatalf("expected ErrInvalidRecord, got %v", err)
	}
}

func TestEncoder_MultipleBatches(t *testing.T) {
	enc := newEncoder(256)

	recs1 := []record{{payload: []byte("batch1"), timestamp: 1}}
	recs2 := []record{{payload: []byte("batch2"), timestamp: 2}}

	if err := enc.encodeBatch(recs1, 1, nil, false); err != nil {
		t.Fatal(err)
	}
	if err := enc.encodeBatch(recs2, 2, nil, false); err != nil {
		t.Fatal(err)
	}

	events, _, err := decodeAllBatches(enc.bytes(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("events: %d", len(events))
	}
	if string(events[0].Payload) != "batch1" || events[0].LSN != 1 {
		t.Fatalf("event 0: %+v", events[0])
	}
	if string(events[1].Payload) != "batch2" || events[1].LSN != 2 {
		t.Fatalf("event 1: %+v", events[1])
	}
}

func TestEncodeDecode_EmptyKeyMeta(t *testing.T) {
	recs := []record{{payload: []byte("data"), timestamp: 42}}
	buf := make([]byte, 128)
	frame, _, err := encodeBatchFrame(buf, recs, 1, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	events, _, err := decodeBatchFrame(frame, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if events[0].Key != nil || events[0].Meta != nil {
		t.Fatalf("expected nil key/meta, got key=%v meta=%v", events[0].Key, events[0].Meta)
	}
}

func TestEncoding_HeaderSize(t *testing.T) {
	if batchHeaderLen != 28 {
		t.Fatalf("header size: %d, expected 28", batchHeaderLen)
	}
	if batchOverhead != 32 {
		t.Fatalf("overhead: %d, expected 32", batchOverhead)
	}
}

// --- Compression tests ---

type testCompressor struct {
	compressErr   error
	decompressErr error
}

func (c *testCompressor) Compress(data []byte) ([]byte, error) {
	if c.compressErr != nil {
		return nil, c.compressErr
	}
	// Simple "compression": prefix with magic byte + original data (always smaller test not hit).
	out := make([]byte, 0, len(data)+1)
	out = append(out, 0xCC)
	out = append(out, data...)
	return out, nil
}

func (c *testCompressor) Decompress(data []byte) ([]byte, error) {
	if c.decompressErr != nil {
		return nil, c.decompressErr
	}
	if len(data) < 1 || data[0] != 0xCC {
		return nil, ErrDecompress
	}
	return data[1:], nil
}

// realCompressor simulates effective compression by returning shorter output.
type realCompressor struct{}

func (c *realCompressor) Compress(data []byte) ([]byte, error) {
	if len(data) <= 4 {
		return data, nil
	}
	out := make([]byte, len(data)/2)
	copy(out, data[:len(data)/2])
	return out, nil
}

func (c *realCompressor) Decompress(data []byte) ([]byte, error) {
	out := make([]byte, len(data)*2)
	copy(out, data)
	return out, nil
}

func TestEncodeDecode_CompressionAutoBypass(t *testing.T) {
	recs := []record{{payload: []byte("data"), timestamp: 1}}
	buf := make([]byte, 256)

	comp := &testCompressor{}
	frame, _, err := encodeBatchFrame(buf, recs, 1, comp, false)
	if err != nil {
		t.Fatal(err)
	}

	if frame[5]&flagCompressed != 0 {
		t.Fatal("compression should be auto-bypassed when output is larger")
	}

	events, _, err := decodeBatchFrame(frame, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(events[0].Payload) != "data" {
		t.Fatalf("payload: %q", events[0].Payload)
	}
}

func TestEncodeDecode_CompressionEffective(t *testing.T) {
	payload := make([]byte, 256)
	recs := []record{{payload: payload, timestamp: 1}}
	buf := make([]byte, 1024)

	comp := &realCompressor{}
	frame, _, err := encodeBatchFrame(buf, recs, 1, comp, false)
	if err != nil {
		t.Fatal(err)
	}

	if frame[5]&flagCompressed == 0 {
		t.Fatal("expected flagCompressed to be set")
	}

	events, _, err := decodeBatchFrame(frame, 0, comp)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("events: %d", len(events))
	}
}

func TestDecode_CompressorRequired(t *testing.T) {
	payload := make([]byte, 256)
	recs := []record{{payload: payload, timestamp: 1}}
	buf := make([]byte, 1024)

	comp := &realCompressor{}
	frame, _, err := encodeBatchFrame(buf, recs, 1, comp, false)
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = decodeBatchFrame(frame, 0, nil)
	if err != ErrCompressorRequired {
		t.Fatalf("expected ErrCompressorRequired, got %v", err)
	}
}

func TestDecode_DecompressError(t *testing.T) {
	payload := make([]byte, 256)
	recs := []record{{payload: payload, timestamp: 1}}
	buf := make([]byte, 1024)

	comp := &realCompressor{}
	frame, _, err := encodeBatchFrame(buf, recs, 1, comp, false)
	if err != nil {
		t.Fatal(err)
	}

	badDecomp := &testCompressor{decompressErr: fmt.Errorf("bad")}
	_, _, err = decodeBatchFrame(frame, 0, badDecomp)
	if !errors.Is(err, ErrDecompress) {
		t.Fatalf("expected ErrDecompress, got %v", err)
	}
}

func TestEncode_CompressorError(t *testing.T) {
	recs := []record{{payload: make([]byte, 128), timestamp: 1}}
	buf := make([]byte, 512)

	comp := &testCompressor{compressErr: fmt.Errorf("compress boom")}
	_, _, err := encodeBatchFrame(buf, recs, 1, comp, false)
	if err == nil {
		t.Fatal("expected compressor error")
	}
}

func TestEncode_NoCompress(t *testing.T) {
	recs := []record{{payload: make([]byte, 256), timestamp: 1}}
	buf := make([]byte, 1024)

	comp := &realCompressor{}
	frame, _, err := encodeBatchFrame(buf, recs, 1, comp, true)
	if err != nil {
		t.Fatal(err)
	}
	if frame[5]&flagCompressed != 0 {
		t.Fatal("noCompress=true should skip compression")
	}
}

func TestScanBatchFrame_BadMagic(t *testing.T) {
	data := make([]byte, 64)
	copy(data[0:4], []byte("XXXX"))
	_, err := scanBatchFrame(data, 0)
	if err != ErrInvalidRecord {
		t.Fatalf("expected ErrInvalidRecord for bad magic, got %v", err)
	}
}

func TestScanBatchFrame_BadVersion(t *testing.T) {
	recs := []record{{payload: []byte("x"), timestamp: 1}}
	buf := make([]byte, 128)
	frame, _, _ := encodeBatchFrame(buf, recs, 1, nil, false)

	frame[4] = 0
	_, err := scanBatchFrame(frame, 0)
	if err != ErrInvalidRecord {
		t.Fatalf("expected ErrInvalidRecord for version=0, got %v", err)
	}

	frame[4] = batchVersion + 1
	_, err = scanBatchFrame(frame, 0)
	if err != ErrInvalidRecord {
		t.Fatalf("expected ErrInvalidRecord for future version, got %v", err)
	}
}

func TestScanBatchFrame_TooSmallBatchSize(t *testing.T) {
	recs := []record{{payload: []byte("x"), timestamp: 1}}
	buf := make([]byte, 128)
	frame, _, _ := encodeBatchFrame(buf, recs, 1, nil, false)

	binary.LittleEndian.PutUint32(frame[24:28], uint32(batchOverhead-1))
	_, err := scanBatchFrame(frame, 0)
	if err != ErrInvalidRecord {
		t.Fatalf("expected ErrInvalidRecord for too-small batchSize, got %v", err)
	}
}

func TestDecodeBatchFrameInto_Reuse(t *testing.T) {
	recs := []record{
		{payload: []byte("a"), timestamp: 1},
		{payload: []byte("b"), timestamp: 1},
	}
	buf := make([]byte, 256)
	frame, _, err := encodeBatchFrame(buf, recs, 1, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	reuseBuf := make([]Event, 0, 10)
	events, _, err := decodeBatchFrameInto(frame, 0, nil, reuseBuf)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("events: %d", len(events))
	}
	if cap(events) != 10 {
		t.Fatalf("expected reuse: cap=%d", cap(events))
	}
}

func TestDecodeAllBatches_PartialCorruption(t *testing.T) {
	enc := newEncoder(256)
	enc.encodeBatch([]record{{payload: []byte("ok"), timestamp: 1}}, 1, nil, false)
	data := make([]byte, len(enc.bytes())+10)
	copy(data, enc.bytes())
	copy(data[len(enc.bytes()):], []byte("corrupted!"))

	events, lastValid, err := decodeAllBatches(data, nil)
	if err == nil {
		t.Fatal("expected error from partial corruption")
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 valid event before corruption, got %d", len(events))
	}
	if lastValid != len(enc.bytes()) {
		t.Fatalf("lastValid=%d, expected %d", lastValid, len(enc.bytes()))
	}
}

func TestEncoding_RecordFixedLen(t *testing.T) {
	if got := recordFixedLen(true); got != 16 {
		t.Fatalf("perRecTS=true: got %d, want 16", got)
	}
	if got := recordFixedLen(false); got != 8 {
		t.Fatalf("perRecTS=false: got %d, want 8", got)
	}
}

func TestEncoding_UniformTimestamp(t *testing.T) {
	if !uniformTimestamp(nil) {
		t.Fatal("nil should be uniform")
	}
	if !uniformTimestamp([]record{{timestamp: 1}}) {
		t.Fatal("single record should be uniform")
	}
	if !uniformTimestamp([]record{{timestamp: 5}, {timestamp: 5}}) {
		t.Fatal("same timestamps should be uniform")
	}
	if uniformTimestamp([]record{{timestamp: 1}, {timestamp: 2}}) {
		t.Fatal("different timestamps should not be uniform")
	}
}

func TestEncoding_RecordsRegionSize(t *testing.T) {
	recs := []record{
		{payload: []byte("abc"), key: []byte("k"), timestamp: 1},
	}
	got := recordsRegionSize(recs, false)
	want := 8 + 1 + 0 + 3 // fixed(8) + key(1) + meta(0) + payload(3)
	if got != want {
		t.Fatalf("recordsRegionSize(perRecTS=false): got %d, want %d", got, want)
	}
	got = recordsRegionSize(recs, true)
	want = 16 + 1 + 0 + 3
	if got != want {
		t.Fatalf("recordsRegionSize(perRecTS=true): got %d, want %d", got, want)
	}
}

func TestEncoder_Grow(t *testing.T) {
	enc := newEncoder(4)
	enc.encodeBatch([]record{{payload: make([]byte, 100), timestamp: 1}}, 1, nil, false)
	if enc.len() < 100 {
		t.Fatal("encoder should have grown to fit data")
	}
	enc.reset()
	if enc.len() != 0 {
		t.Fatal("reset should zero length")
	}
	if cap(enc.bytes()) < 100 {
		t.Fatal("capacity should be preserved after reset")
	}
}
