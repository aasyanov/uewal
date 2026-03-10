package uewal

import (
	"bytes"
	"testing"
)

func TestBatchFrameRoundTrip(t *testing.T) {
	events := []Event{
		{Payload: []byte("hello"), Meta: []byte("type:greeting")},
		{Payload: []byte("world")},
	}

	buf := make([]byte, batchFrameSize(events)*2)
	frame, n, err := encodeBatchFrame(buf, events, 1, nil)
	if err != nil {
		t.Fatalf("encodeBatchFrame: %v", err)
	}

	decoded, next, err := decodeBatchFrame(frame[:n], 0, nil)
	if err != nil {
		t.Fatalf("decodeBatchFrame: %v", err)
	}
	if next != n {
		t.Fatalf("next=%d, want %d", next, n)
	}
	if len(decoded) != 2 {
		t.Fatalf("got %d events, want 2", len(decoded))
	}
	if decoded[0].LSN != 1 || decoded[1].LSN != 2 {
		t.Fatalf("LSNs: %d, %d; want 1, 2", decoded[0].LSN, decoded[1].LSN)
	}
	if !bytes.Equal(decoded[0].Payload, []byte("hello")) {
		t.Fatalf("Payload[0]=%q, want %q", decoded[0].Payload, "hello")
	}
	if !bytes.Equal(decoded[0].Meta, []byte("type:greeting")) {
		t.Fatalf("Meta[0]=%q, want %q", decoded[0].Meta, "type:greeting")
	}
	if !bytes.Equal(decoded[1].Payload, []byte("world")) {
		t.Fatalf("Payload[1]=%q, want %q", decoded[1].Payload, "world")
	}
	if decoded[1].Meta != nil {
		t.Fatalf("Meta[1] should be nil, got %q", decoded[1].Meta)
	}
}

func TestBatchFrameEmptyPayload(t *testing.T) {
	events := []Event{{Payload: nil}}
	buf := make([]byte, batchFrameSize(events)*2)
	frame, _, err := encodeBatchFrame(buf, events, 1, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, _, err := decodeBatchFrame(frame, 0, nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(decoded) != 1 {
		t.Fatalf("got %d events, want 1", len(decoded))
	}
	if len(decoded[0].Payload) != 0 {
		t.Fatalf("Payload=%q, want empty", decoded[0].Payload)
	}
}

func TestBatchFrameMultiple(t *testing.T) {
	enc := newEncoder(256)
	for batch := 0; batch < 3; batch++ {
		events := make([]Event, 5)
		firstLSN := LSN(batch*5 + 1)
		for i := range events {
			events[i].Payload = []byte("data")
		}
		if err := enc.encodeBatch(events, firstLSN, nil); err != nil {
			t.Fatalf("encodeBatch %d: %v", batch, err)
		}
	}

	all, off, err := decodeAllBatches(enc.bytes(), nil)
	if err != nil {
		t.Fatalf("decodeAllBatches: %v", err)
	}
	if off != len(enc.bytes()) {
		t.Fatalf("offset=%d, want %d", off, len(enc.bytes()))
	}
	if len(all) != 15 {
		t.Fatalf("got %d events, want 15", len(all))
	}
	for i, ev := range all {
		if ev.LSN != LSN(i+1) {
			t.Errorf("events[%d].LSN=%d, want %d", i, ev.LSN, i+1)
		}
	}
}

func TestDecodeTruncatedBatchFrame(t *testing.T) {
	events := []Event{{Payload: []byte("data")}}
	buf := make([]byte, batchFrameSize(events)*2)
	frame, n, _ := encodeBatchFrame(buf, events, 1, nil)

	for _, trunc := range []int{0, 10, batchHeaderLen, n - 1} {
		_, _, err := decodeBatchFrame(frame[:trunc], 0, nil)
		if err != ErrInvalidRecord {
			t.Errorf("truncated at %d: got err=%v, want ErrInvalidRecord", trunc, err)
		}
	}
}

func TestDecodeBatchCRCMismatch(t *testing.T) {
	events := []Event{{Payload: []byte("data")}}
	buf := make([]byte, batchFrameSize(events)*2)
	frame, n, _ := encodeBatchFrame(buf, events, 1, nil)

	frame[batchHeaderLen] ^= 0xFF

	_, _, err := decodeBatchFrame(frame[:n], 0, nil)
	if err != ErrCRCMismatch {
		t.Fatalf("got err=%v, want ErrCRCMismatch", err)
	}
}

func TestDecodeBatchInvalidMagic(t *testing.T) {
	events := []Event{{Payload: []byte("data")}}
	buf := make([]byte, batchFrameSize(events)*2)
	frame, n, _ := encodeBatchFrame(buf, events, 1, nil)

	frame[0] = 0xFF

	_, _, err := decodeBatchFrame(frame[:n], 0, nil)
	if err != ErrInvalidRecord {
		t.Fatalf("got err=%v, want ErrInvalidRecord", err)
	}
}

func TestDecodeBatchInvalidVersion(t *testing.T) {
	events := []Event{{Payload: []byte("data")}}
	buf := make([]byte, batchFrameSize(events)*2)
	frame, n, _ := encodeBatchFrame(buf, events, 1, nil)

	frame[4] = 99
	frame[5] = 0

	_, _, err := decodeBatchFrame(frame[:n], 0, nil)
	if err != ErrInvalidRecord {
		t.Fatalf("got err=%v, want ErrInvalidRecord", err)
	}
}

func TestDecodeAllStopsAtCorruption(t *testing.T) {
	enc := newEncoder(256)
	for i := 0; i < 3; i++ {
		events := []Event{{Payload: []byte("good")}}
		enc.encodeBatch(events, LSN(i+1), nil)
	}

	data := make([]byte, len(enc.bytes()))
	copy(data, enc.bytes())
	_, off2, _ := decodeBatchFrame(data, 0, nil)
	_, off3, _ := decodeBatchFrame(data, off2, nil)
	data[off3+batchHeaderLen] ^= 0xFF

	all, off, err := decodeAllBatches(data, nil)
	if err != ErrCRCMismatch {
		t.Fatalf("got err=%v, want ErrCRCMismatch", err)
	}
	if len(all) != 2 {
		t.Fatalf("got %d events, want 2", len(all))
	}
	if off != off3 {
		t.Fatalf("offset=%d, want %d", off, off3)
	}
}

func TestBatchFrameSize(t *testing.T) {
	events := []Event{
		{Payload: []byte("abc"), Meta: []byte("m")},
		{Payload: []byte("de")},
	}
	got := batchFrameSize(events)
	want := batchOverhead + (4 + 2 + 1 + 3) + (4 + 2 + 0 + 2)
	if got != want {
		t.Fatalf("batchFrameSize=%d, want %d", got, want)
	}
}

func TestEncoderReset(t *testing.T) {
	enc := newEncoder(256)
	enc.encodeBatch([]Event{{Payload: []byte("data")}}, 1, nil)
	enc.reset()
	if len(enc.bytes()) != 0 {
		t.Fatalf("expected empty after reset, got %d bytes", len(enc.bytes()))
	}
}

func TestEncoderWriteTo(t *testing.T) {
	enc := newEncoder(256)
	enc.encodeBatch([]Event{{Payload: []byte("hello")}}, 1, nil)

	var buf bytes.Buffer
	n, err := enc.writeTo(&buf)
	if err != nil {
		t.Fatalf("writeTo: %v", err)
	}
	if n != len(enc.bytes()) {
		t.Fatalf("writeTo n=%d, want %d", n, len(enc.bytes()))
	}
}

func TestBatchFrameZeroCopy(t *testing.T) {
	events := []Event{{Payload: []byte("zero-copy")}}
	buf := make([]byte, batchFrameSize(events)*2)
	frame, n, _ := encodeBatchFrame(buf, events, 1, nil)

	decoded, _, err := decodeBatchFrame(frame[:n], 0, nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	payloadAddr := &decoded[0].Payload[0]
	if payloadAddr == &events[0].Payload[0] {
		t.Fatal("payload should not reference input events, but the encoded buffer")
	}
}

func TestBatchFrameMetaOnlyEvent(t *testing.T) {
	events := []Event{{Meta: []byte("meta-only"), Payload: nil}}
	buf := make([]byte, batchFrameSize(events)*2)
	frame, _, err := encodeBatchFrame(buf, events, 1, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, _, err := decodeBatchFrame(frame, 0, nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !bytes.Equal(decoded[0].Meta, []byte("meta-only")) {
		t.Fatalf("Meta=%q, want %q", decoded[0].Meta, "meta-only")
	}
}

func TestScanBatchHeader(t *testing.T) {
	events := []Event{
		{Payload: []byte("a")},
		{Payload: []byte("b")},
		{Payload: []byte("c")},
	}
	buf := make([]byte, batchFrameSize(events)*2)
	frame, n, err := encodeBatchFrame(buf, events, 10, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	firstLSN, count, next, err := scanBatchHeader(frame[:n], 0)
	if err != nil {
		t.Fatalf("scanBatchHeader: %v", err)
	}
	if firstLSN != 10 {
		t.Fatalf("firstLSN=%d, want 10", firstLSN)
	}
	if count != 3 {
		t.Fatalf("count=%d, want 3", count)
	}
	if next != n {
		t.Fatalf("next=%d, want %d", next, n)
	}
}

func TestScanBatchHeaderMultiple(t *testing.T) {
	enc := newEncoder(512)
	enc.encodeBatch([]Event{{Payload: []byte("x")}}, 1, nil)
	enc.encodeBatch([]Event{{Payload: []byte("y")}, {Payload: []byte("z")}}, 2, nil)

	data := enc.bytes()
	firstLSN1, count1, next1, err := scanBatchHeader(data, 0)
	if err != nil {
		t.Fatalf("scan batch 1: %v", err)
	}
	if firstLSN1 != 1 || count1 != 1 {
		t.Fatalf("batch 1: firstLSN=%d, count=%d", firstLSN1, count1)
	}

	firstLSN2, count2, _, err := scanBatchHeader(data, next1)
	if err != nil {
		t.Fatalf("scan batch 2: %v", err)
	}
	if firstLSN2 != 2 || count2 != 2 {
		t.Fatalf("batch 2: firstLSN=%d, count=%d", firstLSN2, count2)
	}
}

func TestScanBatchHeaderCorrupted(t *testing.T) {
	events := []Event{{Payload: []byte("data")}}
	buf := make([]byte, batchFrameSize(events)*2)
	frame, n, _ := encodeBatchFrame(buf, events, 1, nil)

	frame[batchHeaderLen] ^= 0xFF
	_, _, _, err := scanBatchHeader(frame[:n], 0)
	if err != ErrCRCMismatch {
		t.Fatalf("got %v, want ErrCRCMismatch", err)
	}
}

func TestScanBatchHeaderTruncated(t *testing.T) {
	_, _, _, err := scanBatchHeader([]byte{0x45, 0x57}, 0)
	if err != ErrInvalidRecord {
		t.Fatalf("got %v, want ErrInvalidRecord", err)
	}
}

func TestDecodeBatchFrameIntoReuse(t *testing.T) {
	enc := newEncoder(512)
	enc.encodeBatch([]Event{{Payload: []byte("a")}, {Payload: []byte("b")}}, 1, nil)
	enc.encodeBatch([]Event{{Payload: []byte("c")}}, 3, nil)

	data := enc.bytes()
	buf := make([]Event, 0, 16)

	events1, next1, err := decodeBatchFrameInto(data, 0, nil, buf)
	if err != nil {
		t.Fatalf("decode batch 1: %v", err)
	}
	if len(events1) != 2 {
		t.Fatalf("batch 1: got %d events, want 2", len(events1))
	}

	events2, _, err := decodeBatchFrameInto(data, next1, nil, events1[:0])
	if err != nil {
		t.Fatalf("decode batch 2: %v", err)
	}
	if len(events2) != 1 || events2[0].LSN != 3 {
		t.Fatalf("batch 2: got %d events, LSN=%d", len(events2), events2[0].LSN)
	}

	if cap(events2) != cap(events1) {
		t.Fatal("buffer should have been reused (same capacity)")
	}
}

func TestBatchFrameWithCompressor(t *testing.T) {
	comp := &testCompressor{}
	events := []Event{
		{Payload: []byte("compress me"), Meta: []byte("m")},
		{Payload: []byte("and me too")},
	}

	buf := make([]byte, batchFrameSize(events)*4)
	frame, n, err := encodeBatchFrame(buf, events, 1, comp)
	if err != nil {
		t.Fatalf("encode with compressor: %v", err)
	}

	decoded, _, err := decodeBatchFrame(frame[:n], 0, comp)
	if err != nil {
		t.Fatalf("decode with compressor: %v", err)
	}
	if len(decoded) != 2 {
		t.Fatalf("got %d events, want 2", len(decoded))
	}
	if !bytes.Equal(decoded[0].Payload, []byte("compress me")) {
		t.Fatalf("Payload[0]=%q", decoded[0].Payload)
	}
	if !bytes.Equal(decoded[0].Meta, []byte("m")) {
		t.Fatalf("Meta[0]=%q", decoded[0].Meta)
	}
}

func TestDecodeBatchCompressedWithoutDecompressor(t *testing.T) {
	comp := &testCompressor{}
	events := []Event{{Payload: []byte("data")}}
	buf := make([]byte, batchFrameSize(events)*4)
	frame, n, _ := encodeBatchFrame(buf, events, 1, comp)

	_, _, err := decodeBatchFrame(frame[:n], 0, nil)
	if err != ErrCompressorRequired {
		t.Fatalf("got err=%v, want ErrCompressorRequired", err)
	}
}

// testCompressor is a trivial compressor for testing.
// It prepends a 1-byte marker and returns the data unchanged.
type testCompressor struct{}

func (c *testCompressor) Compress(src []byte) ([]byte, error) {
	out := make([]byte, len(src)+1)
	out[0] = 0xCC
	copy(out[1:], src)
	return out, nil
}

func (c *testCompressor) Decompress(src []byte) ([]byte, error) {
	if len(src) == 0 || src[0] != 0xCC {
		return nil, ErrInvalidRecord
	}
	out := make([]byte, len(src)-1)
	copy(out, src[1:])
	return out, nil
}

