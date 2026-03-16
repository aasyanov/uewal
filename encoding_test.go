package uewal

import (
	"encoding/binary"
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

func TestScanBatchFrame(t *testing.T) {
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

func TestHeaderSize(t *testing.T) {
	if batchHeaderLen != 28 {
		t.Fatalf("header size: %d, expected 28", batchHeaderLen)
	}
	if batchOverhead != 32 {
		t.Fatalf("overhead: %d, expected 32", batchOverhead)
	}
}
