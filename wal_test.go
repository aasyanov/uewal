package uewal

import (
	"context"
	"testing"
	"time"
)

func TestOpenAndAppend(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	lsn, err := w.Append([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if lsn != 1 {
		t.Fatalf("lsn: %d", lsn)
	}

	lsn, err = w.Append([]byte("world"), WithKey([]byte("k1")), WithMeta([]byte("m1")))
	if err != nil {
		t.Fatal(err)
	}
	if lsn != 2 {
		t.Fatalf("lsn: %d", lsn)
	}

	if err := w.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAppendBatch(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	batch := NewBatch(3)
	batch.Append([]byte("a"))
	batch.Append([]byte("b"), WithKey([]byte("key-b")))
	batch.Append([]byte("c"), WithMeta([]byte("meta-c")))

	lsn, err := w.AppendBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if lsn != 3 {
		t.Fatalf("lsn: %d", lsn)
	}
}

func TestReplay(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	w.Append([]byte("first"), WithKey([]byte("k1")))
	w.Append([]byte("second"), WithMeta([]byte("m2")))
	w.Append([]byte("third"))

	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	count := 0
	err = w.Replay(0, func(ev Event) error {
		switch count {
		case 0:
			if string(ev.Payload) != "first" {
				t.Fatalf("event 0 payload: %q", ev.Payload)
			}
			if string(ev.Key) != "k1" {
				t.Fatalf("event 0 key: %q", ev.Key)
			}
			if ev.Timestamp == 0 {
				t.Fatal("event 0 timestamp should be auto-filled")
			}
		case 1:
			if string(ev.Payload) != "second" {
				t.Fatalf("event 1 payload: %q", ev.Payload)
			}
			if string(ev.Meta) != "m2" {
				t.Fatalf("event 1 meta: %q", ev.Meta)
			}
		case 2:
			if string(ev.Payload) != "third" {
				t.Fatalf("event 2 payload: %q", ev.Payload)
			}
		}
		count++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("events: %d", count)
	}

	if err := w.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestReplayFrom(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		w.Append([]byte("data"))
	}
	w.Flush()

	count := 0
	var firstLSN LSN
	w.Replay(3, func(ev Event) error {
		if count == 0 {
			firstLSN = ev.LSN
		}
		count++
		return nil
	})
	if count != 3 {
		t.Fatalf("expected 3 events from LSN 3, got %d", count)
	}
	if firstLSN != 3 {
		t.Fatalf("first LSN: %d", firstLSN)
	}

	w.Shutdown(context.Background())
}

func TestIterator(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	w.Append([]byte("a"))
	w.Append([]byte("b"))
	w.Flush()

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
	if count != 2 {
		t.Fatalf("count: %d", count)
	}

	w.Shutdown(context.Background())
}

func TestRecovery(t *testing.T) {
	dir := t.TempDir()

	w1, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	w1.Append([]byte("before-crash"))
	w1.Flush()
	w1.Sync()
	w1.Shutdown(context.Background())

	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	w2.Replay(0, func(ev Event) error {
		if count == 0 {
			if string(ev.Payload) != "before-crash" {
				t.Fatalf("payload: %q", ev.Payload)
			}
			if ev.LSN != 1 {
				t.Fatalf("LSN: %d", ev.LSN)
			}
		}
		count++
		return nil
	})
	if count != 1 {
		t.Fatalf("expected 1 event after recovery, got %d", count)
	}

	lsn, _ := w2.Append([]byte("after-recovery"))
	if lsn != 2 {
		t.Fatalf("LSN after recovery: %d", lsn)
	}

	w2.Shutdown(context.Background())
}

func TestDirectoryLock(t *testing.T) {
	dir := t.TempDir()

	w1, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w1.Close()

	_, err = Open(dir)
	if err != ErrDirectoryLocked {
		t.Fatalf("expected ErrDirectoryLocked, got %v", err)
	}
}

func TestHooks(t *testing.T) {
	dir := t.TempDir()

	var started, shutdownStarted bool
	var appendCount int

	w, err := Open(dir, WithHooks(Hooks{
		OnStart:         func() { started = true },
		OnShutdownStart: func() { shutdownStarted = true },
		AfterAppend:     func(first, last LSN, count int) { appendCount += count },
	}))
	if err != nil {
		t.Fatal(err)
	}

	if !started {
		t.Fatal("OnStart not called")
	}

	w.Append([]byte("x"))
	w.Flush()

	time.Sleep(10 * time.Millisecond)
	if appendCount != 1 {
		t.Fatalf("appendCount: %d", appendCount)
	}

	w.Shutdown(context.Background())
	if !shutdownStarted {
		t.Fatal("OnShutdownStart not called")
	}
}

func TestStats(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	w.Append([]byte("a"))
	w.Append([]byte("b"))
	w.Flush()
	time.Sleep(10 * time.Millisecond)

	s := w.Stats()
	if s.EventsWritten != 2 {
		t.Fatalf("events: %d", s.EventsWritten)
	}
	if s.BatchesWritten != 2 {
		t.Fatalf("batches: %d", s.BatchesWritten)
	}
	if s.State != StateRunning {
		t.Fatalf("state: %v", s.State)
	}
	if s.SegmentCount != 1 {
		t.Fatalf("segments: %d", s.SegmentCount)
	}

	w.Shutdown(context.Background())
}

func TestWithTimestamp(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	ts := int64(1234567890)
	w.Append([]byte("data"), WithTimestamp(ts))
	w.Flush()

	w.Replay(0, func(ev Event) error {
		if ev.Timestamp != ts {
			t.Fatalf("timestamp: %d, expected %d", ev.Timestamp, ts)
		}
		return nil
	})

	w.Shutdown(context.Background())
}

func TestBatchAppendUnsafe(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	batch := NewBatch(2)
	batch.AppendUnsafe([]byte("fast1"))
	batch.AppendUnsafe([]byte("fast2"), WithKey([]byte("k")))

	lsn, err := w.AppendBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if lsn != 2 {
		t.Fatalf("lsn: %d", lsn)
	}

	w.Flush()

	replayCount := 0
	w.Replay(0, func(ev Event) error {
		if ev.LSN == 2 {
			if string(ev.Key) != "k" {
				t.Fatalf("key: %q", ev.Key)
			}
		}
		replayCount++
		return nil
	})
	if replayCount != 2 {
		t.Fatalf("events: %d", replayCount)
	}

	w.Shutdown(context.Background())
}

func TestWithStartLSN(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, WithStartLSN(100))
	if err != nil {
		t.Fatal(err)
	}

	lsn, err := w.Append([]byte("data"))
	if err != nil {
		t.Fatal(err)
	}
	if lsn != 100 {
		t.Fatalf("expected LSN 100 with StartLSN, got %d", lsn)
	}

	w.Shutdown(context.Background())
}

func TestEmptyBatch(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	batch := NewBatch(0)
	_, err = w.AppendBatch(batch)
	if err != ErrEmptyBatch {
		t.Fatalf("expected ErrEmptyBatch, got %v", err)
	}
}

func TestIndexer(t *testing.T) {
	dir := t.TempDir()

	var infos []IndexInfo
	idx := &testIndexer{onAppend: func(info IndexInfo) {
		infos = append(infos, info)
	}}

	w, err := Open(dir, WithIndex(idx))
	if err != nil {
		t.Fatal(err)
	}

	w.Append([]byte("data"), WithKey([]byte("k1")), WithMeta([]byte("m1")))
	w.Flush()
	time.Sleep(10 * time.Millisecond)

	if len(infos) != 1 {
		t.Fatalf("indexer calls: %d", len(infos))
	}
	if infos[0].LSN != 1 {
		t.Fatalf("LSN: %d", infos[0].LSN)
	}
	if string(infos[0].Key) != "k1" {
		t.Fatalf("key: %q", infos[0].Key)
	}
	if string(infos[0].Meta) != "m1" {
		t.Fatalf("meta: %q", infos[0].Meta)
	}

	w.Shutdown(context.Background())
}

type testIndexer struct {
	onAppend func(info IndexInfo)
}

func (ti *testIndexer) OnAppend(info IndexInfo) {
	if ti.onAppend != nil {
		ti.onAppend(info)
	}
}
