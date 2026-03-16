package uewal

import (
	"context"
	"testing"
	"time"
)

func TestWAL_OpenAndAppend(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	lsn, err := writeOne(w, []byte("hello"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if lsn != 1 {
		t.Fatalf("lsn: %d", lsn)
	}

	lsn, err = writeOne(w, []byte("world"), []byte("k1"), []byte("m1"))
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

func TestWAL_AppendBatch(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	batch := NewBatch(3)
	batch.Append([]byte("a"), nil, nil)
	batch.Append([]byte("b"), []byte("key-b"), nil)
	batch.Append([]byte("c"), nil, []byte("meta-c"))

	lsn, err := w.Write(batch)
	if err != nil {
		t.Fatal(err)
	}
	if lsn != 3 {
		t.Fatalf("lsn: %d", lsn)
	}
}

func TestWAL_Replay_AllEvents(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	writeOne(w, []byte("first"), []byte("k1"), nil)
	writeOne(w, []byte("second"), nil, []byte("m2"))
	writeOne(w, []byte("third"), nil, nil)

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

func TestWAL_Replay_FromLSN(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		writeOne(w, []byte("data"), nil, nil)
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

func TestWAL_Iterator_Basic(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	writeOne(w, []byte("a"), nil, nil)
	writeOne(w, []byte("b"), nil, nil)
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

func TestWAL_Recovery_Basic(t *testing.T) {
	dir := t.TempDir()

	w1, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	writeOne(w1, []byte("before-crash"), nil, nil)
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

	lsn, _ := writeOne(w2, []byte("after-recovery"), nil, nil)
	if lsn != 2 {
		t.Fatalf("LSN after recovery: %d", lsn)
	}

	w2.Shutdown(context.Background())
}

func TestWAL_Open_DirectoryLock(t *testing.T) {
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

func TestWAL_Hooks_Basic(t *testing.T) {
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

	writeOne(w, []byte("x"), nil, nil)
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

func TestWAL_Stats_Basic(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	writeOne(w, []byte("a"), nil, nil)
	writeOne(w, []byte("b"), nil, nil)
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

func TestWAL_Write_WithTimestamp(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	ts := int64(1234567890)
	writeOne(w, []byte("data"), nil, nil, WithTimestamp(ts))
	w.Flush()

	w.Replay(0, func(ev Event) error {
		if ev.Timestamp != ts {
			t.Fatalf("timestamp: %d, expected %d", ev.Timestamp, ts)
		}
		return nil
	})

	w.Shutdown(context.Background())
}

func TestWAL_WriteUnsafe_Basic(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	batch := NewBatch(2)
	batch.AppendUnsafe([]byte("fast1"), nil, nil)
	batch.AppendUnsafe([]byte("fast2"), []byte("k"), nil)

	lsn, err := w.WriteUnsafe(batch)
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

func TestWAL_Open_WithStartLSN(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, WithStartLSN(100))
	if err != nil {
		t.Fatal(err)
	}

	lsn, err := writeOne(w, []byte("data"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if lsn != 100 {
		t.Fatalf("expected LSN 100 with StartLSN, got %d", lsn)
	}

	w.Shutdown(context.Background())
}

func TestWAL_Write_EmptyBatch(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	batch := NewBatch(0)
	_, err = w.Write(batch)
	if err != ErrEmptyBatch {
		t.Fatalf("expected ErrEmptyBatch, got %v", err)
	}
}

func TestWAL_Indexer_Basic(t *testing.T) {
	dir := t.TempDir()

	var infos []IndexInfo
	idx := &testIndexer{onAppend: func(info IndexInfo) {
		infos = append(infos, info)
	}}

	w, err := Open(dir, WithIndex(idx))
	if err != nil {
		t.Fatal(err)
	}

	writeOne(w, []byte("data"), []byte("k1"), []byte("m1"))
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

func TestWAL_Write_BatchTooLarge(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, WithMaxBatchSize(64))
	if err != nil {
		t.Fatal(err)
	}

	batch := NewBatch(10)
	for i := 0; i < 10; i++ {
		batch.Append(make([]byte, 100), nil, nil)
	}
	_, err = w.Write(batch)
	if err != ErrBatchTooLarge {
		t.Fatalf("expected ErrBatchTooLarge, got %v", err)
	}

	_, err = writeOne(w, []byte("small"), nil, nil)
	if err != nil {
		t.Fatalf("small write should succeed: %v", err)
	}

	w.Shutdown(context.Background())
}

func TestWAL_Follow_UnblocksOnShutdown(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	it, err := w.Follow(0)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		for it.Next() {
		}
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	w.Shutdown(context.Background())

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Follow did not unblock after Shutdown")
	}
}

func TestWAL_Follow_UnblocksOnClose(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	it, err := w.Follow(0)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		for it.Next() {
		}
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	w.Close()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Follow did not unblock after Close")
	}
}

func TestWAL_ReplayRange_Basic(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		writeOne(w, []byte("data"), nil, nil)
	}
	w.Flush()

	count := 0
	var lsns []LSN
	w.ReplayRange(3, 7, func(ev Event) error {
		lsns = append(lsns, ev.LSN)
		count++
		return nil
	})
	if count != 5 {
		t.Fatalf("ReplayRange(3,7): %d events, want 5", count)
	}
	if lsns[0] != 3 || lsns[len(lsns)-1] != 7 {
		t.Fatalf("LSN range: %v", lsns)
	}

	w.Shutdown(context.Background())
}

func TestWAL_ReplayRange_InvalidRange(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	err = w.ReplayRange(5, 3, func(ev Event) error { return nil })
	if err != ErrLSNOutOfRange {
		t.Fatalf("expected ErrLSNOutOfRange, got %v", err)
	}
}

func TestWAL_ReplayBatches_Basic(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	batch := NewBatch(3)
	batch.Append([]byte("a"), nil, nil)
	batch.Append([]byte("b"), nil, nil)
	batch.Append([]byte("c"), nil, nil)
	w.Write(batch)

	writeOne(w, []byte("single"), nil, nil)
	w.Flush()

	batchCount := 0
	totalEvents := 0
	w.ReplayBatches(0, func(events []Event) error {
		batchCount++
		totalEvents += len(events)
		return nil
	})

	if totalEvents != 4 {
		t.Fatalf("total events: %d, want 4", totalEvents)
	}
	if batchCount != 2 {
		t.Fatalf("batches: %d, want 2", batchCount)
	}

	w.Shutdown(context.Background())
}

func TestWAL_DeleteBefore_Basic(t *testing.T) {
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

	segsBefore := w.Segments()
	if len(segsBefore) < 3 {
		t.Fatalf("expected >= 3 segments, got %d", len(segsBefore))
	}

	w.DeleteBefore(15)

	segsAfter := w.Segments()
	if len(segsAfter) >= len(segsBefore) {
		t.Fatalf("DeleteBefore should have removed segments: before=%d, after=%d",
			len(segsBefore), len(segsAfter))
	}

	for _, s := range segsAfter {
		if s.Sealed && s.LastLSN < 15 {
			t.Fatalf("segment with LastLSN %d should have been deleted", s.LastLSN)
		}
	}

	w.Shutdown(context.Background())
}

func TestWAL_ReplayRange_SingleEvent(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		writeOne(w, []byte("data"), nil, nil)
	}
	w.Flush()

	count := 0
	w.ReplayRange(3, 3, func(ev Event) error {
		if ev.LSN != 3 {
			t.Fatalf("expected LSN 3, got %d", ev.LSN)
		}
		count++
		return nil
	})
	if count != 1 {
		t.Fatalf("ReplayRange(3,3) returned %d events, want 1", count)
	}
	w.Shutdown(context.Background())
}

func TestWAL_Write_AfterClose(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	_, err = writeOne(w, []byte("data"), nil, nil)
	if err == nil {
		t.Fatal("expected error after Close")
	}
}

func TestWAL_Write_AfterShutdown(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.Shutdown(context.Background())

	_, err = writeOne(w, []byte("data"), nil, nil)
	if err == nil {
		t.Fatal("expected error after Shutdown")
	}
}

func TestWAL_WaitDurable_Basic(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	writeOne(w, []byte("data"), nil, nil)
	w.Flush()

	done := make(chan struct{})
	go func() {
		w.WaitDurable(1)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("WaitDurable blocked for too long")
	}
	w.Shutdown(context.Background())
}

func TestBatch_Reset_ClearsReferences(t *testing.T) {
	b := NewBatch(3)
	b.Append([]byte("big-payload-1"), []byte("key"), nil)
	b.Append([]byte("big-payload-2"), nil, []byte("meta"))
	b.Reset()

	if b.Len() != 0 {
		t.Fatalf("after Reset, Len()=%d, want 0", b.Len())
	}
	if b.noCompress {
		t.Fatal("noCompress should be false after Reset")
	}
}

type testIndexer struct {
	onAppend func(info IndexInfo)
}

func (ti *testIndexer) OnAppend(info IndexInfo) {
	if ti.onAppend != nil {
		ti.onAppend(info)
	}
}

func TestWAL_Backpressure_DropMode(t *testing.T) {
	dir := t.TempDir()

	var dropCount int
	w, err := Open(dir,
		WithBackpressure(DropMode),
		WithQueueSize(2),
		WithHooks(Hooks{
			OnDrop: func(count int) { dropCount += count },
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		lsn, err := writeOne(w, []byte("data"), nil, nil)
		if err != nil {
			t.Fatalf("DropMode should never return error, got %v", err)
		}
		if lsn == 0 {
			t.Fatal("LSN should never be 0 in DropMode")
		}
	}

	w.Flush()
	time.Sleep(50 * time.Millisecond)

	stats := w.Stats()
	total := stats.EventsWritten + stats.Drops
	if total != 100 {
		t.Fatalf("written(%d) + dropped(%d) = %d, want 100",
			stats.EventsWritten, stats.Drops, total)
	}

	w.Shutdown(context.Background())
}

func TestWAL_Backpressure_ErrorMode(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir,
		WithBackpressure(ErrorMode),
		WithQueueSize(2),
	)
	if err != nil {
		t.Fatal(err)
	}

	var queueFullSeen bool
	for i := 0; i < 100; i++ {
		_, err := writeOne(w, []byte("data"), nil, nil)
		if err == ErrQueueFull {
			queueFullSeen = true
			break
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if !queueFullSeen {
		t.Fatal("ErrorMode should return ErrQueueFull when queue is full")
	}

	w.Shutdown(context.Background())
}

func TestWAL_Write_EmptyPayload(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	lsn, err := writeOne(w, []byte{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if lsn != 1 {
		t.Fatalf("lsn: %d", lsn)
	}

	w.Flush()
	count := 0
	w.Replay(0, func(ev Event) error {
		if len(ev.Payload) != 0 {
			t.Fatalf("expected empty payload, got %d bytes", len(ev.Payload))
		}
		count++
		return nil
	})
	if count != 1 {
		t.Fatalf("expected 1 event, got %d", count)
	}

	w.Shutdown(context.Background())
}

func TestWAL_Write_NilPayload(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	lsn, err := writeOne(w, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if lsn != 1 {
		t.Fatalf("lsn: %d", lsn)
	}

	w.Flush()
	count := 0
	w.Replay(0, func(ev Event) error {
		count++
		return nil
	})
	if count != 1 {
		t.Fatalf("expected 1 event, got %d", count)
	}

	w.Shutdown(context.Background())
}

func TestWAL_Rotate_Manual(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	writeOne(w, []byte("before"), nil, nil)
	w.Flush()

	if err := w.Rotate(); err != nil {
		t.Fatal(err)
	}

	writeOne(w, []byte("after"), nil, nil)
	w.Flush()

	segs := w.Segments()
	if len(segs) < 2 {
		t.Fatalf("expected >= 2 segments after Rotate, got %d", len(segs))
	}

	count := 0
	w.Replay(0, func(ev Event) error {
		count++
		return nil
	})
	if count != 2 {
		t.Fatalf("replay after rotate: %d events, want 2", count)
	}

	w.Shutdown(context.Background())
}

func TestWAL_Follow_SeesNewData(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	it, err := w.Follow(0)
	if err != nil {
		t.Fatal(err)
	}

	writeOne(w, []byte("ev1"), nil, nil)
	writeOne(w, []byte("ev2"), nil, nil)
	w.Flush()

	done := make(chan []Event)
	go func() {
		var events []Event
		for it.Next() {
			ev := it.Event()
			events = append(events, Event{
				LSN:     ev.LSN,
				Payload: append([]byte{}, ev.Payload...),
			})
			if len(events) == 2 {
				break
			}
		}
		done <- events
	}()

	select {
	case events := <-done:
		if len(events) != 2 {
			t.Fatalf("expected 2 events, got %d", len(events))
		}
		if events[0].LSN != 1 || events[1].LSN != 2 {
			t.Fatalf("LSNs: %d, %d", events[0].LSN, events[1].LSN)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Follow timed out")
	}

	it.Close()
	w.Shutdown(context.Background())
}

func TestWAL_Segments_Basic(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	writeOne(w, []byte("a"), nil, nil)
	w.Flush()

	segs := w.Segments()
	if len(segs) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(segs))
	}
	if segs[0].FirstLSN != 1 {
		t.Fatalf("FirstLSN: %d", segs[0].FirstLSN)
	}

	w.Shutdown(context.Background())
}

func TestWAL_Shutdown_ContextCancel(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = w.Shutdown(ctx)
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	w.Shutdown(context.Background())
}

func TestWAL_Recovery_LSNContinuity(t *testing.T) {
	dir := t.TempDir()
	w1, _ := Open(dir)
	for i := 0; i < 10; i++ {
		writeOne(w1, []byte("data"), nil, nil)
	}
	w1.Flush()
	w1.Sync()
	w1.Shutdown(context.Background())

	w2, _ := Open(dir)
	lsn, _ := writeOne(w2, []byte("after"), nil, nil)
	if lsn != 11 {
		t.Fatalf("LSN after recovery: %d, want 11", lsn)
	}
	w2.Shutdown(context.Background())
}
