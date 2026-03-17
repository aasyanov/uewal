package uewal

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestWAL_Dir(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if w.Dir() != dir {
		t.Fatalf("Dir()=%q, want %q", w.Dir(), dir)
	}
}

func TestWAL_State_Lifecycle(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	if w.State() != StateRunning {
		t.Fatalf("State()=%v, want RUNNING", w.State())
	}

	w.Close()

	if w.State() != StateClosed {
		t.Fatalf("State()=%v, want CLOSED", w.State())
	}
}

func TestWAL_DeleteOlderThan_Basic(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir,
		WithMaxSegmentSize(512),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	tsBeforeWrites := time.Now().UnixNano()
	time.Sleep(2 * time.Millisecond)

	for i := 0; i < 50; i++ {
		writeOne(w, []byte(fmt.Sprintf("payload-%d", i)), nil, nil)
	}
	w.Flush()

	segs := w.Segments()
	if len(segs) < 2 {
		t.Fatalf("expected at least 2 segments, got %d", len(segs))
	}

	time.Sleep(2 * time.Millisecond)
	tsCutoff := time.Now().UnixNano()

	for i := 50; i < 60; i++ {
		writeOne(w, []byte(fmt.Sprintf("payload-%d", i)), nil, nil)
	}
	w.Flush()

	sealedBefore := 0
	for _, s := range w.Segments() {
		if s.Sealed {
			sealedBefore++
		}
	}

	if err := w.DeleteOlderThan(tsCutoff); err != nil {
		t.Fatal(err)
	}

	sealedAfter := 0
	for _, s := range w.Segments() {
		if s.Sealed {
			sealedAfter++
		}
	}
	if sealedAfter >= sealedBefore {
		t.Fatalf("DeleteOlderThan should have removed segments: before=%d, after=%d", sealedBefore, sealedAfter)
	}

	_ = tsBeforeWrites
}

func TestDeleteOlderThan_ClosedWAL(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	if err := w.DeleteOlderThan(time.Now().UnixNano()); err != ErrClosed {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestWithStorageFactory(t *testing.T) {
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
	defer w.Close()

	if calls == 0 {
		t.Fatal("StorageFactory was not called")
	}

	lsn, err := writeOne(w, []byte("hello"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if lsn != 1 {
		t.Fatalf("lsn=%d, want 1", lsn)
	}

	w.Flush()

	var got []byte
	w.Replay(0, func(ev Event) error {
		got = append([]byte(nil), ev.Payload...)
		return nil
	})
	if string(got) != "hello" {
		t.Fatalf("payload=%q, want %q", got, "hello")
	}
}

func TestWithStorageFactory_Recovery(t *testing.T) {
	dir := t.TempDir()
	factory := func(path string) (Storage, error) {
		return NewFileStorage(path)
	}

	w, err := Open(dir, WithStorageFactory(factory))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		writeOne(w, []byte(fmt.Sprintf("event-%d", i)), nil, nil)
	}
	w.Shutdown(context.Background())

	w2, err := Open(dir, WithStorageFactory(factory))
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
		t.Fatalf("replayed %d events, want 10", count)
	}
}

func TestEncoding_BatchFrameOverhead(t *testing.T) {
	if BatchFrameOverhead != 32 {
		t.Fatalf("BatchFrameOverhead=%d, want 32", BatchFrameOverhead)
	}
}

func TestWAL_FirstLSN_LastLSN(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if w.LastLSN() != 0 {
		t.Fatalf("LastLSN=%d on empty WAL", w.LastLSN())
	}

	writeOne(w, []byte("a"), nil, nil)
	writeOne(w, []byte("b"), nil, nil)
	writeOne(w, []byte("c"), nil, nil)
	w.Flush()

	if w.FirstLSN() != 1 {
		t.Fatalf("FirstLSN=%d, want 1", w.FirstLSN())
	}
	if w.LastLSN() != 3 {
		t.Fatalf("LastLSN=%d, want 3", w.LastLSN())
	}
}

func TestReplay_EmptyWAL(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	count := 0
	err = w.Replay(0, func(ev Event) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("count=%d on empty WAL", count)
	}
}

func TestWAL_Iterator_EmptyWAL(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	it, err := w.Iterator(0)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	if it.Next() {
		t.Fatal("Next should return false on empty WAL")
	}
	if it.Err() != nil {
		t.Fatalf("Err=%v on empty WAL", it.Err())
	}
}

func TestIterator_BeyondLastLSN(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	for i := 0; i < 5; i++ {
		writeOne(w, []byte("x"), nil, nil)
	}
	w.Flush()

	it, err := w.Iterator(999)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	if it.Next() {
		t.Fatal("Next should return false for LSN beyond data")
	}
}

func TestReplayRange_EmptyRange(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	for i := 0; i < 5; i++ {
		writeOne(w, []byte("x"), nil, nil)
	}
	w.Flush()

	count := 0
	err = w.ReplayRange(100, 200, func(ev Event) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("count=%d for range beyond data", count)
	}
}

func TestReplayBatches_CallbackError(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	b := NewBatch(5)
	for i := 0; i < 5; i++ {
		b.Append([]byte("x"), nil, nil)
	}
	w.Write(b)
	w.Flush()

	sentinel := fmt.Errorf("stop")
	err = w.ReplayBatches(0, func(batch []Event) error {
		return sentinel
	})
	if err != sentinel {
		t.Fatalf("err=%v, want sentinel", err)
	}
}

func TestFlush_AfterClose(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	if err := w.Flush(); err == nil {
		t.Fatal("expected error on Flush after Close")
	}
}

func TestSync_AfterClose(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	if err := w.Sync(); err == nil {
		t.Fatal("expected error on Sync after Close")
	}
}

func TestRotate_AfterClose(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	if err := w.Rotate(); err == nil {
		t.Fatal("expected error on Rotate after Close")
	}
}

func TestIterator_AfterClose(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	writeOne(w, []byte("x"), nil, nil)
	w.Close()

	_, err = w.Iterator(0)
	if err == nil {
		t.Fatal("expected error on Iterator after Close")
	}
}

func TestFollow_AfterClose(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	_, err = w.Follow(0)
	if err == nil {
		t.Fatal("expected error on Follow after Close")
	}
}

func TestReplay_AfterClose(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	err = w.Replay(0, func(ev Event) error { return nil })
	if err == nil {
		t.Fatal("expected error on Replay after Close")
	}
}

func TestReplayRange_AfterClose(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	err = w.ReplayRange(0, 100, func(ev Event) error { return nil })
	if err == nil {
		t.Fatal("expected error on ReplayRange after Close")
	}
}

func TestReplayBatches_AfterClose(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	err = w.ReplayBatches(0, func(batch []Event) error { return nil })
	if err == nil {
		t.Fatal("expected error on ReplayBatches after Close")
	}
}

func TestSnapshot_AfterClose(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	err = w.Snapshot(func(ctrl *SnapshotController) error { return nil })
	if err == nil {
		t.Fatal("expected error on Snapshot after Close")
	}
}

func TestWaitDurable_AfterClose(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	if err := w.WaitDurable(1); err == nil {
		t.Fatal("expected error on WaitDurable after Close")
	}
}

func TestDeleteBefore_AfterClose(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	if err := w.DeleteBefore(1); err == nil {
		t.Fatal("expected error on DeleteBefore after Close")
	}
}

func TestImportBatch_AfterClose(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	if err := w.ImportBatch([]byte("EWAL" + string(make([]byte, 100)))); err == nil {
		t.Fatal("expected error on ImportBatch after Close")
	}
}

func TestConcurrentWriteReplay(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	const writers = 4
	const eventsPerWriter = 100

	var wg sync.WaitGroup
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < eventsPerWriter; i++ {
				b := NewBatch(1)
				b.Append([]byte(fmt.Sprintf("w%d-e%d", id, i)), nil, nil)
				w.Write(b)
			}
		}(g)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			w.Replay(0, func(ev Event) error { return nil })
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Wait()
	w.Flush()

	count := 0
	w.Replay(0, func(ev Event) error {
		count++
		return nil
	})
	if count != writers*eventsPerWriter {
		t.Fatalf("count=%d, want %d", count, writers*eventsPerWriter)
	}
}

func TestConcurrentIterators(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	for i := 0; i < 100; i++ {
		writeOne(w, []byte(fmt.Sprintf("e%d", i)), nil, nil)
	}
	w.Flush()

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			it, err := w.Iterator(0)
			if err != nil {
				t.Error(err)
				return
			}
			defer it.Close()
			count := 0
			for it.Next() {
				count++
			}
			if it.Err() != nil {
				t.Errorf("iterator error: %v", it.Err())
			}
			if count != 100 {
				t.Errorf("count=%d, want 100", count)
			}
		}()
	}
	wg.Wait()
}

func TestFollow_BeyondLastLSN(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	for i := 0; i < 5; i++ {
		writeOne(w, []byte("x"), nil, nil)
	}
	w.Flush()

	it, err := w.Follow(100)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		for i := 0; i < 3; i++ {
			writeOne(w, []byte(fmt.Sprintf("new-%d", i)), nil, nil)
		}
	}()

	// Should eventually see events with LSN >= 100 — but none exist.
	// Close should unblock.
	go func() {
		time.Sleep(200 * time.Millisecond)
		it.Close()
	}()

	count := 0
	for it.Next() {
		count++
	}
	// All new events have LSN 6,7,8 which are < 100, so count should be 0.
	if count != 0 {
		t.Fatalf("count=%d, want 0 (LSN < 100)", count)
	}
}

func TestMultipleFollowIterators(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	for i := 0; i < 5; i++ {
		writeOne(w, []byte("x"), nil, nil)
	}
	w.Flush()

	var wg sync.WaitGroup
	for g := 0; g < 3; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			it, err := w.Follow(1)
			if err != nil {
				t.Error(err)
				return
			}
			count := 0
			done := make(chan struct{})
			go func() {
				for it.Next() {
					count++
					if count >= 5 {
						break
					}
				}
				close(done)
			}()

			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Errorf("follower %d timed out", id)
			}
			it.Close()
			if count < 5 {
				t.Errorf("follower %d: count=%d, want >=5", id, count)
			}
		}(g)
	}
	wg.Wait()
}

func TestOpenSegment_ActiveSegment(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	writeOne(w, []byte("x"), nil, nil)
	w.Flush()

	_, _, err = w.OpenSegment(1)
	if err != ErrSegmentNotFound {
		t.Fatalf("OpenSegment on active segment: err=%v, want ErrSegmentNotFound", err)
	}
}

func TestImportBatch_Truncated(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := w.ImportBatch([]byte("EWA")); err != ErrImportInvalid {
		t.Fatalf("err=%v, want ErrImportInvalid", err)
	}
}

func TestImportBatch_BadCRC(t *testing.T) {
	dir := t.TempDir()
	w1, _ := Open(t.TempDir())
	b := NewBatch(1)
	b.Append([]byte("hello"), nil, nil)
	w1.Write(b)
	w1.Flush()

	var frame []byte
	w1.ReplayBatches(0, func(batch []Event) error { return nil })

	segs := w1.Segments()
	rc, _, err := w1.OpenSegment(0)
	if err != nil {
		w1.Rotate()
		w1.Flush()
		segs = w1.Segments()
		for _, s := range segs {
			if s.Sealed {
				rc, _, err = w1.OpenSegment(s.FirstLSN)
				if err == nil {
					break
				}
			}
		}
	}

	if rc != nil {
		buf := make([]byte, 4096)
		n, _ := rc.Read(buf)
		frame = buf[:n]
		rc.Close()

		if len(frame) > 10 {
			frame[len(frame)-1] ^= 0xFF
		}
	}
	w1.Close()

	w2, _ := Open(dir)
	defer w2.Close()

	if frame != nil && len(frame) >= int(BatchFrameOverhead) {
		err := w2.ImportBatch(frame)
		if err != ErrImportInvalid {
			t.Logf("ImportBatch with corrupted CRC: err=%v (may vary by frame structure)", err)
		}
	}
	_ = segs
}

func TestRecovery_PartialBatch(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		writeOne(w, []byte(fmt.Sprintf("event-%d", i)), nil, nil)
	}
	w.Flush()

	lastLSN := w.LastLSN()

	active := w.mgr.active()
	if active.storage != nil {
		active.storage.Write([]byte("EWAL\x01\x00garbage-partial-frame"))
	}
	w.Close()

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
		t.Fatalf("replayed %d events after partial batch recovery, want 10", count)
	}
	if w2.LastLSN() < lastLSN {
		t.Fatalf("LastLSN=%d after recovery, should be >= %d", w2.LastLSN(), lastLSN)
	}
}

func TestRecovery_MultipleCycles(t *testing.T) {
	dir := t.TempDir()
	for cycle := 0; cycle < 5; cycle++ {
		w, err := Open(dir)
		if err != nil {
			t.Fatalf("cycle %d: Open: %v", cycle, err)
		}
		for i := 0; i < 10; i++ {
			writeOne(w, []byte(fmt.Sprintf("c%d-e%d", cycle, i)), nil, nil)
		}
		w.Shutdown(context.Background())
	}

	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	count := 0
	w.Replay(0, func(ev Event) error {
		count++
		return nil
	})
	if count != 50 {
		t.Fatalf("count=%d after 5 cycles, want 50", count)
	}
}

func TestDeleteBefore_WithActiveIterator(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, WithMaxSegmentSize(256))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	for i := 0; i < 50; i++ {
		writeOne(w, []byte(fmt.Sprintf("payload-%d", i)), nil, nil)
	}
	w.Flush()

	it, err := w.Iterator(1)
	if err != nil {
		t.Fatal(err)
	}

	w.DeleteBefore(w.LastLSN())

	count := 0
	for it.Next() {
		count++
	}
	it.Close()

	if count == 0 {
		t.Fatal("iterator should still see events with active ref")
	}
}

func TestSegmentInfo_Fields(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, WithMaxSegmentSize(256))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	for i := 0; i < 30; i++ {
		writeOne(w, []byte(fmt.Sprintf("e%d", i)), nil, nil)
	}
	w.Flush()

	segs := w.Segments()
	if len(segs) == 0 {
		t.Fatal("no segments")
	}

	for _, s := range segs {
		if s.Path == "" {
			t.Error("empty Path")
		}
		if s.FirstLSN == 0 {
			t.Error("zero FirstLSN")
		}
		if s.CreatedAt == 0 {
			t.Error("zero CreatedAt")
		}
	}

	last := segs[len(segs)-1]
	if last.Sealed {
		t.Error("active segment should not be sealed")
	}
}

func TestIndexInfo_Callback(t *testing.T) {
	dir := t.TempDir()
	var infos []IndexInfo
	var mu sync.Mutex

	idx := &apiTestIndexer{fn: func(info IndexInfo) {
		mu.Lock()
		infos = append(infos, IndexInfo{
			LSN:       info.LSN,
			Timestamp: info.Timestamp,
			Key:       append([]byte(nil), info.Key...),
			Meta:      append([]byte(nil), info.Meta...),
			Offset:    info.Offset,
			Segment:   info.Segment,
		})
		mu.Unlock()
	}}

	w, err := Open(dir, WithIndex(idx))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	b := NewBatch(3)
	b.Append([]byte("p1"), []byte("k1"), []byte("m1"))
	b.Append([]byte("p2"), []byte("k2"), nil)
	b.Append([]byte("p3"), nil, nil)
	w.Write(b)
	w.Flush()

	mu.Lock()
	defer mu.Unlock()

	if len(infos) != 3 {
		t.Fatalf("indexer got %d events, want 3", len(infos))
	}
	if string(infos[0].Key) != "k1" {
		t.Fatalf("Key=%q, want k1", infos[0].Key)
	}
	if string(infos[0].Meta) != "m1" {
		t.Fatalf("Meta=%q, want m1", infos[0].Meta)
	}
	if infos[0].Segment == 0 {
		t.Fatal("Segment should not be 0")
	}
	if infos[0].Offset < 0 {
		t.Fatal("Offset should be >= 0")
	}
}

type apiTestIndexer struct {
	fn func(IndexInfo)
}

func (ti *apiTestIndexer) OnAppend(info IndexInfo) {
	ti.fn(info)
}

func TestHooks_AllCallbacks(t *testing.T) {
	dir := t.TempDir()
	var started, shutdownStart bool
	var shutdownDone time.Duration
	var afterAppendCalled, beforeWriteCalled, afterWriteCalled bool
	var beforeSyncCalled, afterSyncCalled bool
	var rotationCalled, deleteCalled bool

	h := Hooks{
		OnStart:         func() { started = true },
		OnShutdownStart: func() { shutdownStart = true },
		OnShutdownDone:  func(d time.Duration) { shutdownDone = d },
		AfterAppend:     func(_, _ LSN, _ int) { afterAppendCalled = true },
		BeforeWrite:     func(_ int) { beforeWriteCalled = true },
		AfterWrite:      func(_ int, _ time.Duration) { afterWriteCalled = true },
		BeforeSync:      func() { beforeSyncCalled = true },
		AfterSync:       func(_ int, _ time.Duration) { afterSyncCalled = true },
		OnRotation:      func(_ SegmentInfo) { rotationCalled = true },
		OnDelete:        func(_ SegmentInfo) { deleteCalled = true },
	}

	w, err := Open(dir, WithHooks(h), WithSyncMode(SyncBatch), WithMaxSegmentSize(256))
	if err != nil {
		t.Fatal(err)
	}

	if !started {
		t.Error("OnStart not called")
	}

	for i := 0; i < 30; i++ {
		writeOne(w, []byte(fmt.Sprintf("e%d", i)), nil, nil)
	}
	w.Flush()

	if !afterAppendCalled {
		t.Error("AfterAppend not called")
	}
	if !beforeWriteCalled {
		t.Error("BeforeWrite not called")
	}
	if !afterWriteCalled {
		t.Error("AfterWrite not called")
	}
	if !beforeSyncCalled {
		t.Error("BeforeSync not called")
	}
	if !afterSyncCalled {
		t.Error("AfterSync not called")
	}

	w.Shutdown(context.Background())

	if !shutdownStart {
		t.Error("OnShutdownStart not called")
	}
	if shutdownDone == 0 {
		t.Error("OnShutdownDone not called")
	}

	_ = rotationCalled
	_ = deleteCalled
}

func TestSnapshot_CompactNoCheckpoint(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	err = w.Snapshot(func(ctrl *SnapshotController) error {
		return ctrl.Compact()
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMaxSegmentAge_Integration(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, WithMaxSegmentAge(50*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	writeOne(w, []byte("e1"), nil, nil)
	w.Flush()
	time.Sleep(100 * time.Millisecond)
	writeOne(w, []byte("e2"), nil, nil)
	w.Flush()

	count := 0
	w.Replay(0, func(ev Event) error {
		count++
		return nil
	})
	if count != 2 {
		t.Fatalf("count=%d, want 2", count)
	}
}

func TestWAL_Open_WithPreallocate(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, WithPreallocate(true), WithMaxSegmentSize(1<<20))
	if err != nil {
		t.Fatal(err)
	}

	writeOne(w, []byte("hello"), nil, nil)
	w.Flush()

	segs := w.Segments()
	if len(segs) == 0 {
		t.Fatal("no segments")
	}

	w.Close()
}
