package uewal

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestReplayEmpty(t *testing.T) {
	w := openTestWAL(t)
	defer w.Shutdown(context.Background())

	var count int
	err := w.Replay(0, func(Event) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if count != 0 {
		t.Fatalf("count=%d, want 0", count)
	}
}

func TestReplayAfterAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replay.wal")
	w, err := Open(path, WithSyncMode(SyncBatch))
	if err != nil {
		t.Fatal(err)
	}

	payloads := []string{"alpha", "beta", "gamma"}
	for _, p := range payloads {
		w.Append(Event{Payload: []byte(p)})
	}
	w.Shutdown(context.Background())

	w2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	var events []Event
	err = w2.Replay(0, func(e Event) error {
		cp := make([]byte, len(e.Payload))
		copy(cp, e.Payload)
		events = append(events, Event{LSN: e.LSN, Payload: cp})
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("got %d events, want 3", len(events))
	}
	for i, ev := range events {
		if string(ev.Payload) != payloads[i] {
			t.Errorf("event[%d]=%q, want %q", i, ev.Payload, payloads[i])
		}
		if ev.LSN != LSN(i+1) {
			t.Errorf("event[%d].LSN=%d, want %d", i, ev.LSN, i+1)
		}
	}

	w2.Shutdown(context.Background())
}

func TestReplayFromLSN(t *testing.T) {
	path := filepath.Join(t.TempDir(), "from_lsn.wal")
	w, err := Open(path, WithSyncMode(SyncBatch))
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		w.Append(Event{Payload: []byte("event")})
	}
	w.Shutdown(context.Background())

	w2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	var events []Event
	err = w2.Replay(6, func(e Event) error {
		events = append(events, e)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(events) != 5 {
		t.Fatalf("got %d events (from LSN 6), want 5", len(events))
	}
	if events[0].LSN != 6 {
		t.Fatalf("first event LSN=%d, want 6", events[0].LSN)
	}

	w2.Shutdown(context.Background())
}

func TestReplayCorruptionTruncation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt.wal")
	s, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}

	enc := newEncoder(256)
	enc.encodeBatch([]Event{{Payload: []byte("good-1")}}, 1, nil)
	enc.encodeBatch([]Event{{Payload: []byte("good-2")}}, 2, nil)
	enc.encodeBatch([]Event{{Payload: []byte("good-3")}}, 3, nil)
	s.Write(enc.bytes())
	s.Sync()

	s.Write([]byte{0xFF, 0xFE, 0xFD, 0xFC, 0xFB})
	s.Sync()
	s.Close()

	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	var events []Event
	w.Replay(0, func(e Event) error {
		events = append(events, e)
		return nil
	})

	if len(events) != 3 {
		t.Fatalf("got %d events, want 3", len(events))
	}
	if w.LastLSN() != 3 {
		t.Fatalf("LastLSN=%d, want 3", w.LastLSN())
	}

	w.Shutdown(context.Background())
}

func TestIteratorBasic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "iter.wal")
	w, err := Open(path, WithSyncMode(SyncBatch))
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		w.Append(Event{Payload: []byte("iter")})
	}
	w.Shutdown(context.Background())

	w2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	it, err := w2.Iterator(0)
	if err != nil {
		w2.Close()
		t.Fatalf("Iterator: %v", err)
	}
	defer it.Close()

	count := 0
	for it.Next() {
		count++
		ev := it.Event()
		if ev.LSN != LSN(count) {
			t.Errorf("event LSN=%d, want %d", ev.LSN, count)
		}
	}
	if it.Err() != nil {
		t.Fatalf("Iterator error: %v", it.Err())
	}
	if count != 5 {
		t.Fatalf("iterated %d events, want 5", count)
	}

	w2.Shutdown(context.Background())
}

func TestIteratorFromLSN(t *testing.T) {
	path := filepath.Join(t.TempDir(), "iter_lsn.wal")
	w, err := Open(path, WithSyncMode(SyncBatch))
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		w.Append(Event{Payload: []byte("iter")})
	}
	w.Shutdown(context.Background())

	w2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	it, err := w2.Iterator(8)
	if err != nil {
		w2.Close()
		t.Fatalf("Iterator: %v", err)
	}
	defer it.Close()

	count := 0
	for it.Next() {
		count++
	}
	if count != 3 {
		t.Fatalf("iterated %d events from LSN 8, want 3", count)
	}

	w2.Shutdown(context.Background())
}

func TestIteratorEmpty(t *testing.T) {
	w := openTestWAL(t)
	defer w.Shutdown(context.Background())

	it, err := w.Iterator(0)
	if err != nil {
		t.Fatalf("Iterator: %v", err)
	}
	defer it.Close()

	if it.Next() {
		t.Fatal("expected no events in empty WAL")
	}
}

func TestReplayPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "persist.wal")

	w1, err := Open(path, WithSyncMode(SyncBatch))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		w1.Append(Event{Payload: []byte("persistent")})
	}
	w1.Shutdown(context.Background())

	w2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	var count int
	w2.Replay(0, func(Event) error {
		count++
		return nil
	})

	if count != 10 {
		t.Fatalf("replayed %d events, want 10", count)
	}
	if w2.LastLSN() != 10 {
		t.Fatalf("recovered LastLSN=%d, want 10", w2.LastLSN())
	}

	lsn, _ := w2.Append(Event{Payload: []byte("new")})
	if lsn != 11 {
		t.Fatalf("new LSN=%d, want 11", lsn)
	}

	w2.Shutdown(context.Background())
}

func TestReplayCallbackErrorStopsIteration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cb_err.wal")
	w, err := Open(path, WithSyncMode(SyncBatch))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		w.Append(Event{Payload: []byte("event")})
	}
	w.Shutdown(context.Background())

	w2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Shutdown(context.Background())

	sentinel := errors.New("stop here")
	var count int
	err = w2.Replay(0, func(e Event) error {
		count++
		if count == 3 {
			return sentinel
		}
		return nil
	})
	if err != sentinel {
		t.Fatalf("Replay error: got %v, want sentinel", err)
	}
	if count != 3 {
		t.Fatalf("count=%d, want 3", count)
	}
}

func TestReplayCorruptionStatsAndHooks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corr_hooks.wal")
	s, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}

	enc := newEncoder(256)
	enc.encodeBatch([]Event{{Payload: []byte("valid")}}, 1, nil)
	validSize := len(enc.bytes())
	s.Write(enc.bytes())
	s.Write([]byte{0xFF, 0xFE, 0xFD})
	s.Sync()
	s.Close()

	var corruptionOffset int64 = -1
	w, err := Open(path, WithHooks(Hooks{
		OnCorruption: func(off int64) { corruptionOffset = off },
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Shutdown(context.Background())

	if corruptionOffset != int64(validSize) {
		t.Fatalf("OnCorruption offset=%d, want %d", corruptionOffset, validSize)
	}

	s2 := w.Stats()
	if s2.Corruptions != 1 {
		t.Fatalf("Corruptions=%d, want 1", s2.Corruptions)
	}
}

func TestIteratorCloseIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "iter_close.wal")
	w, err := Open(path, WithSyncMode(SyncBatch))
	if err != nil {
		t.Fatal(err)
	}
	w.Append(Event{Payload: []byte("x")})
	w.Shutdown(context.Background())

	w2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Shutdown(context.Background())

	it, err := w2.Iterator(0)
	if err != nil {
		t.Fatal(err)
	}

	if err := it.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestIteratorOnEmptyNilClose(t *testing.T) {
	w := openTestWAL(t)
	defer w.Shutdown(context.Background())

	it, err := w.Iterator(0)
	if err != nil {
		t.Fatal(err)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Close on empty iterator: %v", err)
	}
}

func TestReplayLSNBeyondData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "beyond.wal")
	w, err := Open(path, WithSyncMode(SyncBatch))
	if err != nil {
		t.Fatal(err)
	}
	w.Append(Event{Payload: []byte("only-one")})
	w.Shutdown(context.Background())

	w2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Shutdown(context.Background())

	var count int
	err = w2.Replay(999, func(Event) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if count != 0 {
		t.Fatalf("count=%d, want 0", count)
	}
}

func TestIteratorLSNBeyondData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "iter_beyond.wal")
	w, err := Open(path, WithSyncMode(SyncBatch))
	if err != nil {
		t.Fatal(err)
	}
	w.Append(Event{Payload: []byte("only-one")})
	w.Shutdown(context.Background())

	w2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Shutdown(context.Background())

	it, err := w2.Iterator(999)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	if it.Next() {
		t.Fatal("expected no events for LSN beyond data")
	}
}

func TestIteratorCorruptionSetsErr(t *testing.T) {
	path := filepath.Join(t.TempDir(), "iter_corrupt.wal")
	s, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}

	enc := newEncoder(256)
	enc.encodeBatch([]Event{{Payload: []byte("valid")}}, 1, nil)
	goodData := make([]byte, len(enc.bytes()))
	copy(goodData, enc.bytes())
	s.Write(goodData)

	// Write a second batch with corrupted CRC.
	enc.reset()
	enc.encodeBatch([]Event{{Payload: []byte("bad!")}}, 2, nil)
	corruptData := make([]byte, len(enc.bytes()))
	copy(corruptData, enc.bytes())
	corruptData[len(corruptData)-1] ^= 0xFF
	s.Write(corruptData)
	s.Sync()
	s.Close()

	s2, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	it, err := newIterator(s2, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	count := 0
	for it.Next() {
		count++
	}
	if count != 1 {
		t.Fatalf("iterated %d, want 1 (before corruption)", count)
	}
	if it.Err() != ErrCorrupted {
		t.Fatalf("Err()=%v, want ErrCorrupted", it.Err())
	}
}

func TestReplayWithCustomStorage(t *testing.T) {
	ms := &memStorage{}

	w, err := Open("", WithStorage(ms), WithSyncMode(SyncBatch))
	if err != nil {
		t.Fatal(err)
	}

	w.Append(Event{Payload: []byte("custom-1")})
	w.Append(Event{Payload: []byte("custom-2")})
	w.Append(Event{Payload: []byte("custom-3")})

	if flushErr := w.Flush(); flushErr != nil {
		t.Fatal(flushErr)
	}

	var count int
	err = w.Replay(0, func(e Event) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if count != 3 {
		t.Fatalf("replayed %d, want 3", count)
	}

	w.Shutdown(context.Background())
}

func TestReplayOnRunningWAL(t *testing.T) {
	w := openTestWAL(t, WithSyncMode(SyncBatch))
	defer w.Shutdown(context.Background())

	for i := 0; i < 5; i++ {
		w.Append(Event{Payload: []byte("running")})
	}
	w.Flush()

	var count int
	err := w.Replay(0, func(Event) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("Replay on running WAL: %v", err)
	}
	if count != 5 {
		t.Fatalf("replayed %d, want 5", count)
	}
}

func TestReplayWithMeta(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta_replay.wal")
	w, err := Open(path, WithSyncMode(SyncBatch))
	if err != nil {
		t.Fatal(err)
	}

	w.Append(Event{Payload: []byte("data"), Meta: []byte("type:insert")})
	w.Shutdown(context.Background())

	w2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Shutdown(context.Background())

	var count int
	w2.Replay(0, func(e Event) error {
		count++
		if !bytes.Equal(e.Meta, []byte("type:insert")) {
			t.Fatalf("Meta=%q, want %q", e.Meta, "type:insert")
		}
		return nil
	})
	if count != 1 {
		t.Fatalf("count=%d, want 1", count)
	}
}

func TestBatchWithCompressor(t *testing.T) {
	comp := &testCompressor{}

	path := filepath.Join(t.TempDir(), "compressed.wal")
	w, err := Open(path, WithSyncMode(SyncBatch), WithCompressor(comp))
	if err != nil {
		t.Fatal(err)
	}

	w.Append(Event{Payload: []byte("compressed event"), Meta: []byte("m")})
	w.Append(Event{Payload: []byte("another one")})
	w.Shutdown(context.Background())

	w2, err := Open(path, WithCompressor(comp))
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Shutdown(context.Background())

	var events []Event
	w2.Replay(0, func(e Event) error {
		cp := make([]byte, len(e.Payload))
		copy(cp, e.Payload)
		var meta []byte
		if len(e.Meta) > 0 {
			meta = make([]byte, len(e.Meta))
			copy(meta, e.Meta)
		}
		events = append(events, Event{LSN: e.LSN, Payload: cp, Meta: meta})
		return nil
	})

	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	if string(events[0].Payload) != "compressed event" {
		t.Fatalf("Payload[0]=%q", events[0].Payload)
	}
	if string(events[0].Meta) != "m" {
		t.Fatalf("Meta[0]=%q", events[0].Meta)
	}
}

