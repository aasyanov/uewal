package uewal

import (
	"context"
	"testing"
	"time"
)

func TestWriter_GroupCommit(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Append 100 events in batches of 10; group commit coalesces multiple
	// batches per drain, so BatchesWritten < 100.
	for i := 0; i < 10; i++ {
		batch := NewBatch(10)
		for j := 0; j < 10; j++ {
			batch.Append([]byte("x"), nil, nil)
		}
		_, err := w.WriteUnsafe(batch)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	s := w.Stats()
	if s.BatchesWritten >= 100 {
		t.Errorf("BatchesWritten=%d, expected <100 (group commit coalesced)", s.BatchesWritten)
	}
	if s.EventsWritten != 100 {
		t.Errorf("EventsWritten=%d, want 100", s.EventsWritten)
	}

	if err := w.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestWriter_SyncBatch(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, WithSyncMode(SyncBatch))
	if err != nil {
		t.Fatal(err)
	}

	_, err = writeOne(w, []byte("sync-batch-test"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	s := w.Stats()
	if s.SyncCount == 0 {
		t.Errorf("SyncCount=%d, want >0 with SyncBatch", s.SyncCount)
	}

	if err := w.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestWriter_SyncInterval(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, WithSyncMode(SyncInterval), WithSyncInterval(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}

	// Let the sync ticker fire at least once before we append.
	time.Sleep(15 * time.Millisecond)
	_, err = writeOne(w, []byte("sync-interval-test"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	s := w.Stats()
	if s.SyncCount == 0 {
		t.Errorf("SyncCount=%d, want >0 with SyncInterval", s.SyncCount)
	}

	if err := w.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestWriter_PendingRotation(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, WithMaxSegmentSize(500))
	if err != nil {
		t.Fatal(err)
	}

	payload := make([]byte, 100)
	for i := range payload {
		payload[i] = 'x'
	}
	for i := 0; i < 20; i++ {
		_, err := writeOne(w, payload, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	s := w.Stats()
	if s.SegmentCount <= 1 {
		t.Errorf("SegmentCount=%d, want >1 (rotation should have occurred)", s.SegmentCount)
	}

	if err := w.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
