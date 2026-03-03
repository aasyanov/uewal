package uewal

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestMmapReaderEmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.wal")
	s, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	r, err := newMmapReader(s, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.bytes()) != 0 {
		t.Fatalf("expected empty bytes, got %d", len(r.bytes()))
	}
	r.close()
}

func TestMmapReaderReadData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.wal")
	s, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("hello mmap world")
	s.Write(data)
	s.Sync()
	size, _ := s.Size()

	r, err := newMmapReader(s, size)
	if err != nil {
		s.Close()
		t.Fatal(err)
	}

	if !bytes.Equal(r.bytes(), data) {
		t.Fatalf("mmap data=%q, want %q", r.bytes(), data)
	}
	r.close()
	s.Close()
}

func TestMmapReaderCloseIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "close.wal")
	s, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}

	s.Write([]byte("test"))
	s.Sync()
	size, _ := s.Size()

	r, err := newMmapReader(s, size)
	if err != nil {
		s.Close()
		t.Fatal(err)
	}

	if err := r.close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := r.close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	s.Close()
}

func TestMmapReaderWithEncodedRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.wal")
	s, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}

	enc := newEncoder(256)
	enc.encodeBatch([]Event{{Payload: []byte("event-1")}}, 1, nil)
	enc.encodeBatch([]Event{{Payload: []byte("event-2")}}, 2, nil)
	s.Write(enc.bytes())
	s.Sync()
	size, _ := s.Size()

	r, err := newMmapReader(s, size)
	if err != nil {
		s.Close()
		t.Fatal(err)
	}
	defer func() {
		r.close()
		s.Close()
	}()

	events, _, decErr := decodeAllBatches(r.bytes(), nil)
	if decErr != nil {
		t.Fatalf("decodeAllBatches error: %v", decErr)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	if events[0].LSN != 1 || string(events[0].Payload) != "event-1" {
		t.Errorf("event[0]=%+v", events[0])
	}
	if events[1].LSN != 2 || string(events[1].Payload) != "event-2" {
		t.Errorf("event[1]=%+v", events[1])
	}
}
