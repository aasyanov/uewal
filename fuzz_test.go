package uewal

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func FuzzDecodeBatch(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0x55, 0x57, 0x41, 0x4C}) // "UWAL" magic only

	events := []Event{{Payload: []byte("hello")}}
	buf := make([]byte, batchFrameSize(events)*2)
	frame, _, _ := encodeBatchFrame(buf, events, 1, nil)
	f.Add(frame)

	f.Fuzz(func(t *testing.T, data []byte) {
		decodeBatchFrame(data, 0, nil)
		decodeAllBatches(data, nil)
	})
}

func FuzzAppendReplay(f *testing.F) {
	f.Add([]byte("simple"), 1)
	f.Add([]byte{}, 0)
	f.Add([]byte("larger payload with more content for testing"), 5)

	f.Fuzz(func(t *testing.T, payload []byte, count int) {
		if count <= 0 || count > 100 {
			return
		}
		if len(payload) > 1024 {
			return
		}

		dir := t.TempDir()
		path := filepath.Join(dir, "fuzz.wal")

		w, err := Open(path, WithSyncMode(SyncBatch))
		if err != nil {
			t.Fatal(err)
		}

		for i := 0; i < count; i++ {
			if _, appendErr := w.Append(Event{Payload: payload}); appendErr != nil {
				t.Fatal(appendErr)
			}
		}
		w.Shutdown(context.Background())

		w2, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}

		replayed := 0
		w2.Replay(0, func(e Event) error {
			replayed++
			return nil
		})
		if replayed != count {
			t.Fatalf("replayed %d, want %d", replayed, count)
		}
		w2.Shutdown(context.Background())
	})
}

func FuzzRecoveryAfterCorruption(f *testing.F) {
	f.Add(3, []byte{0xFF, 0xFE})
	f.Add(5, []byte{0x00, 0x00, 0x00})
	f.Add(1, []byte{0x10})

	f.Fuzz(func(t *testing.T, validRecords int, garbage []byte) {
		if validRecords <= 0 || validRecords > 50 {
			return
		}
		if len(garbage) > 100 {
			return
		}

		dir := t.TempDir()
		path := filepath.Join(dir, "fuzz_corrupt.wal")

		s, err := NewFileStorage(path)
		if err != nil {
			t.Fatal(err)
		}

		enc := newEncoder(4096)
		for i := 0; i < validRecords; i++ {
			enc.encodeBatch([]Event{{Payload: []byte("valid")}}, LSN(i+1), nil)
		}
		s.Write(enc.bytes())
		if len(garbage) > 0 {
			s.Write(garbage)
		}
		s.Sync()
		s.Close()

		w, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}

		count := 0
		w.Replay(0, func(Event) error {
			count++
			return nil
		})

		if count != validRecords {
			t.Fatalf("replayed %d, want %d", count, validRecords)
		}

		w.Shutdown(context.Background())

		fi, _ := os.Stat(path)
		expectedSize := int64(len(enc.bytes()))
		if fi.Size() != expectedSize {
			t.Fatalf("file size=%d, want %d", fi.Size(), expectedSize)
		}
	})
}
