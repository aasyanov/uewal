package uewal

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"
)

// FuzzDecodeBatch feeds random bytes to decodeBatchFrame.
// Verifies that no input causes a panic — only clean errors.
func FuzzDecodeBatch(f *testing.F) {
	recs := []record{{payload: []byte("hello"), timestamp: 1000}}
	enc := newEncoder(1024)
	enc.encodeBatch(recs, 1, nil, false)
	f.Add(enc.bytes())

	f.Add([]byte{})
	f.Add([]byte{0})
	f.Add([]byte("EWAL"))

	f.Fuzz(func(t *testing.T, data []byte) {
		decodeBatchFrame(data, 0, nil)
	})
}

func fuzzShutdown(w *WAL) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	w.Shutdown(ctx)
}

// FuzzAppendReplay writes random payloads through a WAL and replays them,
// verifying that every appended event round-trips correctly.
func FuzzAppendReplay(f *testing.F) {
	f.Add([]byte("hello"))
	f.Add([]byte{})
	f.Add(make([]byte, 4096))

	f.Fuzz(func(t *testing.T, payload []byte) {
		if len(payload) > 1<<20 {
			payload = payload[:1<<20]
		}

		dir := t.TempDir()
		w, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer fuzzShutdown(w)

		lsn, err := writeOne(w, payload, nil, nil)
		if err != nil {
			t.Skip(err)
		}
		if err = w.Flush(); err != nil {
			t.Fatal(err)
		}

		var got []byte
		err = w.Replay(lsn, func(ev Event) error {
			got = make([]byte, len(ev.Payload))
			copy(got, ev.Payload)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		if len(payload) == 0 {
			if len(got) != 0 {
				t.Fatalf("expected empty payload, got %d bytes", len(got))
			}
		} else {
			if !bytes.Equal(got, payload) {
				t.Fatalf("payload mismatch: got %d bytes, want %d bytes", len(got), len(payload))
			}
		}
	})
}

// FuzzAppendReplayKeyMeta writes random key/meta/payload combinations
// and verifies round-trip correctness.
func FuzzAppendReplayKeyMeta(f *testing.F) {
	f.Add([]byte("p"), []byte("k"), []byte("m"))
	f.Add([]byte{}, []byte{}, []byte{})
	f.Add([]byte("payload"), []byte{}, []byte("meta"))
	f.Add([]byte{}, []byte("key"), []byte{})

	f.Fuzz(func(t *testing.T, payload, key, meta []byte) {
		if len(payload) > 1<<20 {
			payload = payload[:1<<20]
		}

		dir := t.TempDir()
		w, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer fuzzShutdown(w)

		lsn, err := writeOne(w, payload, key, meta)
		if err != nil {
			t.Skip(err)
		}
		if err = w.Flush(); err != nil {
			t.Fatal(err)
		}

		var ev Event
		w.Replay(lsn, func(e Event) error {
			ev = Event{
				LSN:       e.LSN,
				Timestamp: e.Timestamp,
				Key:       append([]byte(nil), e.Key...),
				Meta:      append([]byte(nil), e.Meta...),
				Payload:   append([]byte(nil), e.Payload...),
			}
			return nil
		})

		if !bytes.Equal(ev.Payload, payload) {
			t.Fatalf("payload mismatch")
		}
		if !bytes.Equal(ev.Key, key) {
			t.Fatalf("key mismatch")
		}
		if !bytes.Equal(ev.Meta, meta) {
			t.Fatalf("meta mismatch")
		}
	})
}

// FuzzImportBatch feeds random bytes to ImportBatch.
// Verifies that no input causes a panic.
func FuzzImportBatch(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("EWAL"))
	f.Add(make([]byte, 100))

	recs := []record{{payload: []byte("test"), timestamp: 1000}}
	enc := newEncoder(1024)
	enc.encodeBatch(recs, 1, nil, false)
	f.Add(enc.bytes())

	f.Fuzz(func(t *testing.T, data []byte) {
		dir := t.TempDir()
		w, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer w.Close()

		w.ImportBatch(data)
	})
}

// FuzzRecoveryAfterCorruption writes data, corrupts random bytes in the
// WAL segment file, and verifies that Open recovers without panic.
func FuzzRecoveryAfterCorruption(f *testing.F) {
	f.Add(uint64(0), byte(0xFF))
	f.Add(uint64(10), byte(0x00))
	f.Add(uint64(100), byte(0xAB))

	f.Fuzz(func(t *testing.T, corruptOffset uint64, corruptByte byte) {
		dir := t.TempDir()
		w, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}

		for i := 0; i < 10; i++ {
			if _, err = writeOne(w, []byte("event data for fuzz test"), nil, nil); err != nil {
				t.Fatal(err)
			}
		}
		w.Flush()
		w.Close()

		segs := w.Segments()
		if len(segs) == 0 {
			return
		}

		segPath := segs[len(segs)-1].Path
		data, err := os.ReadFile(segPath)
		if err != nil || len(data) == 0 {
			return
		}

		idx := corruptOffset % uint64(len(data))
		data[idx] ^= corruptByte

		os.WriteFile(segPath, data, 0644)

		w2, err := Open(dir)
		if err != nil {
			return
		}
		w2.Close()
	})
}
