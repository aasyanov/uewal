package uewal

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkAppend128B(b *testing.B) {
	dir := b.TempDir()
	w, err := Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Shutdown(context.Background())

	payload := make([]byte, 128)
	b.SetBytes(128)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := w.Append(payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendBatch(b *testing.B) {
	for _, size := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("batch_%d", size), func(b *testing.B) {
			dir := b.TempDir()
			w, err := Open(dir)
			if err != nil {
				b.Fatal(err)
			}
			defer w.Shutdown(context.Background())

			payload := make([]byte, 128)
			b.SetBytes(int64(size) * 128)
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				batch := NewBatch(size)
				for j := 0; j < size; j++ {
					batch.AppendUnsafe(payload)
				}
				if _, err := w.AppendBatch(batch); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkEncode(b *testing.B) {
	recs := make([]record, 100)
	for i := range recs {
		recs[i] = record{payload: make([]byte, 128), timestamp: 1000}
	}

	enc := newEncoder(64 << 10)
	b.SetBytes(int64(100 * 128))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		enc.reset()
		if err := enc.encodeBatch(recs, 1, nil, false); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecode(b *testing.B) {
	recs := make([]record, 100)
	for i := range recs {
		recs[i] = record{payload: make([]byte, 128), timestamp: 1000}
	}
	enc := newEncoder(64 << 10)
	enc.encodeBatch(recs, 1, nil, false)
	data := make([]byte, len(enc.bytes()))
	copy(data, enc.bytes())

	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, err := decodeBatchFrame(data, 0, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendWithKeyMeta(b *testing.B) {
	dir := b.TempDir()
	w, err := Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Shutdown(context.Background())

	payload := make([]byte, 128)
	key := []byte("user-12345")
	meta := []byte("event-type")

	b.SetBytes(128 + 10 + 10)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := w.Append(payload, WithKey(key), WithMeta(meta)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendDurable(b *testing.B) {
	dir := b.TempDir()
	w, err := Open(dir, WithSyncMode(SyncBatch))
	if err != nil {
		b.Fatal(err)
	}
	defer w.Shutdown(context.Background())

	payload := make([]byte, 128)
	b.SetBytes(128)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := w.Append(payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendParallel(b *testing.B) {
	dir := b.TempDir()
	w, err := Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Shutdown(context.Background())

	payload := make([]byte, 128)
	b.SetBytes(128)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := w.Append(payload); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkFlush(b *testing.B) {
	dir := b.TempDir()
	w, err := Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Shutdown(context.Background())

	payload := make([]byte, 128)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w.Append(payload)
		if err := w.Flush(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFlushAndSync(b *testing.B) {
	dir := b.TempDir()
	w, err := Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Shutdown(context.Background())

	payload := make([]byte, 128)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w.Append(payload)
		w.Flush()
		if err := w.Sync(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReplay(b *testing.B) {
	dir := b.TempDir()
	w, err := Open(dir)
	if err != nil {
		b.Fatal(err)
	}

	const eventCount = 100_000
	payload := make([]byte, 256)
	for i := 0; i < eventCount; i++ {
		w.Append(payload)
	}
	w.Flush()

	b.SetBytes(int64(eventCount) * 256)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w.Replay(0, func(ev Event) error { return nil })
	}

	b.StopTimer()
	w.Shutdown(context.Background())
}

func BenchmarkIterator(b *testing.B) {
	dir := b.TempDir()
	w, err := Open(dir)
	if err != nil {
		b.Fatal(err)
	}

	const eventCount = 100_000
	payload := make([]byte, 256)
	for i := 0; i < eventCount; i++ {
		w.Append(payload)
	}
	w.Flush()

	b.SetBytes(int64(eventCount) * 256)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		it, err := w.Iterator(0)
		if err != nil {
			b.Fatal(err)
		}
		for it.Next() {
		}
		it.Close()
	}

	b.StopTimer()
	w.Shutdown(context.Background())
}

func BenchmarkScanBatchHeader(b *testing.B) {
	recs := make([]record, 10)
	for i := range recs {
		recs[i] = record{payload: make([]byte, 128), timestamp: 1000}
	}
	enc := newEncoder(64 << 10)
	enc.encodeBatch(recs, 1, nil, false)
	data := make([]byte, len(enc.bytes()))
	copy(data, enc.bytes())

	b.SetBytes(int64(batchHeaderLen))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := scanBatchFrame(data, 0)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRecovery(b *testing.B) {
	dir := b.TempDir()
	w, err := Open(dir)
	if err != nil {
		b.Fatal(err)
	}

	const eventCount = 100_000
	payload := make([]byte, 128)
	for i := 0; i < eventCount; i++ {
		w.Append(payload)
	}
	w.Shutdown(context.Background())

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w2, err := Open(dir)
		if err != nil {
			b.Fatal(err)
		}
		w2.Close()
	}
}

func BenchmarkWaitDurable(b *testing.B) {
	dir := b.TempDir()
	w, err := Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Shutdown(context.Background())

	payload := make([]byte, 128)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		lsn, _ := w.Append(payload)
		if err := w.WaitDurable(lsn); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeInto(b *testing.B) {
	recs := make([]record, 100)
	for i := range recs {
		recs[i] = record{payload: make([]byte, 128), timestamp: 1000}
	}
	enc := newEncoder(64 << 10)
	enc.encodeBatch(recs, 1, nil, false)
	data := make([]byte, len(enc.bytes()))
	copy(data, enc.bytes())

	buf := make([]Event, 0, 100)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf = buf[:0]
		_, _, err := decodeBatchFrameInto(data, 0, nil, buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

var _ = fmt.Sprint
