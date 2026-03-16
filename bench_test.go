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
