package uewal

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// ─────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────

type nopCompressor struct{}

func (nopCompressor) Compress(src []byte) ([]byte, error)   { return append([]byte(nil), src...), nil }
func (nopCompressor) Decompress(src []byte) ([]byte, error) { return append([]byte(nil), src...), nil }

type shrinkCompressor struct{ ratio float64 }

func (c shrinkCompressor) Compress(src []byte) ([]byte, error) {
	n := int(float64(len(src)) * c.ratio)
	if n < 1 {
		n = 1
	}
	return make([]byte, n), nil
}
func (c shrinkCompressor) Decompress(src []byte) ([]byte, error) { return src, nil }

func openBench(b *testing.B, opts ...Option) *WAL {
	b.Helper()
	dir := b.TempDir()
	w, err := Open(dir, opts...)
	if err != nil {
		b.Fatal(err)
	}
	return w
}

func seedWAL(b *testing.B, w *WAL, count int, payloadSize int) {
	b.Helper()
	payload := make([]byte, payloadSize)
	for i := 0; i < count; i++ {
		if _, err := writeOne(w, payload, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		b.Fatal(err)
	}
}

func seedWALWithBatches(b *testing.B, w *WAL, batchCount, batchSize, payloadSize int) {
	b.Helper()
	payload := make([]byte, payloadSize)
	for i := 0; i < batchCount; i++ {
		batch := NewBatch(batchSize)
		for j := 0; j < batchSize; j++ {
			batch.AppendUnsafe(payload, nil, nil)
		}
		if _, err := w.WriteUnsafe(batch); err != nil {
			b.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		b.Fatal(err)
	}
}

// ═════════════════════════════════════════════════════════════
// 1. WRITE PATH — Single Append
// ═════════════════════════════════════════════════════════════

func BenchmarkAppend_PayloadOnly_128B(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := writeOne(w, payload, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppend_PayloadOnly_1KB(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	payload := make([]byte, 1024)
	b.SetBytes(1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := writeOne(w, payload, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppend_PayloadOnly_64KB(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	payload := make([]byte, 64<<10)
	b.SetBytes(64 << 10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := writeOne(w, payload, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppend_WithKey(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	key := []byte("user-12345")
	b.SetBytes(128 + 10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := writeOne(w, payload, key, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppend_WithTimestamp(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	ts := time.Now().UnixNano()
	b.SetBytes(128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := writeOne(w, payload, nil, nil, WithTimestamp(ts)); err != nil {
			b.Fatal(err)
		}
	}
}

// ═════════════════════════════════════════════════════════════
// 2. WRITE PATH — Batch Append
// ═════════════════════════════════════════════════════════════

func BenchmarkBatchAppend_Size1(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := NewBatch(1)
		batch.AppendUnsafe(payload, nil, nil)
		if _, err := w.WriteUnsafe(batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchAppend_Size10(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(10 * 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := NewBatch(10)
		for j := 0; j < 10; j++ {
			batch.AppendUnsafe(payload, nil, nil)
		}
		if _, err := w.WriteUnsafe(batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchAppend_Size100(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(100 * 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := NewBatch(100)
		for j := 0; j < 100; j++ {
			batch.AppendUnsafe(payload, nil, nil)
		}
		if _, err := w.WriteUnsafe(batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchAppend_Size1000(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(1000 * 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := NewBatch(1000)
		for j := 0; j < 1000; j++ {
			batch.AppendUnsafe(payload, nil, nil)
		}
		if _, err := w.WriteUnsafe(batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchAppend_CopySemantics_100(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(100 * 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := NewBatch(100)
		for j := 0; j < 100; j++ {
			batch.Append(payload, nil, nil)
		}
		if _, err := w.WriteUnsafe(batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchAppend_UnsafeSemantics_100(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(100 * 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := NewBatch(100)
		for j := 0; j < 100; j++ {
			batch.AppendUnsafe(payload, nil, nil)
		}
		if _, err := w.WriteUnsafe(batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchAppend_WithKeyMeta_100(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	key := []byte("entity-key")
	meta := []byte("some-meta")
	b.SetBytes(100 * (128 + 10 + 9))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := NewBatch(100)
		for j := 0; j < 100; j++ {
			batch.AppendUnsafe(payload, key, meta)
		}
		if _, err := w.WriteUnsafe(batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchAppend_Reuse_100(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	batch := NewBatch(100)
	b.SetBytes(100 * 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch.Reset()
		for j := 0; j < 100; j++ {
			batch.AppendUnsafe(payload, nil, nil)
		}
		if _, err := w.Write(batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchAppend_LargePayload4KB_100(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	payload := make([]byte, 4096)
	b.SetBytes(100 * 4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := NewBatch(100)
		for j := 0; j < 100; j++ {
			batch.AppendUnsafe(payload, nil, nil)
		}
		if _, err := w.WriteUnsafe(batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchAppend_MixedTimestamps_100(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(100 * 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := NewBatch(100)
		base := time.Now().UnixNano()
		for j := 0; j < 100; j++ {
			batch.AppendUnsafe(payload, nil, nil, WithTimestamp(base+int64(j)))
		}
		if _, err := w.WriteUnsafe(batch); err != nil {
			b.Fatal(err)
		}
	}
}

// ═════════════════════════════════════════════════════════════
// 3. SYNC MODES
// ═════════════════════════════════════════════════════════════

func BenchmarkAppend_SyncNever(b *testing.B) {
	w := openBench(b, WithSyncMode(SyncNever))
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := writeOne(w, payload, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppend_SyncBatch(b *testing.B) {
	w := openBench(b, WithSyncMode(SyncBatch))
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := writeOne(w, payload, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppend_SyncInterval100ms(b *testing.B) {
	w := openBench(b, WithSyncMode(SyncInterval), WithSyncInterval(100*time.Millisecond))
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := writeOne(w, payload, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppend_SyncCount10(b *testing.B) {
	w := openBench(b, WithSyncCount(10))
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := writeOne(w, payload, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppend_SyncSize4KB(b *testing.B) {
	w := openBench(b, WithSyncSize(4096))
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := writeOne(w, payload, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppend_WithTimestampDirect(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	ts := time.Now().UnixNano()
	b.SetBytes(128)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := NewBatch(1)
		batch.AppendWithTimestamp(payload, nil, nil, ts)
		if _, err := w.Write(batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppend_WithTimestampClosure(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	ts := time.Now().UnixNano()
	b.SetBytes(128)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := NewBatch(1)
		batch.Append(payload, nil, nil, WithTimestamp(ts))
		if _, err := w.Write(batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFlush_SingleEvent(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writeOne(w, payload, nil, nil)
		if err := w.Flush(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFlushThenSync(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writeOne(w, payload, nil, nil)
		w.Flush()
		if err := w.Sync(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWaitDurable_SyncNever(b *testing.B) {
	w := openBench(b, WithSyncMode(SyncNever))
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lsn, _ := writeOne(w, payload, nil, nil)
		if err := w.WaitDurable(lsn); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWaitDurable_SyncBatch(b *testing.B) {
	w := openBench(b, WithSyncMode(SyncBatch))
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lsn, _ := writeOne(w, payload, nil, nil)
		if err := w.WaitDurable(lsn); err != nil {
			b.Fatal(err)
		}
	}
}

// ═════════════════════════════════════════════════════════════
// 4. COMPRESSION
// ═════════════════════════════════════════════════════════════

func BenchmarkAppend_NopCompressor(b *testing.B) {
	w := openBench(b, WithCompressor(nopCompressor{}))
	defer w.Shutdown(context.Background())
	payload := bytes.Repeat([]byte("A"), 1024)
	b.SetBytes(1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := writeOne(w, payload, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppend_ShrinkCompressor50pct(b *testing.B) {
	w := openBench(b, WithCompressor(shrinkCompressor{ratio: 0.5}))
	defer w.Shutdown(context.Background())
	payload := bytes.Repeat([]byte("A"), 1024)
	b.SetBytes(1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := writeOne(w, payload, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppend_WithNoCompressFlag(b *testing.B) {
	w := openBench(b, WithCompressor(nopCompressor{}))
	defer w.Shutdown(context.Background())
	payload := make([]byte, 1024)
	b.SetBytes(1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := writeOne(w, payload, nil, nil, WithNoCompress()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchAppend_MarkNoCompress_100(b *testing.B) {
	w := openBench(b, WithCompressor(nopCompressor{}))
	defer w.Shutdown(context.Background())
	payload := make([]byte, 1024)
	b.SetBytes(100 * 1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := NewBatch(100)
		batch.MarkNoCompress()
		for j := 0; j < 100; j++ {
			batch.AppendUnsafe(payload, nil, nil)
		}
		if _, err := w.WriteUnsafe(batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchAppend_Compression_100(b *testing.B) {
	w := openBench(b, WithCompressor(shrinkCompressor{ratio: 0.5}))
	defer w.Shutdown(context.Background())
	payload := bytes.Repeat([]byte("X"), 1024)
	b.SetBytes(100 * 1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := NewBatch(100)
		for j := 0; j < 100; j++ {
			batch.AppendUnsafe(payload, nil, nil)
		}
		if _, err := w.WriteUnsafe(batch); err != nil {
			b.Fatal(err)
		}
	}
}

// ═════════════════════════════════════════════════════════════
// 5. BUFFER & QUEUE SIZING
// ═════════════════════════════════════════════════════════════

func BenchmarkAppend_BufferSize4KB(b *testing.B) {
	w := openBench(b, WithBufferSize(4<<10))
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := writeOne(w, payload, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppend_BufferSize1MB(b *testing.B) {
	w := openBench(b, WithBufferSize(1<<20))
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := writeOne(w, payload, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppend_QueueSize64(b *testing.B) {
	w := openBench(b, WithQueueSize(64))
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := writeOne(w, payload, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppend_QueueSize16384(b *testing.B) {
	w := openBench(b, WithQueueSize(16384))
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := writeOne(w, payload, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// ═════════════════════════════════════════════════════════════
// 6. BACKPRESSURE MODES
// ═════════════════════════════════════════════════════════════

func BenchmarkAppend_BackpressureBlock(b *testing.B) {
	w := openBench(b, WithBackpressure(BlockMode))
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := writeOne(w, payload, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppend_BackpressureDrop(b *testing.B) {
	w := openBench(b, WithBackpressure(DropMode))
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writeOne(w, payload, nil, nil)
	}
}

func BenchmarkAppend_BackpressureError(b *testing.B) {
	w := openBench(b, WithBackpressure(ErrorMode))
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writeOne(w, payload, nil, nil)
	}
}

// ═════════════════════════════════════════════════════════════
// 7. CONCURRENCY — Parallel Writers
// ═════════════════════════════════════════════════════════════

func BenchmarkAppendParallel_4Writers(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(128)
	b.SetParallelism(4)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := writeOne(w, payload, nil, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkAppendParallel_16Writers(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(128)
	b.SetParallelism(16)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := writeOne(w, payload, nil, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkAppendParallel_SyncBatch(b *testing.B) {
	w := openBench(b, WithSyncMode(SyncBatch))
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(128)
	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := writeOne(w, payload, nil, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkBatchAppendParallel_100(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(100 * 128)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			batch := NewBatch(100)
			for j := 0; j < 100; j++ {
				batch.AppendUnsafe(payload, nil, nil)
			}
			if _, err := w.WriteUnsafe(batch); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkAppendParallel_WithKeyMeta(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	key := []byte("user-key")
	meta := []byte("meta-val")
	b.SetBytes(128 + 8 + 8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := writeOne(w, payload, key, meta); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// ═════════════════════════════════════════════════════════════
// 8. ENCODING — Wire Format
// ═════════════════════════════════════════════════════════════

func BenchmarkEncode_10Records_128B(b *testing.B) {
	recs := make([]record, 10)
	for i := range recs {
		recs[i] = record{payload: make([]byte, 128), timestamp: 1000}
	}
	enc := newEncoder(64 << 10)
	b.SetBytes(int64(10 * 128))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		enc.reset()
		if err := enc.encodeBatch(recs, 1, nil, false); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncode_100Records_128B(b *testing.B) {
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

func BenchmarkEncode_1000Records_128B(b *testing.B) {
	recs := make([]record, 1000)
	for i := range recs {
		recs[i] = record{payload: make([]byte, 128), timestamp: 1000}
	}
	enc := newEncoder(256 << 10)
	b.SetBytes(int64(1000 * 128))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		enc.reset()
		if err := enc.encodeBatch(recs, 1, nil, false); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncode_100Records_4KB(b *testing.B) {
	recs := make([]record, 100)
	for i := range recs {
		recs[i] = record{payload: make([]byte, 4096), timestamp: 1000}
	}
	enc := newEncoder(512 << 10)
	b.SetBytes(int64(100 * 4096))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		enc.reset()
		if err := enc.encodeBatch(recs, 1, nil, false); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncode_UniformTimestamp(b *testing.B) {
	recs := make([]record, 100)
	ts := time.Now().UnixNano()
	for i := range recs {
		recs[i] = record{payload: make([]byte, 128), timestamp: ts}
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

func BenchmarkEncode_PerRecordTimestamp(b *testing.B) {
	recs := make([]record, 100)
	for i := range recs {
		recs[i] = record{payload: make([]byte, 128), timestamp: int64(i * 1000)}
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

func BenchmarkEncode_WithKeyMeta(b *testing.B) {
	recs := make([]record, 100)
	for i := range recs {
		recs[i] = record{
			payload:   make([]byte, 128),
			key:       []byte("entity-key"),
			meta:      []byte("meta-data"),
			timestamp: 1000,
		}
	}
	enc := newEncoder(64 << 10)
	b.SetBytes(int64(100 * (128 + 10 + 9)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		enc.reset()
		if err := enc.encodeBatch(recs, 1, nil, false); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncode_WithCompressor(b *testing.B) {
	recs := make([]record, 100)
	for i := range recs {
		recs[i] = record{payload: bytes.Repeat([]byte("A"), 128), timestamp: 1000}
	}
	comp := shrinkCompressor{ratio: 0.5}
	enc := newEncoder(64 << 10)
	b.SetBytes(int64(100 * 128))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		enc.reset()
		if err := enc.encodeBatch(recs, 1, comp, false); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeRecordsRegion_100x128B(b *testing.B) {
	recs := make([]record, 100)
	for i := range recs {
		recs[i] = record{payload: make([]byte, 128), timestamp: 1000}
	}
	size := recordsRegionSize(recs, false)
	dst := make([]byte, size)
	b.SetBytes(int64(size))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encodeRecordsRegion(dst, recs, false, false)
	}
}

func BenchmarkEncodeRecordsRegion_PerRecTS(b *testing.B) {
	recs := make([]record, 100)
	for i := range recs {
		recs[i] = record{payload: make([]byte, 128), timestamp: int64(i)}
	}
	size := recordsRegionSize(recs, true)
	dst := make([]byte, size)
	b.SetBytes(int64(size))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encodeRecordsRegion(dst, recs, true, false)
	}
}

func BenchmarkEncodeBatchFrame_Direct_100x128B(b *testing.B) {
	recs := make([]record, 100)
	for i := range recs {
		recs[i] = record{payload: make([]byte, 128), timestamp: 1000}
	}
	size := batchOverhead + recordsRegionSize(recs, false)
	dst := make([]byte, size)
	b.SetBytes(int64(size))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := encodeBatchFrame(dst, recs, 1, nil, false); err != nil {
			b.Fatal(err)
		}
	}
}

// ═════════════════════════════════════════════════════════════
// 9. DECODING — Wire Format
// ═════════════════════════════════════════════════════════════

func benchDecodePrepare(b *testing.B, numRecs int, payloadSize int, perRecTS bool, comp Compressor) []byte {
	b.Helper()
	recs := make([]record, numRecs)
	for i := range recs {
		recs[i] = record{payload: make([]byte, payloadSize)}
		if perRecTS {
			recs[i].timestamp = int64(i * 1000)
		} else {
			recs[i].timestamp = 1000
		}
	}
	enc := newEncoder(512 << 10)
	if err := enc.encodeBatch(recs, 1, comp, false); err != nil {
		b.Fatal(err)
	}
	data := make([]byte, len(enc.bytes()))
	copy(data, enc.bytes())
	return data
}

func BenchmarkDecode_10Records_128B(b *testing.B) {
	data := benchDecodePrepare(b, 10, 128, false, nil)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := decodeBatchFrame(data, 0, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecode_100Records_128B(b *testing.B) {
	data := benchDecodePrepare(b, 100, 128, false, nil)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := decodeBatchFrame(data, 0, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecode_1000Records_128B(b *testing.B) {
	data := benchDecodePrepare(b, 1000, 128, false, nil)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := decodeBatchFrame(data, 0, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecode_100Records_4KB(b *testing.B) {
	data := benchDecodePrepare(b, 100, 4096, false, nil)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := decodeBatchFrame(data, 0, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecode_PerRecordTimestamp(b *testing.B) {
	data := benchDecodePrepare(b, 100, 128, true, nil)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := decodeBatchFrame(data, 0, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeInto_BufferReuse_100(b *testing.B) {
	data := benchDecodePrepare(b, 100, 128, false, nil)
	buf := make([]Event, 0, 100)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = buf[:0]
		if _, _, err := decodeBatchFrameInto(data, 0, nil, buf); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeInto_NoBufferReuse_100(b *testing.B) {
	data := benchDecodePrepare(b, 100, 128, false, nil)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := decodeBatchFrameInto(data, 0, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeInto_WithDecompressor(b *testing.B) {
	comp := nopCompressor{}
	data := benchDecodePrepare(b, 100, 128, false, comp)
	buf := make([]Event, 0, 100)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = buf[:0]
		if _, _, err := decodeBatchFrameInto(data, 0, comp, buf); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkScanBatchFrame_HeaderOnly(b *testing.B) {
	data := benchDecodePrepare(b, 10, 128, false, nil)
	b.SetBytes(int64(batchHeaderLen))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := scanBatchFrame(data, 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkScanBatchFrame_LargeFrame(b *testing.B) {
	data := benchDecodePrepare(b, 1000, 1024, false, nil)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := scanBatchFrame(data, 0); err != nil {
			b.Fatal(err)
		}
	}
}

// ═════════════════════════════════════════════════════════════
// 10. READ PATH — Replay
// ═════════════════════════════════════════════════════════════

func BenchmarkReplay_10KEvents_256B(b *testing.B) {
	w := openBench(b)
	seedWAL(b, w, 10_000, 256)
	b.SetBytes(int64(10_000) * 256)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Replay(0, func(ev Event) error { return nil })
	}
	b.StopTimer()
	w.Shutdown(context.Background())
}

func BenchmarkReplay_100KEvents_256B(b *testing.B) {
	w := openBench(b)
	seedWAL(b, w, 100_000, 256)
	b.SetBytes(int64(100_000) * 256)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Replay(0, func(ev Event) error { return nil })
	}
	b.StopTimer()
	w.Shutdown(context.Background())
}

func BenchmarkReplay_FromMiddle_100KEvents(b *testing.B) {
	w := openBench(b)
	seedWAL(b, w, 100_000, 128)
	b.SetBytes(int64(50_000) * 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Replay(50_001, func(ev Event) error { return nil })
	}
	b.StopTimer()
	w.Shutdown(context.Background())
}

func BenchmarkReplayRange_Last1K_Of100K(b *testing.B) {
	w := openBench(b)
	seedWAL(b, w, 100_000, 128)
	b.SetBytes(int64(1_000) * 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.ReplayRange(99_001, 100_000, func(ev Event) error { return nil })
	}
	b.StopTimer()
	w.Shutdown(context.Background())
}

func BenchmarkReplayBatches_100KEvents(b *testing.B) {
	w := openBench(b)
	seedWAL(b, w, 100_000, 128)
	b.SetBytes(int64(100_000) * 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.ReplayBatches(0, func(batch []Event) error { return nil })
	}
	b.StopTimer()
	w.Shutdown(context.Background())
}

func BenchmarkReplay_MultiSegment_100KEvents(b *testing.B) {
	w := openBench(b, WithMaxSegmentSize(1<<20))
	seedWAL(b, w, 100_000, 128)
	b.SetBytes(int64(100_000) * 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Replay(0, func(ev Event) error { return nil })
	}
	b.StopTimer()
	w.Shutdown(context.Background())
}

func BenchmarkReplay_SparseSeek_100KEvents(b *testing.B) {
	w := openBench(b)
	seedWAL(b, w, 100_000, 128)
	b.SetBytes(int64(1_000) * 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Replay(99_001, func(ev Event) error { return nil })
	}
	b.StopTimer()
	w.Shutdown(context.Background())
}

func BenchmarkReplay_Batched_1KBatches_100(b *testing.B) {
	w := openBench(b)
	seedWALWithBatches(b, w, 1000, 100, 128)
	b.SetBytes(int64(1000*100) * 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Replay(0, func(ev Event) error { return nil })
	}
	b.StopTimer()
	w.Shutdown(context.Background())
}

// ═════════════════════════════════════════════════════════════
// 11. READ PATH — Iterator
// ═════════════════════════════════════════════════════════════

func BenchmarkIterator_10KEvents(b *testing.B) {
	w := openBench(b)
	seedWAL(b, w, 10_000, 256)
	b.SetBytes(int64(10_000) * 256)
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

func BenchmarkIterator_100KEvents(b *testing.B) {
	w := openBench(b)
	seedWAL(b, w, 100_000, 256)
	b.SetBytes(int64(100_000) * 256)
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

func BenchmarkIterator_FromMiddle_100K(b *testing.B) {
	w := openBench(b)
	seedWAL(b, w, 100_000, 128)
	b.SetBytes(int64(50_000) * 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it, err := w.Iterator(50_001)
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

func BenchmarkIterator_MultiSegment_100K(b *testing.B) {
	w := openBench(b, WithMaxSegmentSize(1<<20))
	seedWAL(b, w, 100_000, 128)
	b.SetBytes(int64(100_000) * 128)
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

func BenchmarkIterator_ReadPayload_100K(b *testing.B) {
	w := openBench(b)
	seedWAL(b, w, 100_000, 128)
	b.SetBytes(int64(100_000) * 128)
	b.ResetTimer()
	var sink byte
	for i := 0; i < b.N; i++ {
		it, err := w.Iterator(0)
		if err != nil {
			b.Fatal(err)
		}
		for it.Next() {
			ev := it.Event()
			if len(ev.Payload) > 0 {
				sink = ev.Payload[0]
			}
		}
		it.Close()
	}
	_ = sink
	b.StopTimer()
	w.Shutdown(context.Background())
}

// ═════════════════════════════════════════════════════════════
// 12. RECOVERY — Open After Crash
// ═════════════════════════════════════════════════════════════

func BenchmarkRecovery_1KEvents(b *testing.B) {
	dir := b.TempDir()
	w, _ := Open(dir)
	seedWAL(b, w, 1_000, 128)
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

func BenchmarkRecovery_100KEvents(b *testing.B) {
	dir := b.TempDir()
	w, _ := Open(dir)
	seedWAL(b, w, 100_000, 128)
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

func BenchmarkRecovery_MultiSegment_100K(b *testing.B) {
	dir := b.TempDir()
	w, _ := Open(dir, WithMaxSegmentSize(1<<20))
	seedWAL(b, w, 100_000, 128)
	w.Shutdown(context.Background())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w2, err := Open(dir, WithMaxSegmentSize(1<<20))
		if err != nil {
			b.Fatal(err)
		}
		w2.Close()
	}
}

func BenchmarkRecovery_LargePayload4KB_10K(b *testing.B) {
	dir := b.TempDir()
	w, _ := Open(dir)
	seedWAL(b, w, 10_000, 4096)
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

// ═════════════════════════════════════════════════════════════
// 13. ROTATION
// ═════════════════════════════════════════════════════════════

func BenchmarkRotation_Manual(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writeOne(w, payload, nil, nil)
		if err := w.Rotate(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRotation_AutoBySize_1MB(b *testing.B) {
	w := openBench(b, WithMaxSegmentSize(1<<20))
	defer w.Shutdown(context.Background())
	payload := make([]byte, 4096)
	b.SetBytes(4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := writeOne(w, payload, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppend_SmallSegment256KB(b *testing.B) {
	w := openBench(b, WithMaxSegmentSize(256<<10))
	defer w.Shutdown(context.Background())
	payload := make([]byte, 1024)
	b.SetBytes(1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := writeOne(w, payload, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppend_LargeSegment256MB(b *testing.B) {
	w := openBench(b, WithMaxSegmentSize(256<<20))
	defer w.Shutdown(context.Background())
	payload := make([]byte, 1024)
	b.SetBytes(1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := writeOne(w, payload, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// ═════════════════════════════════════════════════════════════
// 14. PREALLOCATE
// ═════════════════════════════════════════════════════════════

func BenchmarkAppend_Preallocate_On(b *testing.B) {
	w := openBench(b, WithPreallocate(true), WithMaxSegmentSize(64<<20))
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := writeOne(w, payload, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppend_Preallocate_Off(b *testing.B) {
	w := openBench(b, WithPreallocate(false), WithMaxSegmentSize(64<<20))
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := writeOne(w, payload, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// ═════════════════════════════════════════════════════════════
// 15. SPARSE INDEX — Lookup Performance
// ═════════════════════════════════════════════════════════════

func BenchmarkSparseIndex_FindByLSN_10KEntries(b *testing.B) {
	si := &sparseIndex{}
	for i := 0; i < 10_000; i++ {
		si.append(sparseEntry{FirstLSN: LSN(i * 100), Offset: int64(i * 4096), Timestamp: int64(i * 1000)})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		si.findByLSN(LSN(500_000))
	}
}

func BenchmarkSparseIndex_FindByTimestamp_10KEntries(b *testing.B) {
	si := &sparseIndex{}
	for i := 0; i < 10_000; i++ {
		si.append(sparseEntry{FirstLSN: LSN(i * 100), Offset: int64(i * 4096), Timestamp: int64(i * 1000)})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		si.findByTimestamp(5_000_000)
	}
}

func BenchmarkSparseIndex_Append(b *testing.B) {
	si := &sparseIndex{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		si.append(sparseEntry{FirstLSN: LSN(i * 100), Offset: int64(i * 4096), Timestamp: int64(i * 1000)})
	}
}

func BenchmarkSparseIndex_Marshal_1KEntries(b *testing.B) {
	si := &sparseIndex{}
	for i := 0; i < 1000; i++ {
		si.append(sparseEntry{FirstLSN: LSN(i * 100), Offset: int64(i * 4096), Timestamp: int64(i)})
	}
	b.SetBytes(int64(1000 * sparseEntrySize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		si.marshal()
	}
}

func BenchmarkSparseIndex_Unmarshal_1KEntries(b *testing.B) {
	si := &sparseIndex{}
	for i := 0; i < 1000; i++ {
		si.append(sparseEntry{FirstLSN: LSN(i * 100), Offset: int64(i * 4096), Timestamp: int64(i)})
	}
	data := si.marshal()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := unmarshalSparseIndex(data); err != nil {
			b.Fatal(err)
		}
	}
}

// ═════════════════════════════════════════════════════════════
// 16. WRITE QUEUE — Internal Throughput
// ═════════════════════════════════════════════════════════════

func BenchmarkQueue_EnqueueDequeue_Paired(b *testing.B) {
	q := newWriteQueue(4096)
	done := make(chan struct{})
	go func() {
		var buf []writeBatch
		for {
			var ok bool
			buf, ok = q.dequeueAllInto(buf[:0])
			if !ok {
				close(done)
				return
			}
		}
	}()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.enqueue(writeBatch{lsnStart: LSN(i)})
	}
	q.close()
	<-done
}

func BenchmarkQueue_TryEnqueue_Fast(b *testing.B) {
	q := newWriteQueue(4096)
	done := make(chan struct{})
	go func() {
		var buf []writeBatch
		for {
			var ok bool
			buf, ok = q.dequeueAllInto(buf[:0])
			if !ok {
				close(done)
				return
			}
		}
	}()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.tryEnqueue(writeBatch{lsnStart: LSN(i)})
	}
	q.close()
	<-done
}

func BenchmarkQueue_Contended_8Producers(b *testing.B) {
	q := newWriteQueue(8192)
	done := make(chan struct{})
	go func() {
		var buf []writeBatch
		for {
			var ok bool
			buf, ok = q.dequeueAllInto(buf[:0])
			if !ok {
				close(done)
				return
			}
		}
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			q.enqueue(writeBatch{lsnStart: LSN(i)})
			i++
		}
	})
	q.close()
	<-done
}

// ═════════════════════════════════════════════════════════════
// 17. DURABLE NOTIFIER — Sync Wait Overhead
// ═════════════════════════════════════════════════════════════

func BenchmarkDurableNotifier_AdvanceNoWaiters(b *testing.B) {
	d := &durableNotifier{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.advance(LSN(i))
	}
}

func BenchmarkDurableNotifier_WaitAlreadySynced(b *testing.B) {
	d := &durableNotifier{}
	d.advance(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.wait(1)
	}
}

func BenchmarkDurableNotifier_AdvanceWithWaiters(b *testing.B) {
	d := &durableNotifier{}
	var wg sync.WaitGroup
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lsn := LSN(i + 1)
		wg.Add(1)
		go func() {
			d.wait(lsn)
			wg.Done()
		}()
		d.advance(lsn)
		wg.Wait()
	}
}

// ═════════════════════════════════════════════════════════════
// 18. MANIFEST — Serialization
// ═════════════════════════════════════════════════════════════

func buildTestManifest(segCount int) *manifest {
	m := &manifest{lastLSN: LSN(segCount * 1000)}
	for i := 0; i < segCount; i++ {
		m.entries = append(m.entries, manifestEntry{
			firstLSN:  LSN(i * 1000),
			lastLSN:   LSN(i*1000 + 999),
			size:      1 << 20,
			createdAt: time.Now().UnixNano(),
			firstTS:   time.Now().UnixNano(),
			lastTS:    time.Now().UnixNano(),
			sealed:    true,
		})
	}
	return m
}

func BenchmarkManifest_Marshal_100Segments(b *testing.B) {
	m := buildTestManifest(100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.marshal()
	}
}

func BenchmarkManifest_Unmarshal_100Segments(b *testing.B) {
	data := buildTestManifest(100).marshal()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := unmarshalManifest(data); err != nil {
			b.Fatal(err)
		}
	}
}

// ═════════════════════════════════════════════════════════════
// 19. MEMORY — Pool & Allocation
// ═════════════════════════════════════════════════════════════

func BenchmarkRecordSlicePool_GetPut(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, sp := getRecordSlice(8)
		putRecordSlice(sp, s)
	}
}

func BenchmarkBatch_NewAndFill_100(b *testing.B) {
	payload := make([]byte, 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := NewBatch(100)
		for j := 0; j < 100; j++ {
			batch.Append(payload, nil, nil)
		}
	}
}

func BenchmarkBatch_ResetAndFill_100(b *testing.B) {
	payload := make([]byte, 128)
	batch := NewBatch(100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch.Reset()
		for j := 0; j < 100; j++ {
			batch.Append(payload, nil, nil)
		}
	}
}

// ═════════════════════════════════════════════════════════════
// 20. ENCODER — Buffer Growth
// ═════════════════════════════════════════════════════════════

func BenchmarkEncoder_Reset(b *testing.B) {
	enc := newEncoder(64 << 10)
	recs := make([]record, 100)
	for i := range recs {
		recs[i] = record{payload: make([]byte, 128), timestamp: 1000}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		enc.reset()
		enc.encodeBatch(recs, 1, nil, false)
	}
}

// ═════════════════════════════════════════════════════════════
// 21. HOOKS — Callback Overhead
// ═════════════════════════════════════════════════════════════

func BenchmarkAppend_AllHooks(b *testing.B) {
	hooks := Hooks{
		OnStart:         func() {},
		OnShutdownStart: func() {},
		OnShutdownDone:  func(time.Duration) {},
		AfterAppend:     func(LSN, LSN, int) {},
		BeforeWrite:     func(int) {},
		AfterWrite:      func(int, time.Duration) {},
		BeforeSync:      func() {},
		AfterSync:       func(int, time.Duration) {},
		OnDrop:          func(int) {},
		OnRotation:      func(SegmentInfo) {},
		OnDelete:        func(SegmentInfo) {},
	}
	w := openBench(b, WithHooks(hooks))
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := writeOne(w, payload, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// ═════════════════════════════════════════════════════════════
// 22. INDEXER — Per-Event Callback Overhead
// ═════════════════════════════════════════════════════════════

type nopIndexer struct{}

func (nopIndexer) OnAppend(IndexInfo) {}

func BenchmarkAppend_WithIndexer(b *testing.B) {
	w := openBench(b, WithIndex(nopIndexer{}))
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := writeOne(w, payload, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchAppend_WithIndexer_100(b *testing.B) {
	w := openBench(b, WithIndex(nopIndexer{}))
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(100 * 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := NewBatch(100)
		for j := 0; j < 100; j++ {
			batch.AppendUnsafe(payload, nil, nil)
		}
		if _, err := w.WriteUnsafe(batch); err != nil {
			b.Fatal(err)
		}
	}
}

// ═════════════════════════════════════════════════════════════
// 23. OPEN/CLOSE — Lifecycle
// ═════════════════════════════════════════════════════════════

func BenchmarkOpen_EmptyDir(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dir := b.TempDir()
		w, err := Open(dir)
		if err != nil {
			b.Fatal(err)
		}
		w.Close()
	}
}

func BenchmarkOpen_WithManifest_100Segments(b *testing.B) {
	dir := b.TempDir()
	w, _ := Open(dir, WithMaxSegmentSize(1<<15))
	for i := 0; i < 10_000; i++ {
		writeOne(w, make([]byte, 128), nil, nil)
	}
	w.Shutdown(context.Background())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w2, err := Open(dir, WithMaxSegmentSize(1<<15))
		if err != nil {
			b.Fatal(err)
		}
		w2.Close()
	}
}

func BenchmarkShutdown_WithPendingEvents(b *testing.B) {
	payload := make([]byte, 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dir := b.TempDir()
		w, _ := Open(dir)
		for j := 0; j < 1000; j++ {
			writeOne(w, payload, nil, nil)
		}
		w.Shutdown(context.Background())
	}
}

// ═════════════════════════════════════════════════════════════
// 24. SEGMENTS — Stats & Info
// ═════════════════════════════════════════════════════════════

func BenchmarkStats_Read(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	seedWAL(b, w, 1000, 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = w.Stats()
	}
}

func BenchmarkSegments_List(b *testing.B) {
	w := openBench(b, WithMaxSegmentSize(1<<15))
	defer w.Shutdown(context.Background())
	seedWAL(b, w, 10_000, 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = w.Segments()
	}
}

func BenchmarkFirstLSN(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	seedWAL(b, w, 1000, 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = w.FirstLSN()
	}
}

func BenchmarkLastLSN(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	seedWAL(b, w, 1000, 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = w.LastLSN()
	}
}

// ═════════════════════════════════════════════════════════════
// 25. DELETE BEFORE — Retention
// ═════════════════════════════════════════════════════════════

func BenchmarkDeleteBefore_MultiSegment(b *testing.B) {
	payload := make([]byte, 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := b.TempDir()
		w, _ := Open(dir, WithMaxSegmentSize(1<<15))
		for j := 0; j < 10_000; j++ {
			writeOne(w, payload, nil, nil)
		}
		w.Flush()
		b.StartTimer()
		w.DeleteBefore(5000)
		b.StopTimer()
		w.Shutdown(context.Background())
	}
}

// ═════════════════════════════════════════════════════════════
// 26. END-TO-END — Write + Read Roundtrip
// ═════════════════════════════════════════════════════════════

func BenchmarkE2E_AppendThenReplay_1KEvents(b *testing.B) {
	payload := make([]byte, 256)
	b.SetBytes(int64(1000) * 256 * 2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dir := b.TempDir()
		w, _ := Open(dir)
		for j := 0; j < 1000; j++ {
			writeOne(w, payload, nil, nil)
		}
		w.Flush()
		w.Replay(0, func(ev Event) error { return nil })
		w.Shutdown(context.Background())
	}
}

func BenchmarkE2E_BatchAppendThenIterator_10KEvents(b *testing.B) {
	payload := make([]byte, 128)
	b.SetBytes(int64(10_000) * 128 * 2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dir := b.TempDir()
		w, _ := Open(dir)
		for j := 0; j < 100; j++ {
			batch := NewBatch(100)
			for k := 0; k < 100; k++ {
				batch.AppendUnsafe(payload, nil, nil)
			}
			w.WriteUnsafe(batch)
		}
		w.Flush()
		it, _ := w.Iterator(0)
		for it.Next() {
		}
		it.Close()
		w.Shutdown(context.Background())
	}
}

// ═════════════════════════════════════════════════════════════
// 27. THROUGHPUT — Sustained Write Bursts
// ═════════════════════════════════════════════════════════════

func BenchmarkThroughput_Burst_10K_128B(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(10_000 * 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 10_000; j++ {
			writeOne(w, payload, nil, nil)
		}
		w.Flush()
	}
}

func BenchmarkThroughput_Burst_10K_1KB(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	payload := make([]byte, 1024)
	b.SetBytes(10_000 * 1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 10_000; j++ {
			writeOne(w, payload, nil, nil)
		}
		w.Flush()
	}
}

func BenchmarkThroughput_BatchBurst_100Batches_100Events(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(100 * 100 * 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 100; j++ {
			batch := NewBatch(100)
			for k := 0; k < 100; k++ {
				batch.AppendUnsafe(payload, nil, nil)
			}
			w.WriteUnsafe(batch)
		}
		w.Flush()
	}
}

func BenchmarkThroughput_ParallelBurst(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(128)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			writeOne(w, payload, nil, nil)
		}
	})
	w.Flush()
}

// ═════════════════════════════════════════════════════════════
// 28. RETENTION & LIMITS — Options That Affect Hot Path
// ═════════════════════════════════════════════════════════════

func BenchmarkAppend_MaxBatchSize16KB(b *testing.B) {
	w := openBench(b, WithMaxBatchSize(16<<10))
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := writeOne(w, payload, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppend_MaxSegments5_WithRotation(b *testing.B) {
	w := openBench(b, WithMaxSegments(5), WithMaxSegmentSize(32<<10))
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := writeOne(w, payload, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppend_RetentionSize512KB(b *testing.B) {
	w := openBench(b, WithRetentionSize(512<<10), WithMaxSegmentSize(64<<10))
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	b.SetBytes(128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := writeOne(w, payload, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// ═════════════════════════════════════════════════════════════
// 29. FOLLOW — Live Tail Iterator
// ═════════════════════════════════════════════════════════════

func BenchmarkFollow_PreSeeded_10KEvents(b *testing.B) {
	w := openBench(b)
	seedWAL(b, w, 10_000, 128)
	b.SetBytes(int64(10_000) * 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it, err := w.Follow(0)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for it.Next() {
			count++
			if count >= 10_000 {
				break
			}
		}
		it.Close()
	}
	b.StopTimer()
	w.Shutdown(context.Background())
}

func BenchmarkFollow_ConcurrentWrite(b *testing.B) {
	w := openBench(b)
	defer w.Shutdown(context.Background())
	payload := make([]byte, 128)
	const eventsPerIter = 1000
	b.SetBytes(int64(eventsPerIter) * 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it, err := w.Follow(w.LastLSN() + 1)
		if err != nil {
			b.Fatal(err)
		}
		go func() {
			for j := 0; j < eventsPerIter; j++ {
				writeOne(w, payload, nil, nil)
			}
		}()
		count := 0
		for it.Next() {
			count++
			if count >= eventsPerIter {
				break
			}
		}
		it.Close()
	}
}

// ═════════════════════════════════════════════════════════════
// 30. SNAPSHOT — Consistent Read During Writes
// ═════════════════════════════════════════════════════════════

func BenchmarkSnapshot_Iterate_10KEvents(b *testing.B) {
	w := openBench(b)
	seedWAL(b, w, 10_000, 128)
	b.SetBytes(int64(10_000) * 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Snapshot(func(ctrl *SnapshotController) error {
			it, err := ctrl.Iterator()
			if err != nil {
				b.Fatal(err)
			}
			for it.Next() {
			}
			it.Close()
			return nil
		})
	}
	b.StopTimer()
	w.Shutdown(context.Background())
}

// ═════════════════════════════════════════════════════════════
// 31. IMPORT — Replication Path
// ═════════════════════════════════════════════════════════════

func BenchmarkImportBatch_100Records(b *testing.B) {
	recs := make([]record, 100)
	for i := range recs {
		recs[i] = record{payload: make([]byte, 128), timestamp: 1000}
	}
	enc := newEncoder(64 << 10)
	if err := enc.encodeBatch(recs, 1, nil, false); err != nil {
		b.Fatal(err)
	}
	frame := make([]byte, len(enc.bytes()))
	copy(frame, enc.bytes())

	b.SetBytes(int64(len(frame)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		w := openBench(b)
		b.StartTimer()
		if err := w.ImportBatch(frame); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		w.Shutdown(context.Background())
	}
}

// Ensure packages are used.
var _ = fmt.Sprint
