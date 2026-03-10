package uewal

import (
	"context"
	"path/filepath"
	"testing"
)

func BenchmarkAppendAsync(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench_async.wal")
	w, err := Open(path, WithSyncMode(SyncNever), WithQueueSize(16384))
	if err != nil {
		b.Fatal(err)
	}
	defer w.Shutdown(context.Background())

	payload := make([]byte, 128)
	b.SetBytes(int64(batchFrameSize([]Event{{Payload: payload}})))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w.Append(Event{Payload: payload})
	}

	b.StopTimer()
}

func BenchmarkAppendDurable(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench_durable.wal")
	w, err := Open(path, WithSyncMode(SyncBatch), WithQueueSize(16384))
	if err != nil {
		b.Fatal(err)
	}
	defer w.Shutdown(context.Background())

	payload := make([]byte, 128)
	b.SetBytes(int64(batchFrameSize([]Event{{Payload: payload}})))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w.Append(Event{Payload: payload})
	}

	b.StopTimer()
}

func BenchmarkAppendBatch10(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench_b10.wal")
	w, err := Open(path, WithSyncMode(SyncNever), WithQueueSize(16384))
	if err != nil {
		b.Fatal(err)
	}
	defer w.Shutdown(context.Background())

	payload := make([]byte, 128)
	batch := NewBatch(10)
	for i := 0; i < 10; i++ {
		batch.Add(payload)
	}
	b.SetBytes(int64(batchFrameSize(batch.Events)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w.AppendBatch(batch)
	}

	b.StopTimer()
}

func BenchmarkAppendBatch100(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench_b100.wal")
	w, err := Open(path, WithSyncMode(SyncNever), WithQueueSize(16384))
	if err != nil {
		b.Fatal(err)
	}
	defer w.Shutdown(context.Background())

	payload := make([]byte, 128)
	batch := NewBatch(100)
	for i := 0; i < 100; i++ {
		batch.Add(payload)
	}
	b.SetBytes(int64(batchFrameSize(batch.Events)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w.AppendBatch(batch)
	}

	b.StopTimer()
}

func BenchmarkAppendParallel(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench_parallel.wal")
	w, err := Open(path, WithSyncMode(SyncNever), WithQueueSize(32768))
	if err != nil {
		b.Fatal(err)
	}
	defer w.Shutdown(context.Background())

	payload := make([]byte, 128)
	b.SetBytes(int64(batchFrameSize([]Event{{Payload: payload}})))
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			w.Append(Event{Payload: payload})
		}
	})

	b.StopTimer()
}

func BenchmarkFlush(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench_flush.wal")
	w, err := Open(path, WithSyncMode(SyncNever), WithQueueSize(16384))
	if err != nil {
		b.Fatal(err)
	}
	defer w.Shutdown(context.Background())

	payload := make([]byte, 128)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w.Append(Event{Payload: payload})
		w.Flush()
	}

	b.StopTimer()
}

func BenchmarkFlushSync(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench_flush_sync.wal")
	w, err := Open(path, WithSyncMode(SyncNever), WithQueueSize(16384))
	if err != nil {
		b.Fatal(err)
	}
	defer w.Shutdown(context.Background())

	payload := make([]byte, 128)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w.Append(Event{Payload: payload})
		w.Flush()
		w.Sync()
	}

	b.StopTimer()
}

const replayEvents = 100_000
const replayPayloadSize = 256

func prepareReplayFile(b *testing.B, path string) {
	b.Helper()
	w, err := Open(path, WithSyncMode(SyncBatch), WithQueueSize(16384))
	if err != nil {
		b.Fatal(err)
	}

	payload := make([]byte, replayPayloadSize)
	batch := NewBatch(100)
	for i := 0; i < 100; i++ {
		batch.Add(payload)
	}
	for i := 0; i < replayEvents/100; i++ {
		w.AppendBatch(batch)
	}
	w.Shutdown(context.Background())
}

func BenchmarkReplay(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench_replay.wal")
	prepareReplayFile(b, path)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w, err := Open(path)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		w.Replay(0, func(Event) error {
			count++
			return nil
		})
		if count != replayEvents {
			b.Fatalf("replayed %d, want %d", count, replayEvents)
		}
		w.Shutdown(context.Background())
	}
}

func BenchmarkIterator(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench_iter.wal")
	prepareReplayFile(b, path)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w, err := Open(path)
		if err != nil {
			b.Fatal(err)
		}
		it, err := w.Iterator(0)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for it.Next() {
			count++
		}
		it.Close()
		if count != replayEvents {
			b.Fatalf("iterated %d, want %d", count, replayEvents)
		}
		w.Shutdown(context.Background())
	}
}

func BenchmarkEncodeBatch(b *testing.B) {
	payload := make([]byte, 128)
	events := make([]Event, 10)
	for i := range events {
		events[i].Payload = payload
	}
	enc := newEncoder(batchFrameSize(events) * 2)
	b.SetBytes(int64(batchFrameSize(events)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		enc.encodeBatch(events, 1, nil)
		enc.reset()
	}
}

func BenchmarkDecodeBatch(b *testing.B) {
	payload := make([]byte, 128)
	events := make([]Event, 10)
	for i := range events {
		events[i].Payload = payload
	}
	buf := make([]byte, batchFrameSize(events)*2)
	frame, n, _ := encodeBatchFrame(buf, events, 1, nil)
	data := frame[:n]
	b.SetBytes(int64(n))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		decodeBatchFrame(data, 0, nil)
	}
}

func BenchmarkRecovery(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench_recovery.wal")
	prepareReplayFile(b, path)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w, err := Open(path)
		if err != nil {
			b.Fatal(err)
		}
		if w.LastLSN() != replayEvents {
			b.Fatalf("LastLSN=%d, want %d", w.LastLSN(), replayEvents)
		}
		w.Shutdown(context.Background())
	}
}

func BenchmarkScanBatchHeader(b *testing.B) {
	payload := make([]byte, 128)
	events := make([]Event, 10)
	for i := range events {
		events[i].Payload = payload
	}
	buf := make([]byte, batchFrameSize(events)*2)
	frame, n, _ := encodeBatchFrame(buf, events, 1, nil)
	data := frame[:n]
	b.SetBytes(int64(n))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		scanBatchHeader(data, 0)
	}
}

func BenchmarkDecodeBatchInto(b *testing.B) {
	payload := make([]byte, 128)
	events := make([]Event, 10)
	for i := range events {
		events[i].Payload = payload
	}
	buf := make([]byte, batchFrameSize(events)*2)
	frame, n, _ := encodeBatchFrame(buf, events, 1, nil)
	data := frame[:n]
	decodeBuf := make([]Event, 0, 16)
	b.SetBytes(int64(n))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		decodeBatchFrameInto(data, 0, nil, decodeBuf[:0])
	}
}
