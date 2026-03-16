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

// --- Sparse index benchmarks ---

func BenchmarkSparse_FindByLSN(b *testing.B) {
	si := &sparseIndex{}
	for i := 0; i < 10000; i++ {
		si.append(sparseEntry{
			FirstLSN:  LSN(i * 100),
			Offset:    int64(i * 4096),
			Timestamp: int64(i * 1000),
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		si.findByLSN(LSN(500000))
	}
}

func BenchmarkSparse_FindByTimestamp(b *testing.B) {
	si := &sparseIndex{}
	for i := 0; i < 10000; i++ {
		si.append(sparseEntry{
			FirstLSN:  LSN(i * 100),
			Offset:    int64(i * 4096),
			Timestamp: int64(i * 1000),
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		si.findByTimestamp(5000000)
	}
}

func BenchmarkSparse_MarshalUnmarshal(b *testing.B) {
	si := &sparseIndex{}
	for i := 0; i < 1000; i++ {
		si.append(sparseEntry{
			FirstLSN:  LSN(i * 100),
			Offset:    int64(i * 4096),
			Timestamp: int64(i),
		})
	}
	b.SetBytes(int64(1000 * sparseEntrySize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		data := si.marshal()
		_, err := unmarshalSparseIndex(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// --- Queue benchmarks ---

func BenchmarkQueue_EnqueueDequeue(b *testing.B) {
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

func BenchmarkQueue_TryEnqueue(b *testing.B) {
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

// --- Durable notifier benchmarks ---

func BenchmarkDurable_AdvanceNoWaiters(b *testing.B) {
	d := &durableNotifier{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.advance(LSN(i))
	}
}

func BenchmarkDurable_WaitAlreadySynced(b *testing.B) {
	d := &durableNotifier{}
	d.advance(1000000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.wait(1)
	}
}

// --- Encoding detail benchmarks ---

func BenchmarkEncodeRecordsRegion(b *testing.B) {
	recs := make([]record, 100)
	for i := range recs {
		recs[i] = record{payload: make([]byte, 128), timestamp: 1000}
	}
	size := recordsRegionSize(recs, false)
	dst := make([]byte, size)
	b.SetBytes(int64(size))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		encodeRecordsRegion(dst, recs, false)
	}
}

func BenchmarkRecordSlicePool(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, sp := getRecordSlice(8)
		putRecordSlice(sp, s)
	}
}

// --- Manifest benchmarks ---

func BenchmarkManifest_Marshal(b *testing.B) {
	m := &manifest{lastLSN: 100000}
	for i := 0; i < 100; i++ {
		m.entries = append(m.entries, manifestEntry{
			firstLSN: LSN(i * 1000), lastLSN: LSN(i*1000 + 999),
			size: 1 << 20, sealed: true,
		})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.marshal()
	}
}

func BenchmarkManifest_Unmarshal(b *testing.B) {
	m := &manifest{lastLSN: 100000}
	for i := 0; i < 100; i++ {
		m.entries = append(m.entries, manifestEntry{
			firstLSN: LSN(i * 1000), lastLSN: LSN(i*1000 + 999),
			size: 1 << 20, sealed: true,
		})
	}
	data := m.marshal()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := unmarshalManifest(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

var _ = fmt.Sprint
