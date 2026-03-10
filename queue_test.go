package uewal

import (
	"sync"
	"testing"
	"time"
)

func TestQueueEnqueueDequeue(t *testing.T) {
	q := newWriteQueue(4)

	for i := 0; i < 4; i++ {
		if !q.enqueue(writeBatch{lsnEnd: LSN(i + 1)}) {
			t.Fatalf("enqueue %d failed", i)
		}
	}
	if q.size() != 4 {
		t.Fatalf("size=%d, want 4", q.size())
	}

	for i := 0; i < 4; i++ {
		b, ok := q.dequeue()
		if !ok {
			t.Fatalf("dequeue %d failed", i)
		}
		if b.lsnEnd != LSN(i+1) {
			t.Fatalf("dequeue %d: lsn=%d, want %d", i, b.lsnEnd, i+1)
		}
	}
	if q.size() != 0 {
		t.Fatalf("size=%d, want 0", q.size())
	}
}

func TestQueueTryEnqueue(t *testing.T) {
	q := newWriteQueue(2)

	if !q.tryEnqueue(writeBatch{lsnEnd: 1}) {
		t.Fatal("first tryEnqueue failed")
	}
	if !q.tryEnqueue(writeBatch{lsnEnd: 2}) {
		t.Fatal("second tryEnqueue failed")
	}
	if q.tryEnqueue(writeBatch{lsnEnd: 3}) {
		t.Fatal("third tryEnqueue should fail (full)")
	}
}

func TestQueueBlockingEnqueue(t *testing.T) {
	q := newWriteQueue(1)
	q.enqueue(writeBatch{lsnEnd: 1})

	done := make(chan bool, 1)
	go func() {
		q.enqueue(writeBatch{lsnEnd: 2})
		done <- true
	}()

	select {
	case <-done:
		t.Fatal("enqueue should block when full")
	case <-time.After(50 * time.Millisecond):
	}

	q.dequeue()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("enqueue should unblock after dequeue")
	}
}

func TestQueueBlockingDequeue(t *testing.T) {
	q := newWriteQueue(4)

	done := make(chan writeBatch, 1)
	go func() {
		b, ok := q.dequeue()
		if ok {
			done <- b
		}
	}()

	select {
	case <-done:
		t.Fatal("dequeue should block when empty")
	case <-time.After(50 * time.Millisecond):
	}

	q.enqueue(writeBatch{lsnEnd: 42})

	select {
	case b := <-done:
		if b.lsnEnd != 42 {
			t.Fatalf("lsn=%d, want 42", b.lsnEnd)
		}
	case <-time.After(time.Second):
		t.Fatal("dequeue should unblock after enqueue")
	}
}

func TestQueueClose(t *testing.T) {
	q := newWriteQueue(4)
	q.enqueue(writeBatch{lsnEnd: 1})
	q.close()

	if q.enqueue(writeBatch{lsnEnd: 2}) {
		t.Fatal("enqueue after close should return false")
	}
	if q.tryEnqueue(writeBatch{lsnEnd: 3}) {
		t.Fatal("tryEnqueue after close should return false")
	}

	b, ok := q.dequeue()
	if !ok || b.lsnEnd != 1 {
		t.Fatal("should drain existing item after close")
	}

	_, ok = q.dequeue()
	if ok {
		t.Fatal("dequeue on closed empty queue should return false")
	}
}

func TestQueueCloseUnblocksWaiters(t *testing.T) {
	q := newWriteQueue(4)

	done := make(chan bool, 1)
	go func() {
		_, ok := q.dequeue()
		done <- ok
	}()

	time.Sleep(50 * time.Millisecond)
	q.close()

	select {
	case ok := <-done:
		if ok {
			t.Fatal("dequeue should return false after close on empty queue")
		}
	case <-time.After(time.Second):
		t.Fatal("close should unblock dequeue")
	}
}

func TestQueueDrainAll(t *testing.T) {
	q := newWriteQueue(8)
	for i := 0; i < 5; i++ {
		q.enqueue(writeBatch{lsnEnd: LSN(i + 1)})
	}

	batches := q.drainAll()
	if len(batches) != 5 {
		t.Fatalf("drained %d batches, want 5", len(batches))
	}
	for i, b := range batches {
		if b.lsnEnd != LSN(i+1) {
			t.Errorf("batch[%d].lsnEnd=%d, want %d", i, b.lsnEnd, i+1)
		}
	}
	if q.size() != 0 {
		t.Fatalf("size=%d after drain, want 0", q.size())
	}
}

func TestQueueDrainAllEmpty(t *testing.T) {
	q := newWriteQueue(4)
	batches := q.drainAll()
	if batches != nil {
		t.Fatalf("drain empty queue returned %v, want nil", batches)
	}
}

func TestQueueWraparound(t *testing.T) {
	q := newWriteQueue(3)

	for cycle := 0; cycle < 5; cycle++ {
		for i := 0; i < 3; i++ {
			lsn := LSN(cycle*3 + i + 1)
			if !q.enqueue(writeBatch{lsnEnd: lsn}) {
				t.Fatalf("cycle %d, enqueue %d failed", cycle, i)
			}
		}
		for i := 0; i < 3; i++ {
			b, ok := q.dequeue()
			if !ok {
				t.Fatalf("cycle %d, dequeue %d failed", cycle, i)
			}
			expected := LSN(cycle*3 + i + 1)
			if b.lsnEnd != expected {
				t.Fatalf("cycle %d, dequeue %d: lsn=%d, want %d", cycle, i, b.lsnEnd, expected)
			}
		}
	}
}

func TestQueueConcurrentProducerConsumer(t *testing.T) {
	q := newWriteQueue(64)
	const total = 10000

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < total; i++ {
			if !q.enqueue(writeBatch{lsnEnd: LSN(i + 1)}) {
				t.Errorf("enqueue %d failed", i)
				return
			}
		}
		q.close()
	}()

	received := make([]bool, total)
	go func() {
		defer wg.Done()
		for {
			b, ok := q.dequeue()
			if !ok {
				return
			}
			idx := int(b.lsnEnd) - 1
			if idx < 0 || idx >= total {
				t.Errorf("unexpected lsn %d", b.lsnEnd)
				return
			}
			received[idx] = true
		}
	}()

	wg.Wait()

	for i, got := range received {
		if !got {
			t.Errorf("missing event %d", i+1)
		}
	}
}

func TestQueueMultipleProducers(t *testing.T) {
	q := newWriteQueue(64)
	const producers = 4
	const perProducer = 1000

	var wg sync.WaitGroup
	wg.Add(producers + 1)

	for p := 0; p < producers; p++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				lsn := LSN(id*perProducer + i + 1)
				q.enqueue(writeBatch{lsnEnd: lsn})
			}
		}(p)
	}

	count := 0
	go func() {
		defer wg.Done()
		for {
			_, ok := q.dequeue()
			if !ok {
				return
			}
			count++
			if count == producers*perProducer {
				q.close()
				return
			}
		}
	}()

	wg.Wait()

	if count != producers*perProducer {
		t.Fatalf("received %d items, want %d", count, producers*perProducer)
	}
}

func TestQueueDrainAllIntoReuseBuffer(t *testing.T) {
	q := newWriteQueue(8)

	// Round 1
	for i := 0; i < 3; i++ {
		q.enqueue(writeBatch{lsnEnd: LSN(i + 1)})
	}
	buf := make([]writeBatch, 0, 8)
	buf = q.drainAllInto(buf)
	if len(buf) != 3 {
		t.Fatalf("round 1: drained %d, want 3", len(buf))
	}

	// Round 2: reuse the same buffer
	for i := 0; i < 5; i++ {
		q.enqueue(writeBatch{lsnEnd: LSN(i + 10)})
	}
	buf = q.drainAllInto(buf[:0])
	if len(buf) != 5 {
		t.Fatalf("round 2: drained %d, want 5", len(buf))
	}
	// Verify cap didn't change (no reallocation).
	if cap(buf) != 8 {
		t.Fatalf("cap=%d, want 8 (should reuse)", cap(buf))
	}
}

func TestQueueDrainAllIntoEmpty(t *testing.T) {
	q := newWriteQueue(4)
	buf := make([]writeBatch, 0, 4)
	buf = q.drainAllInto(buf)
	if len(buf) != 0 {
		t.Fatalf("drain empty: got %d items", len(buf))
	}
}

func TestQueueDequeueAllInto(t *testing.T) {
	q := newWriteQueue(8)
	for i := 0; i < 5; i++ {
		q.enqueue(writeBatch{lsnEnd: LSN(i + 1)})
	}

	buf := make([]writeBatch, 0, 8)
	buf, ok := q.dequeueAllInto(buf)
	if !ok {
		t.Fatal("dequeueAllInto returned false on non-empty queue")
	}
	if len(buf) != 5 {
		t.Fatalf("drained %d, want 5", len(buf))
	}
	for i, b := range buf {
		if b.lsnEnd != LSN(i+1) {
			t.Errorf("buf[%d].lsnEnd=%d, want %d", i, b.lsnEnd, i+1)
		}
	}
	if q.size() != 0 {
		t.Fatalf("size=%d, want 0", q.size())
	}
}

func TestQueueDequeueAllIntoBlocks(t *testing.T) {
	q := newWriteQueue(4)

	done := make(chan []writeBatch, 1)
	go func() {
		buf := make([]writeBatch, 0, 4)
		buf, ok := q.dequeueAllInto(buf)
		if ok {
			done <- buf
		}
	}()

	select {
	case <-done:
		t.Fatal("dequeueAllInto should block on empty queue")
	case <-time.After(50 * time.Millisecond):
	}

	q.enqueue(writeBatch{lsnEnd: 1})
	q.enqueue(writeBatch{lsnEnd: 2})
	time.Sleep(10 * time.Millisecond)

	select {
	case buf := <-done:
		if len(buf) < 1 {
			t.Fatal("expected at least 1 item")
		}
	case <-time.After(time.Second):
		t.Fatal("dequeueAllInto should unblock after enqueue")
	}
}

func TestQueueDequeueAllIntoClosedEmpty(t *testing.T) {
	q := newWriteQueue(4)
	q.close()

	buf := make([]writeBatch, 0, 4)
	buf, ok := q.dequeueAllInto(buf)
	if ok {
		t.Fatal("dequeueAllInto should return false on closed empty queue")
	}
	if len(buf) != 0 {
		t.Fatalf("buf should be empty, got %d", len(buf))
	}
}

func TestQueueDequeueAllIntoClosedWithItems(t *testing.T) {
	q := newWriteQueue(4)
	q.enqueue(writeBatch{lsnEnd: 1})
	q.enqueue(writeBatch{lsnEnd: 2})
	q.close()

	buf := make([]writeBatch, 0, 4)
	buf, ok := q.dequeueAllInto(buf)
	if !ok {
		t.Fatal("dequeueAllInto should return true when closed with items")
	}
	if len(buf) != 2 {
		t.Fatalf("drained %d, want 2", len(buf))
	}

	_, ok = q.dequeueAllInto(buf[:0])
	if ok {
		t.Fatal("second dequeueAllInto should return false (closed+empty)")
	}
}

func TestQueueCloseIdempotent(t *testing.T) {
	q := newWriteQueue(4)
	q.enqueue(writeBatch{lsnEnd: 1})

	q.close()
	q.close() // second close must not panic or deadlock
	q.close() // third close for good measure

	// Should still be able to drain existing item.
	b, ok := q.dequeue()
	if !ok || b.lsnEnd != 1 {
		t.Fatal("expected to drain existing item after double close")
	}

	_, ok = q.dequeue()
	if ok {
		t.Fatal("dequeue on closed empty queue should return false")
	}
}
