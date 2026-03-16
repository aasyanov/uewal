package uewal

import (
	"sync"
	"testing"
)

func TestWriteQueue_EnqueueDequeue(t *testing.T) {
	q := newWriteQueue(10)
	defer q.close()

	b1 := writeBatch{lsnStart: 1, lsnEnd: 1}
	b2 := writeBatch{lsnStart: 2, lsnEnd: 2}
	b3 := writeBatch{lsnStart: 3, lsnEnd: 3}

	if !q.enqueue(b1) || !q.enqueue(b2) || !q.enqueue(b3) {
		t.Fatal("enqueue failed")
	}

	var buf []writeBatch
	buf, ok := q.dequeueAllInto(buf)
	if !ok {
		t.Fatal("dequeueAllInto returned false")
	}
	if len(buf) != 3 {
		t.Fatalf("got %d items, want 3", len(buf))
	}
	if buf[0].lsnStart != 1 || buf[1].lsnStart != 2 || buf[2].lsnStart != 3 {
		t.Fatalf("wrong order: %v %v %v", buf[0].lsnStart, buf[1].lsnStart, buf[2].lsnStart)
	}
}

func TestWriteQueue_BlockMode(t *testing.T) {
	q := newWriteQueue(2)
	defer q.close()

	b1 := writeBatch{lsnStart: 1, lsnEnd: 1}
	b2 := writeBatch{lsnStart: 2, lsnEnd: 2}
	b3 := writeBatch{lsnStart: 3, lsnEnd: 3}

	q.enqueue(b1)
	q.enqueue(b2)

	var enqueued sync.WaitGroup
	enqueued.Add(1)
	go func() {
		ok := q.enqueue(b3)
		if !ok {
			t.Error("enqueue b3 returned false")
		}
		enqueued.Done()
	}()

	var buf []writeBatch
	buf, _ = q.dequeueAllInto(buf)
	if len(buf) != 2 {
		t.Fatalf("first drain: got %d, want 2", len(buf))
	}
	enqueued.Wait()

	buf, ok := q.dequeueAllInto(buf[:0])
	if !ok || len(buf) != 1 {
		t.Fatalf("second drain: ok=%v len=%d, want ok=true len=1", ok, len(buf))
	}
	if buf[0].lsnStart != 3 {
		t.Fatalf("got lsnStart %d, want 3", buf[0].lsnStart)
	}
}

func TestWriteQueue_TryEnqueue(t *testing.T) {
	q := newWriteQueue(2)
	defer q.close()

	b1 := writeBatch{lsnStart: 1, lsnEnd: 1}
	b2 := writeBatch{lsnStart: 2, lsnEnd: 2}
	b3 := writeBatch{lsnStart: 3, lsnEnd: 3}

	if !q.tryEnqueue(b1) || !q.tryEnqueue(b2) {
		t.Fatal("tryEnqueue b1/b2 failed")
	}
	if q.tryEnqueue(b3) {
		t.Fatal("tryEnqueue b3 should fail when full")
	}

	var buf []writeBatch
	buf, _ = q.dequeueAllInto(buf)
	if len(buf) != 2 {
		t.Fatalf("got %d items, want 2", len(buf))
	}
}

func TestWriteQueue_Close(t *testing.T) {
	q := newWriteQueue(2)
	q.enqueue(writeBatch{lsnStart: 1, lsnEnd: 1})
	q.enqueue(writeBatch{lsnStart: 2, lsnEnd: 2})

	blocked := make(chan struct{})
	go func() {
		ok := q.enqueue(writeBatch{lsnStart: 3, lsnEnd: 3})
		if ok {
			t.Error("enqueue after close should return false")
		}
		close(blocked)
	}()

	q.close()
	<-blocked

	var buf []writeBatch
	buf, ok := q.dequeueAllInto(buf)
	if !ok || len(buf) != 2 {
		t.Fatalf("drain: ok=%v len=%d, want ok=true len=2", ok, len(buf))
	}

	buf, ok = q.dequeueAllInto(buf[:0])
	if ok {
		t.Fatal("dequeueAllInto on empty closed queue should return false")
	}
}

func TestWriteQueue_Size(t *testing.T) {
	q := newWriteQueue(5)
	defer q.close()

	if q.size() != 0 {
		t.Fatalf("empty size: got %d, want 0", q.size())
	}

	q.enqueue(writeBatch{lsnStart: 1, lsnEnd: 1})
	q.enqueue(writeBatch{lsnStart: 2, lsnEnd: 2})
	if q.size() != 2 {
		t.Fatalf("size after 2 enqueue: got %d, want 2", q.size())
	}

	var buf []writeBatch
	_, _ = q.dequeueAllInto(buf)
	if q.size() != 0 {
		t.Fatalf("size after drain: got %d, want 0", q.size())
	}
}

func TestWriteQueue_DrainOrder(t *testing.T) {
	q := newWriteQueue(10)
	defer q.close()

	for i := LSN(1); i <= 5; i++ {
		q.enqueue(writeBatch{lsnStart: i, lsnEnd: i})
	}

	var buf []writeBatch
	buf, ok := q.dequeueAllInto(buf)
	if !ok || len(buf) != 5 {
		t.Fatalf("got ok=%v len=%d, want ok=true len=5", ok, len(buf))
	}
	for i := range buf {
		if buf[i].lsnStart != LSN(i+1) {
			t.Fatalf("buf[%d].lsnStart=%d, want %d", i, buf[i].lsnStart, i+1)
		}
	}
}
