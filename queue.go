package uewal

import (
	"sync"
	"sync/atomic"
	"runtime"
)

// writeQueue is a bounded MPSC (Multi-Producer Single-Consumer) queue for
// transferring write batches from callers to the single writer goroutine.
//
// Producers use lock-free CAS on head; the single consumer drains from tail
// up to committed slots. A shared condvar coordinates wake-ups for both
// producer backpressure (queue full) and consumer idle (queue empty).
//
// Supports three enqueue strategies corresponding to [BackpressureMode].
type writeQueue struct {
	// Producer-side: CAS-advanced head.
	head   atomic.Uint64
	_pad1  [56]byte //lint:ignore U1000 cache-line padding to prevent false sharing

	// Consumer-side: only the single consumer advances tail.
	tail   atomic.Uint64
	_pad2  [56]byte //lint:ignore U1000 cache-line padding to prevent false sharing

	mask   uint64
	items  []queueSlot
	closed atomic.Bool

	mu   sync.Mutex
	cond *sync.Cond
}

type queueSlot struct {
	batch     writeBatch
	committed atomic.Bool
	_pad      [55]byte //lint:ignore U1000 cache-line padding to prevent false sharing
}

// writeBatch is a single unit of work sent from Append to the writer goroutine.
type writeBatch struct {
	records     []record
	recordPool  *[]record
	noCompress  bool
	tsUniform   bool // hint: all records share the same timestamp
	payloadOnly bool // hint: all records have empty key and meta
	lsnStart    LSN
	lsnEnd      LSN
	barrier     chan struct{}
	rotate      bool   // triggers segment rotation inside writer goroutine
	importFrame []byte // raw batch frame for ImportBatch
}

func nextPowerOf2(n int) int {
	if n <= 0 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n++
	return n
}

func newWriteQueue(capacity int) *writeQueue {
	cap2 := nextPowerOf2(capacity)
	q := &writeQueue{
		items: make([]queueSlot, cap2),
		mask:  uint64(cap2 - 1),
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *writeQueue) enqueue(b writeBatch) bool {
	for {
		if q.closed.Load() {
			return false
		}
		head := q.head.Load()
		tail := q.tail.Load()

		if head-tail > q.mask {
			q.mu.Lock()
			for {
				if q.closed.Load() {
					q.mu.Unlock()
					return false
				}
				h := q.head.Load()
				t := q.tail.Load()
				if h-t <= q.mask {
					break
				}
				q.cond.Wait()
			}
			q.mu.Unlock()
			continue
		}

		if q.head.CompareAndSwap(head, head+1) {
			slot := &q.items[head&q.mask]
			slot.batch = b
			slot.committed.Store(true)
			q.cond.Broadcast()
			return true
		}
		runtime.Gosched()
	}
}

func (q *writeQueue) tryEnqueue(b writeBatch) bool {
	for {
		if q.closed.Load() {
			return false
		}
		head := q.head.Load()
		tail := q.tail.Load()
		if head-tail > q.mask {
			return false
		}
		if q.head.CompareAndSwap(head, head+1) {
			slot := &q.items[head&q.mask]
			slot.batch = b
			slot.committed.Store(true)
			q.cond.Broadcast()
			return true
		}
		runtime.Gosched()
	}
}

func (q *writeQueue) dequeueAllInto(buf []writeBatch) ([]writeBatch, bool) {
	for {
		tail := q.tail.Load()
		head := q.head.Load()

		if tail == head {
			q.mu.Lock()
			for {
				if q.closed.Load() {
					t := q.tail.Load()
					h := q.head.Load()
					if t == h {
						q.mu.Unlock()
						return buf, false
					}
					break
				}
				t := q.tail.Load()
				h := q.head.Load()
				if t < h {
					break
				}
				q.cond.Wait()
			}
			q.mu.Unlock()
			continue
		}

		drained := false
		for tail < head {
			slot := &q.items[tail&q.mask]
			if !slot.committed.Load() {
				for i := 0; i < 64; i++ {
					if slot.committed.Load() {
						break
					}
					runtime.Gosched()
				}
				if !slot.committed.Load() {
					break
				}
			}
			buf = append(buf, slot.batch)
			slot.batch = writeBatch{}
			slot.committed.Store(false)
			tail++
			drained = true
		}

		if drained {
			q.mu.Lock()
			q.tail.Store(tail)
			q.cond.Broadcast()
			q.mu.Unlock()
			return buf, true
		}

		runtime.Gosched()
	}
}

func (q *writeQueue) close() {
	q.closed.Store(true)
	q.cond.Broadcast()
}

func (q *writeQueue) size() int {
	head := q.head.Load()
	tail := q.tail.Load()
	if head >= tail {
		return int(head - tail)
	}
	return 0
}
