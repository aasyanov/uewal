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
// up to committed slots. A notify channel replaces condvar for wake-up.
//
// Supports three enqueue strategies corresponding to [BackpressureMode].
type writeQueue struct {
	// Producer-side: CAS-advanced head.
	head   atomic.Uint64
	_pad1  [56]byte //nolint:unused // cache-line padding

	// Consumer-side: only the single consumer advances tail.
	tail   atomic.Uint64
	_pad2  [56]byte //nolint:unused // cache-line padding

	mask   uint64
	items  []queueSlot
	closed atomic.Bool

	// notify wakes the consumer when items are available.
	notify chan struct{}

	// blockMu + blockCond used only for BlockMode when queue is full.
	blockMu   sync.Mutex
	blockCond *sync.Cond
}

type queueSlot struct {
	batch     writeBatch
	committed atomic.Bool
	_pad      [55]byte //nolint:unused // prevent false sharing between slots
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
		items:  make([]queueSlot, cap2),
		mask:   uint64(cap2 - 1),
		notify: make(chan struct{}, 1),
	}
	q.blockCond = sync.NewCond(&q.blockMu)
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
			q.blockMu.Lock()
			for {
				if q.closed.Load() {
					q.blockMu.Unlock()
					return false
				}
				h := q.head.Load()
				t := q.tail.Load()
				if h-t <= q.mask {
					break
				}
				q.blockCond.Wait()
			}
			q.blockMu.Unlock()
			// After waking, the consumer may have already re-entered
			// dequeueAllInto and be blocking on <-notify with a stale
			// empty-queue snapshot. Re-send a notify so it re-checks.
			q.notifyConsumer()
			continue
		}

		if q.head.CompareAndSwap(head, head+1) {
			slot := &q.items[head&q.mask]
			slot.batch = b
			slot.committed.Store(true)

			q.notifyConsumer()
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
			q.notifyConsumer()
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
			if q.closed.Load() {
				return buf, false
			}
			<-q.notify
			if q.closed.Load() {
				tail = q.tail.Load()
				head = q.head.Load()
				if tail == head {
					return buf, false
				}
			}
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
			q.tail.Store(tail)
			q.blockCond.Broadcast()
			return buf, true
		}

		// Slots reserved (head > tail) but none committed yet.
		// Yield and retry — producers will commit shortly.
		runtime.Gosched()
	}
}

// notifyConsumer ensures the consumer goroutine will wake up from
// <-q.notify. Called by enqueue after blockCond wakeup to close
// the timing gap where consumer may block on an empty channel.
func (q *writeQueue) notifyConsumer() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

func (q *writeQueue) close() {
	q.closed.Store(true)
	q.blockCond.Broadcast()
	q.notifyConsumer()
}

func (q *writeQueue) size() int {
	head := q.head.Load()
	tail := q.tail.Load()
	if head >= tail {
		return int(head - tail)
	}
	return 0
}
