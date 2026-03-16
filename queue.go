package uewal

import "sync"

// writeQueue is a bounded, blocking FIFO queue for transferring write
// batches from callers to the single writer goroutine. Supports three
// enqueue strategies corresponding to [BackpressureMode].
type writeQueue struct {
	mu       sync.Mutex
	notEmpty *sync.Cond
	notFull  *sync.Cond
	items    []writeBatch
	head     int
	tail     int
	count    int
	cap      int
	closed   bool
}

// writeBatch is a single unit of work sent from Append to the writer goroutine.
// Special batches: barrier-only (Flush), rotate flag (manual Rotate),
// importFrame (ImportBatch — raw frame written directly).
type writeBatch struct {
	records     []record
	recordPool  *[]record
	noCompress  bool
	lsnStart    LSN
	lsnEnd      LSN
	barrier     chan struct{}
	rotate      bool   // triggers segment rotation inside writer goroutine
	importFrame []byte // raw batch frame for ImportBatch
}

func newWriteQueue(capacity int) *writeQueue {
	q := &writeQueue{
		items: make([]writeBatch, capacity),
		cap:   capacity,
	}
	q.notEmpty = sync.NewCond(&q.mu)
	q.notFull = sync.NewCond(&q.mu)
	return q
}

func (q *writeQueue) enqueue(b writeBatch) bool {
	q.mu.Lock()
	for q.count == q.cap && !q.closed {
		q.notFull.Wait()
	}
	if q.closed {
		q.mu.Unlock()
		return false
	}
	q.items[q.tail] = b
	q.tail = (q.tail + 1) % q.cap
	q.count++
	q.notEmpty.Signal()
	q.mu.Unlock()
	return true
}

func (q *writeQueue) tryEnqueue(b writeBatch) bool {
	q.mu.Lock()
	if q.closed || q.count == q.cap {
		q.mu.Unlock()
		return false
	}
	q.items[q.tail] = b
	q.tail = (q.tail + 1) % q.cap
	q.count++
	q.notEmpty.Signal()
	q.mu.Unlock()
	return true
}

func (q *writeQueue) dequeueAllInto(buf []writeBatch) ([]writeBatch, bool) {
	q.mu.Lock()
	for q.count == 0 && !q.closed {
		q.notEmpty.Wait()
	}
	if q.count == 0 {
		q.mu.Unlock()
		return buf, false
	}
	for q.count > 0 {
		buf = append(buf, q.items[q.head])
		q.items[q.head] = writeBatch{}
		q.head = (q.head + 1) % q.cap
		q.count--
	}
	q.notFull.Broadcast()
	q.mu.Unlock()
	return buf, true
}

func (q *writeQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.notEmpty.Broadcast()
	q.notFull.Broadcast()
}

func (q *writeQueue) size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.count
}
