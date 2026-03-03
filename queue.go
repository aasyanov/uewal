package uewal

import "sync"

// writeQueue is a bounded, blocking FIFO queue used to transfer write
// batches from [WAL.Append] callers to the single writer goroutine.
//
// It is implemented as a circular buffer with condition variables for
// blocking on full (producers) and empty (consumer). The queue supports
// three enqueue strategies corresponding to [BackpressureMode]:
//   - [enqueue]: blocks until space is available (BlockMode).
//   - [tryEnqueue]: returns false immediately if full (DropMode, ErrorMode).
//
// The queue is closed during shutdown to signal the writer to drain and exit.
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
type writeBatch struct {
	events   []Event
	lsnStart LSN
	lsnEnd   LSN
	// barrier, when non-nil, is closed by the writer after this batch
	// (and all prior batches) have been written to storage. Used by
	// [WAL.Flush] to implement a synchronous drain barrier.
	barrier chan struct{}
}

// newWriteQueue creates a queue with the given capacity (number of batches).
func newWriteQueue(capacity int) *writeQueue {
	q := &writeQueue{
		items: make([]writeBatch, capacity),
		cap:   capacity,
	}
	q.notEmpty = sync.NewCond(&q.mu)
	q.notFull = sync.NewCond(&q.mu)
	return q
}

// enqueue adds a batch to the queue, blocking if full ([BlockMode]).
// Returns false if the queue has been closed.
func (q *writeQueue) enqueue(b writeBatch) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.count == q.cap && !q.closed {
		q.notFull.Wait()
	}
	if q.closed {
		return false
	}
	q.items[q.tail] = b
	q.tail = (q.tail + 1) % q.cap
	q.count++
	q.notEmpty.Signal()
	return true
}

// tryEnqueue attempts to add a batch without blocking.
// Returns false if the queue is full or closed.
func (q *writeQueue) tryEnqueue(b writeBatch) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || q.count == q.cap {
		return false
	}
	q.items[q.tail] = b
	q.tail = (q.tail + 1) % q.cap
	q.count++
	q.notEmpty.Signal()
	return true
}

// dequeue removes and returns the next batch, blocking until one is
// available. Returns ok=false only when the queue is closed AND empty
// (all items have been drained).
func (q *writeQueue) dequeue() (writeBatch, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.count == 0 && !q.closed {
		q.notEmpty.Wait()
	}
	if q.count == 0 {
		return writeBatch{}, false
	}
	b := q.items[q.head]
	q.items[q.head] = writeBatch{}
	q.head = (q.head + 1) % q.cap
	q.count--
	q.notFull.Signal()
	return b, true
}

// drainAll removes and returns all items currently in the queue as a
// new slice. Non-blocking: returns nil immediately if the queue is empty.
func (q *writeQueue) drainAll() []writeBatch {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.count == 0 {
		return nil
	}
	result := make([]writeBatch, 0, q.count)
	for q.count > 0 {
		result = append(result, q.items[q.head])
		q.items[q.head] = writeBatch{}
		q.head = (q.head + 1) % q.cap
		q.count--
	}
	q.notFull.Broadcast()
	return result
}

// drainAllInto drains all items into the provided buffer, avoiding heap
// allocation when the buffer has sufficient capacity. Returns the buffer
// with appended items.
func (q *writeQueue) drainAllInto(buf []writeBatch) []writeBatch {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.count == 0 {
		return buf
	}
	for q.count > 0 {
		buf = append(buf, q.items[q.head])
		q.items[q.head] = writeBatch{}
		q.head = (q.head + 1) % q.cap
		q.count--
	}
	q.notFull.Broadcast()
	return buf
}

// close signals the queue to stop accepting new items. Pending items
// can still be drained via dequeue or drainAll. Wakes all blocked
// producers and the consumer.
func (q *writeQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.notEmpty.Broadcast()
	q.notFull.Broadcast()
}

// size returns the current number of pending batches in the queue.
func (q *writeQueue) size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.count
}
