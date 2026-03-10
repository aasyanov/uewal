package uewal

import (
	"sync"
	"sync/atomic"
)

// lsnCounter is an atomic counter for monotonic LSN assignment. Batch
// LSN ranges are reserved via val.Add(n) in appendEvents; current and
// store are used for reads and recovery. Safe for concurrent use.
type lsnCounter struct {
	val atomic.Uint64
}

// current returns the current counter value without incrementing.
func (c *lsnCounter) current() LSN {
	return c.val.Load()
}

// store sets the counter to the given value. Used during recovery to
// restore the LSN sequence after scanning existing records.
func (c *lsnCounter) store(v LSN) {
	c.val.Store(v)
}

// eventSlicePool amortizes the per-Append allocation of []Event copies.
// Slices are acquired in appendEvents and returned by the writer after
// encoding, keeping GC pressure low under sustained write load.
//
// The pool stores *[]Event pointers. The same pointer obtained from Get
// is passed through writeBatch.eventsPool and returned via Put, avoiding
// a heap allocation on every put cycle.
var eventSlicePool = sync.Pool{
	New: func() any {
		s := make([]Event, 0, 8)
		return &s
	},
}

// getEventSlice returns a []Event of length n from the pool, growing
// the underlying slice if needed. The returned *[]Event must be passed
// to putEventSlice after the slice is no longer needed.
func getEventSlice(n int) ([]Event, *[]Event) {
	sp := eventSlicePool.Get().(*[]Event)
	s := *sp
	if cap(s) < n {
		s = make([]Event, n)
	} else {
		s = s[:n]
	}
	return s, sp
}

// putEventSlice zeros all elements in s and returns the slice to the
// pool via sp. sp must be the pointer returned by getEventSlice.
func putEventSlice(sp *[]Event, s []Event) {
	for i := range s {
		s[i] = Event{}
	}
	*sp = s[:0]
	eventSlicePool.Put(sp)
}

// appendEvents is the internal implementation shared by [WAL.Append] and
// [WAL.AppendBatch].
//
// It obtains a pooled event slice, copies the caller's events to prevent
// mutation, assigns contiguous LSNs via a single atomic Add, fires the
// BeforeAppend hook (when configured), and enqueues the batch to the
// writer according to the configured backpressure mode. On enqueue
// failure the pooled slice is returned immediately.
func (w *WAL) appendEvents(events []Event) (LSN, error) {
	if len(events) == 0 {
		return 0, ErrEmptyBatch
	}

	if err := w.sm.mustBeRunning(); err != nil {
		return 0, err
	}

	owned, sp := getEventSlice(len(events))
	copy(owned, events)

	n := uint64(len(owned))
	lastLSN := w.lsn.val.Add(n)
	firstLSN := lastLSN - n + 1
	for i := range owned {
		owned[i].LSN = firstLSN + uint64(i)
	}

	if w.hooks.h.BeforeAppend != nil {
		batch := &Batch{Events: owned}
		w.hooks.beforeAppend(batch)
	}

	wb := writeBatch{
		events:     owned,
		eventsPool: sp,
		lsnStart:   firstLSN,
		lsnEnd:     lastLSN,
	}

	switch w.cfg.backpressure {
	case BlockMode:
		if !w.queue.enqueue(wb) {
			putEventSlice(sp, owned)
			return 0, ErrClosed
		}
	case DropMode:
		if !w.queue.tryEnqueue(wb) {
			w.stats.addDrop(uint64(len(owned)))
			w.hooks.onDrop(len(owned))
			putEventSlice(sp, owned)
			return lastLSN, nil
		}
	case ErrorMode:
		if !w.queue.tryEnqueue(wb) {
			putEventSlice(sp, owned)
			return 0, ErrQueueFull
		}
	}

	return lastLSN, nil
}
