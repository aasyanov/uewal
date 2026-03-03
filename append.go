package uewal

import "sync/atomic"

// lsnCounter provides monotonically increasing LSN assignment using
// atomic operations. Safe for concurrent use from multiple goroutines.
type lsnCounter struct {
	val atomic.Uint64
}

// next atomically increments the counter and returns the new value.
func (c *lsnCounter) next() LSN {
	return c.val.Add(1)
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

// appendEvents is the internal implementation shared by [WAL.Append] and
// [WAL.AppendBatch].
//
// It copies the input events slice to prevent mutation of the caller's data,
// assigns monotonic LSNs, fires the BeforeAppend hook, and enqueues the
// batch to the writer according to the configured backpressure mode.
func (w *WAL) appendEvents(events []Event) (LSN, error) {
	if len(events) == 0 {
		return 0, ErrEmptyBatch
	}

	if err := w.sm.mustBeRunning(); err != nil {
		return 0, err
	}

	owned := make([]Event, len(events))
	copy(owned, events)

	var firstLSN, lastLSN LSN
	for i := range owned {
		owned[i].LSN = w.lsn.next()
		if i == 0 {
			firstLSN = owned[i].LSN
		}
		lastLSN = owned[i].LSN
	}

	batch := &Batch{Events: owned}
	w.hooks.beforeAppend(batch)

	wb := writeBatch{
		events:   owned,
		lsnStart: firstLSN,
		lsnEnd:   lastLSN,
	}

	switch w.cfg.backpressure {
	case BlockMode:
		if !w.queue.enqueue(wb) {
			return 0, ErrClosed
		}
	case DropMode:
		if !w.queue.tryEnqueue(wb) {
			w.stats.addDrop(uint64(len(owned)))
			w.hooks.onDrop(len(owned))
			return lastLSN, nil
		}
	case ErrorMode:
		if !w.queue.tryEnqueue(wb) {
			return 0, ErrQueueFull
		}
	}

	return lastLSN, nil
}
