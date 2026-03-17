package uewal

import "sync/atomic"

// lsnCounter is an atomic counter for monotonic LSN assignment.
// Padded to a full cache line to prevent false sharing with adjacent fields.
type lsnCounter struct {
	val  atomic.Uint64
	_pad [56]byte //lint:ignore U1000 cache-line padding to prevent false sharing
}

func (c *lsnCounter) current() LSN {
	return c.val.Load()
}

func (c *lsnCounter) store(v LSN) {
	c.val.Store(v)
}

// appendRecords is the internal write path shared by WAL.Write and WAL.WriteUnsafe.
func (w *WAL) appendRecords(recs []record, pool *[]record, noCompress bool, tsUniformHint bool, payloadOnlyHint bool) (LSN, error) {
	if len(recs) == 0 {
		return 0, ErrEmptyBatch
	}

	if err := w.sm.mustBeRunning(); err != nil {
		return 0, err
	}

	if w.cfg.maxBatchSize > 0 && len(recs) > 1 {
		var perRecTS bool
		if tsUniformHint {
			perRecTS = false
		} else {
			perRecTS = !uniformTimestamp(recs)
		}
		size := batchOverhead + recordsRegionSize(recs, perRecTS)
		if size > w.cfg.maxBatchSize {
			if pool != nil {
				putRecordSlice(pool, recs)
			}
			return 0, ErrBatchTooLarge
		}
	} else if w.cfg.maxBatchSize > 0 && len(recs) == 1 {
		size := batchOverhead + recordFixedLen(false) + len(recs[0].payload) + len(recs[0].key) + len(recs[0].meta)
		if size > w.cfg.maxBatchSize {
			if pool != nil {
				putRecordSlice(pool, recs)
			}
			return 0, ErrBatchTooLarge
		}
	}

	n := uint64(len(recs))
	lastLSN := w.lsn.val.Add(n)
	firstLSN := lastLSN - n + 1

	wb := writeBatch{
		records:     recs,
		recordPool:  pool,
		noCompress:  noCompress,
		tsUniform:   tsUniformHint,
		payloadOnly: payloadOnlyHint,
		lsnStart:    firstLSN,
		lsnEnd:      lastLSN,
	}

	switch w.cfg.backpressure {
	case BlockMode:
		if !w.queue.enqueue(wb) {
			if pool != nil {
				putRecordSlice(pool, recs)
			}
			return 0, ErrClosed
		}
	case DropMode:
		if !w.queue.tryEnqueue(wb) {
			w.stats.addDrop(uint64(len(recs)))
			w.hooks.onDrop(len(recs))
			if pool != nil {
				putRecordSlice(pool, recs)
			}
			return 0, nil
		}
	case ErrorMode:
		if !w.queue.tryEnqueue(wb) {
			if pool != nil {
				putRecordSlice(pool, recs)
			}
			return 0, ErrQueueFull
		}
	}

	return lastLSN, nil
}
