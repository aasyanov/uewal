package uewal

import (
	"sync"
	"sync/atomic"
	"time"
)

// lsnCounter is an atomic counter for monotonic LSN assignment.
// Padded to a full cache line to prevent false sharing with adjacent fields.
type lsnCounter struct {
	val  atomic.Uint64
	_pad [56]byte //nolint:unused // cache-line padding
}

func (c *lsnCounter) current() LSN {
	return c.val.Load()
}

func (c *lsnCounter) store(v LSN) {
	c.val.Store(v)
}

// recordSlicePool amortizes per-Append allocation of []record.
var recordSlicePool = sync.Pool{
	New: func() any {
		s := make([]record, 0, 8)
		return &s
	},
}

// payloadPools provides tiered reuse of byte buffers for common payload sizes.
// Class index 0..5 maps to sizes 64, 128, 256, 512, 1024, 4096.
var payloadPoolSizes = [...]int{64, 128, 256, 512, 1024, 4096}

var payloadPools [6]sync.Pool

func init() {
	for i := range payloadPools {
		sz := payloadPoolSizes[i]
		payloadPools[i].New = func() any {
			b := make([]byte, sz)
			return &b
		}
	}
}

// getPayloadBuf returns a buffer of at least size bytes from the tiered pool.
// Returns the buffer, pool class (1-based), or (nil, 0) if size exceeds all pools.
func getPayloadBuf(size int) ([]byte, int8) {
	for i, psz := range payloadPoolSizes {
		if size <= psz {
			bp := payloadPools[i].Get().(*[]byte)
			return (*bp)[:size], int8(i + 1)
		}
	}
	return make([]byte, size), 0
}

// putPayloadBuf returns a buffer to the appropriate pool.
func putPayloadBuf(buf []byte, class int8) {
	if class <= 0 || int(class) > len(payloadPools) {
		return
	}
	b := buf[:cap(buf)]
	payloadPools[class-1].Put(&b)
}

func getRecordSlice(n int) ([]record, *[]record) {
	sp := recordSlicePool.Get().(*[]record)
	s := *sp
	if cap(s) < n {
		s = make([]record, n)
	} else {
		s = s[:n]
	}
	return s, sp
}

func putRecordSlice(sp *[]record, s []record) {
	for i := range s {
		if s[i].poolClass > 0 {
			putPayloadBuf(s[i].payload, s[i].poolClass)
		}
		s[i] = record{}
	}
	*sp = s[:0]
	recordSlicePool.Put(sp)
}

// appendRecords is the internal path shared by WAL.Append and WAL.AppendBatch.
func (w *WAL) appendRecords(recs []record, pool *[]record, noCompress bool, tsUniformHint bool) (LSN, error) {
	if len(recs) == 0 {
		return 0, ErrEmptyBatch
	}

	if err := w.sm.mustBeRunning(); err != nil {
		return 0, err
	}

	if w.cfg.maxBatchSize > 0 && len(recs) > 1 {
		size := batchOverhead + recordsRegionSize(recs, !uniformTimestamp(recs))
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
		records:    recs,
		recordPool: pool,
		noCompress: noCompress,
		tsUniform:  tsUniformHint,
		lsnStart:   firstLSN,
		lsnEnd:     lastLSN,
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
			return lastLSN, nil
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

// singleAppend handles WAL.Append — single event with copy semantics.
func (w *WAL) singleAppend(payload []byte, opts ...RecordOption) (LSN, error) {
	recs, sp := getRecordSlice(1)

	if len(opts) == 0 {
		if len(payload) > 0 {
			buf, cls := getPayloadBuf(len(payload))
			copy(buf, payload)
			recs[0] = record{payload: buf, timestamp: time.Now().UnixNano(), poolClass: cls}
		} else {
			recs[0] = record{timestamp: time.Now().UnixNano()}
		}
		return w.appendRecords(recs, sp, false, true)
	}

	return w.singleAppendSlow(payload, recs, sp, opts)
}

//go:noinline
func (w *WAL) singleAppendSlow(payload []byte, recs []record, sp *[]record, opts []RecordOption) (LSN, error) {
	var o recordOptions
	for _, fn := range opts {
		fn(&o)
	}
	if o.timestamp == 0 {
		o.timestamp = time.Now().UnixNano()
	}

	total := len(payload) + len(o.key) + len(o.meta)
	if total > 0 {
		buf := make([]byte, total)
		pn := copy(buf, payload)
		kn := copy(buf[pn:], o.key)
		mn := copy(buf[pn+kn:], o.meta)
		recs[0] = record{
			payload:   buf[:pn],
			key:       sliceOrNil(buf[pn : pn+kn]),
			meta:      sliceOrNil(buf[pn+kn : pn+kn+mn]),
			timestamp: o.timestamp,
		}
	} else {
		recs[0] = record{timestamp: o.timestamp}
	}

	return w.appendRecords(recs, sp, o.noCompress, true)
}
