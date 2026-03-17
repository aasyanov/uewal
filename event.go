package uewal

import "time"

// LSN (Log Sequence Number) is a monotonically increasing identifier
// assigned to each event written to the WAL. LSNs start at 1 and
// increment by 1 for each event. Zero means "not assigned".
type LSN = uint64

// Event represents a single entry in the write-ahead log.
//
// On write, LSN is assigned by the WAL and Timestamp is auto-filled
// with time.Now().UnixNano() unless overridden via [WithTimestamp].
//
// On replay, all fields are populated. Key, Meta, and Payload point
// directly into memory-mapped storage (zero-copy). If data must outlive
// the replay callback or iterator, the caller must copy it.
type Event struct {
	LSN       LSN
	Timestamp int64
	Key       []byte
	Meta      []byte
	Payload   []byte
}

// recordOptions holds per-record optional overrides set via [RecordOption].
type recordOptions struct {
	timestamp  int64
	noCompress bool
}

// RecordOption configures rare per-record overrides.
type RecordOption func(*recordOptions)

// WithTimestamp overrides the auto-assigned timestamp (UnixNano).
// If not set, the WAL uses time.Now().UnixNano() at append time.
func WithTimestamp(ts int64) RecordOption {
	return func(o *recordOptions) { o.timestamp = ts }
}

// WithNoCompress marks this record to skip compression. If any record
// in a batch is marked, the entire batch is written uncompressed.
// Useful for already-compressed data (JPEG, protobuf, zstd payloads).
func WithNoCompress() RecordOption {
	return func(o *recordOptions) { o.noCompress = true }
}

// record is the internal representation of a single event before encoding.
type record struct {
	payload   []byte
	key       []byte
	meta      []byte
	timestamp int64
	owned     bool
}

// Batch groups multiple records for atomic submission via [WAL.Write].
//
// Records within a batch are assigned contiguous LSNs and encoded into a
// single batch frame with one CRC-32C checksum. The entire batch is atomic:
// on recovery, either all records are valid (CRC matches) or the whole
// frame is discarded.
type Batch struct {
	records    []record
	noCompress bool
	tsUniform  bool  // true while all records share the same timestamp
	firstTS    int64 // timestamp of the first record, for uniformity tracking
	hasKeyMeta bool  // true if any record has non-empty key or meta
}

// NewBatch creates a Batch pre-allocated for n records.
func NewBatch(n int) *Batch {
	return &Batch{records: make([]record, 0, n)}
}

// Append adds a record to the batch, copying payload, key, and meta.
// The caller may reuse buffers after the call returns.
func (b *Batch) Append(payload, key, meta []byte, opts ...RecordOption) {
	if len(opts) == 0 {
		ts := time.Now().UnixNano()
		b.trackTS(ts)
		b.trackKeyMeta(key, meta)
		total := len(payload) + len(key) + len(meta)
		if total > 0 {
			buf := make([]byte, total)
			pn := copy(buf, payload)
			kn := copy(buf[pn:], key)
			mn := copy(buf[pn+kn:], meta)
			b.records = append(b.records, record{
				payload:   buf[:pn],
				key:       sliceOrNil(buf[pn : pn+kn]),
				meta:      sliceOrNil(buf[pn+kn : pn+kn+mn]),
				timestamp: ts,
			})
		} else {
			b.records = append(b.records, record{timestamp: ts})
		}
		return
	}
	b.appendSlow(payload, key, meta, opts)
}

//go:noinline
func (b *Batch) appendSlow(payload, key, meta []byte, opts []RecordOption) {
	var o recordOptions
	for _, fn := range opts {
		fn(&o)
	}
	if o.timestamp == 0 {
		o.timestamp = time.Now().UnixNano()
	}
	b.trackTS(o.timestamp)
	b.trackKeyMeta(key, meta)
	if o.noCompress {
		b.noCompress = true
	}
	total := len(payload) + len(key) + len(meta)
	if total > 0 {
		buf := make([]byte, total)
		pn := copy(buf, payload)
		kn := copy(buf[pn:], key)
		mn := copy(buf[pn+kn:], meta)
		b.records = append(b.records, record{
			payload:   buf[:pn],
			key:       sliceOrNil(buf[pn : pn+kn]),
			meta:      sliceOrNil(buf[pn+kn : pn+kn+mn]),
			timestamp: o.timestamp,
		})
	} else {
		b.records = append(b.records, record{timestamp: o.timestamp})
	}
}

// AppendWithTimestamp adds a record with an explicit timestamp, avoiding
// the closure allocation that [WithTimestamp] incurs.
func (b *Batch) AppendWithTimestamp(payload, key, meta []byte, ts int64) {
	b.trackTS(ts)
	b.trackKeyMeta(key, meta)
	total := len(payload) + len(key) + len(meta)
	if total > 0 {
		buf := make([]byte, total)
		pn := copy(buf, payload)
		kn := copy(buf[pn:], key)
		mn := copy(buf[pn+kn:], meta)
		b.records = append(b.records, record{
			payload:   buf[:pn],
			key:       sliceOrNil(buf[pn : pn+kn]),
			meta:      sliceOrNil(buf[pn+kn : pn+kn+mn]),
			timestamp: ts,
		})
	} else {
		b.records = append(b.records, record{timestamp: ts})
	}
}

// AppendUnsafe adds a record to the batch, taking ownership of all slices.
// Zero-copy: the caller MUST NOT modify payload, key, or meta after the call.
func (b *Batch) AppendUnsafe(payload, key, meta []byte, opts ...RecordOption) {
	if len(opts) == 0 {
		ts := time.Now().UnixNano()
		b.trackTS(ts)
		b.trackKeyMeta(key, meta)
		b.records = append(b.records, record{
			payload:   payload,
			key:       key,
			meta:      meta,
			timestamp: ts,
			owned:     true,
		})
		return
	}
	b.appendUnsafeSlow(payload, key, meta, opts)
}

// MarkNoCompress sets the no-compress flag on the entire batch, equivalent
// to passing [WithNoCompress] on every record but without closure allocations.
// Call once before or after appending records.
func (b *Batch) MarkNoCompress() {
	b.noCompress = true
}

// AppendUnsafeWithTimestamp is like [AppendUnsafe] with an explicit timestamp,
// avoiding the closure allocation that [WithTimestamp] incurs.
func (b *Batch) AppendUnsafeWithTimestamp(payload, key, meta []byte, ts int64) {
	b.trackTS(ts)
	b.trackKeyMeta(key, meta)
	b.records = append(b.records, record{
		payload:   payload,
		key:       key,
		meta:      meta,
		timestamp: ts,
		owned:     true,
	})
}

//go:noinline
func (b *Batch) appendUnsafeSlow(payload, key, meta []byte, opts []RecordOption) {
	var o recordOptions
	for _, fn := range opts {
		fn(&o)
	}
	if o.timestamp == 0 {
		o.timestamp = time.Now().UnixNano()
	}
	b.trackTS(o.timestamp)
	b.trackKeyMeta(key, meta)
	if o.noCompress {
		b.noCompress = true
	}
	b.records = append(b.records, record{
		payload:   payload,
		key:       key,
		meta:      meta,
		timestamp: o.timestamp,
		owned:     true,
	})
}

// Len returns the number of records currently in the batch.
func (b *Batch) Len() int { return len(b.records) }

// Reset clears the batch for reuse without releasing the underlying allocation.
func (b *Batch) Reset() {
	clear(b.records)
	b.records = b.records[:0]
	b.noCompress = false
	b.tsUniform = false
	b.firstTS = 0
	b.hasKeyMeta = false
}

func (b *Batch) trackTS(ts int64) {
	if len(b.records) == 0 {
		b.firstTS = ts
		b.tsUniform = true
	} else if b.tsUniform && ts != b.firstTS {
		b.tsUniform = false
	}
}

func (b *Batch) trackKeyMeta(key, meta []byte) {
	if len(key) > 0 || len(meta) > 0 {
		b.hasKeyMeta = true
	}
}

func sliceOrNil(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return b
}
