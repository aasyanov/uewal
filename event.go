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

// recordOptions holds per-record optional fields set via [RecordOption].
type recordOptions struct {
	key        []byte
	meta       []byte
	timestamp  int64
	noCompress bool
	owned      bool
}

// RecordOption configures optional fields for a single record.
// Shared between [WAL.Append], [Batch.Append], and [Batch.AppendUnsafe].
type RecordOption func(*recordOptions)

// WithKey attaches an application key to the record. Keys are stored in
// the wire format and exposed via [Indexer] for building key-based indexes.
func WithKey(key []byte) RecordOption {
	return func(o *recordOptions) { o.key = key }
}

// WithMeta attaches opaque metadata to the record. The WAL stores it
// alongside the payload but never interprets its contents.
func WithMeta(meta []byte) RecordOption {
	return func(o *recordOptions) { o.meta = meta }
}

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

// Batch groups multiple records for atomic submission via [WAL.AppendBatch].
//
// Records within a batch are assigned contiguous LSNs and encoded into a
// single batch frame with one CRC-32C checksum. The entire batch is atomic:
// on recovery, either all records are valid (CRC matches) or the whole
// frame is discarded.
type Batch struct {
	records    []record
	noCompress bool
}

// NewBatch creates a Batch pre-allocated for n records.
func NewBatch(n int) *Batch {
	return &Batch{records: make([]record, 0, n)}
}

// Append adds a record to the batch, copying payload, key, and meta.
// The caller may reuse buffers after the call returns.
func (b *Batch) Append(payload []byte, opts ...RecordOption) {
	if len(opts) == 0 {
		ts := time.Now().UnixNano()
		if len(payload) > 0 {
			buf := make([]byte, len(payload))
			copy(buf, payload)
			b.records = append(b.records, record{payload: buf, timestamp: ts})
		} else {
			b.records = append(b.records, record{timestamp: ts})
		}
		return
	}
	b.appendSlow(payload, opts)
}

//go:noinline
func (b *Batch) appendSlow(payload []byte, opts []RecordOption) {
	var o recordOptions
	for _, fn := range opts {
		fn(&o)
	}
	if o.timestamp == 0 {
		o.timestamp = time.Now().UnixNano()
	}
	if o.noCompress {
		b.noCompress = true
	}
	total := len(payload) + len(o.key) + len(o.meta)
	if total > 0 {
		buf := make([]byte, total)
		pn := copy(buf, payload)
		kn := copy(buf[pn:], o.key)
		mn := copy(buf[pn+kn:], o.meta)
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

// AppendUnsafe adds a record to the batch, taking ownership of all slices.
// Zero-copy: the caller MUST NOT modify payload, key, or meta after the call.
func (b *Batch) AppendUnsafe(payload []byte, opts ...RecordOption) {
	if len(opts) == 0 {
		b.records = append(b.records, record{
			payload:   payload,
			timestamp: time.Now().UnixNano(),
			owned:     true,
		})
		return
	}
	b.appendUnsafeSlow(payload, opts)
}

//go:noinline
func (b *Batch) appendUnsafeSlow(payload []byte, opts []RecordOption) {
	var o recordOptions
	for _, fn := range opts {
		fn(&o)
	}
	if o.timestamp == 0 {
		o.timestamp = time.Now().UnixNano()
	}
	if o.noCompress {
		b.noCompress = true
	}
	b.records = append(b.records, record{
		payload:   payload,
		key:       o.key,
		meta:      o.meta,
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
}

func applyOptions(opts []RecordOption) recordOptions {
	if len(opts) == 0 {
		return recordOptions{timestamp: time.Now().UnixNano()}
	}
	o := applyOptionsSlow(opts)
	return o
}

//go:noinline
func applyOptionsSlow(opts []RecordOption) recordOptions {
	var o recordOptions
	for _, fn := range opts {
		fn(&o)
	}
	if o.timestamp == 0 {
		o.timestamp = time.Now().UnixNano()
	}
	return o
}

func sliceOrNil(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return b
}

func copyBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	cp := make([]byte, len(b))
	copy(cp, b)
	return cp
}
