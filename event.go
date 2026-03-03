package uewal

// LSN (Log Sequence Number) is a monotonically increasing identifier
// assigned to each event written to the WAL. LSNs start at 1 and
// increment by 1 for each event. The zero value indicates "no LSN assigned"
// and is used as the initial state before any appends.
type LSN = uint64

// Event represents a single entry in the write-ahead log.
//
// When submitting events via [WAL.Append] or [WAL.AppendBatch], leave LSN
// as zero; it is assigned automatically by the WAL. The Payload field carries
// the application data and is stored as-is.
//
// Meta is an optional opaque byte slice for application-level metadata
// (event type, aggregate ID, shard key, tags, etc.). The WAL stores it
// alongside the Payload but never interprets its contents. When Meta is
// nil, zero bytes are written (MetaLen=0 in the wire format).
//
// During replay (via [WAL.Replay] or [Iterator]), both Payload and Meta
// point directly into memory-mapped storage (zero-copy). If the data must
// outlive the replay callback or iterator, the caller must copy it.
type Event struct {
	// LSN is assigned by the WAL upon write. Zero on input.
	LSN LSN

	// Meta is optional opaque metadata. The WAL does not interpret it.
	Meta []byte

	// Payload is the raw event data. The WAL does not interpret its contents.
	Payload []byte
}

// Batch is a group of events submitted together via [WAL.AppendBatch].
//
// Events within a batch are assigned contiguous LSNs and encoded into a
// single batch frame with one CRC-32C checksum. The entire batch is
// atomic: on recovery, either all events in the frame are valid (CRC
// matches) or the whole frame is discarded.
type Batch struct {
	Events []Event
}

// NewBatch creates a Batch pre-allocated for n events.
// The returned Batch has zero length but capacity n.
func NewBatch(n int) Batch {
	return Batch{Events: make([]Event, 0, n)}
}

// Add appends an event with the given payload to the batch.
// Meta is left nil; use [Batch.AddWithMeta] to include metadata.
func (b *Batch) Add(payload []byte) {
	b.Events = append(b.Events, Event{Payload: payload})
}

// AddWithMeta appends an event with both payload and metadata to the batch.
func (b *Batch) AddWithMeta(payload, meta []byte) {
	b.Events = append(b.Events, Event{Payload: payload, Meta: meta})
}

// Len returns the number of events currently in the batch.
func (b *Batch) Len() int {
	return len(b.Events)
}

// Reset clears the batch for reuse without releasing the underlying
// slice allocation. After Reset, Len returns 0 but capacity is preserved.
func (b *Batch) Reset() {
	b.Events = b.Events[:0]
}
