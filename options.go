package uewal

import "time"

// SyncMode determines when the writer goroutine calls fsync.
type SyncMode int

const (
	SyncNever   SyncMode = iota // OS page cache only; max throughput
	SyncBatch                   // fsync after every write batch
	SyncInterval                // fsync at regular time intervals
)

// BackpressureMode determines behavior when the write queue is full.
type BackpressureMode int

const (
	BlockMode BackpressureMode = iota // block caller until space available
	DropMode                          // silently drop, fire Hooks.OnDrop
	ErrorMode                         // return ErrQueueFull immediately
)

const (
	defaultQueueSize      = 4096
	defaultBufferSize     = 64 << 10         // 64 KB
	defaultSyncInterval   = 100 * time.Millisecond
	defaultMaxSegmentSize = 256 << 20        // 256 MB
	defaultMaxBatchSize   = 4 << 20          // 4 MB
)

// Compressor provides optional compression for batch records region.
// The header remains plaintext; CRC covers compressed bytes.
type Compressor interface {
	Compress(src []byte) ([]byte, error)
	Decompress(src []byte) ([]byte, error)
}

// IndexInfo carries per-event metadata passed to [Indexer.OnAppend].
// Key and Meta are borrowed slices valid only during the callback.
type IndexInfo struct {
	LSN       LSN
	Timestamp int64
	Key       []byte
	Meta      []byte
	Offset    int64 // batch frame byte offset within segment file
	Segment   LSN   // segment identifier (= segment FirstLSN)
}

// Indexer receives notifications when events are persisted.
// Called from the writer goroutine after successful write, per-event.
// Must not block. Panics are recovered.
type Indexer interface {
	OnAppend(info IndexInfo)
}

// config holds all WAL configuration assembled via [Option] values.
type config struct {
	// Durability
	syncMode     SyncMode
	syncInterval time.Duration

	// Backpressure
	backpressure BackpressureMode
	queueSize    int
	bufferSize   int
	maxBatchSize int

	// Segments
	maxSegmentSize int64
	maxSegmentAge  time.Duration
	maxSegments    int
	retentionSize  int64
	retentionAge   time.Duration
	preallocate    bool
	startLSN       LSN

	// Extensions
	compressor Compressor
	indexer    Indexer
	hooks      Hooks
}

func defaultConfig() config {
	return config{
		syncMode:       SyncNever,
		syncInterval:   defaultSyncInterval,
		backpressure:   BlockMode,
		queueSize:      defaultQueueSize,
		bufferSize:     defaultBufferSize,
		maxBatchSize:   defaultMaxBatchSize,
		maxSegmentSize: defaultMaxSegmentSize,
	}
}

// Option configures the WAL. Passed to [Open], applied in order.
type Option func(*config)

func WithSyncMode(m SyncMode) Option {
	return func(c *config) { c.syncMode = m }
}

func WithSyncInterval(d time.Duration) Option {
	return func(c *config) { c.syncInterval = d }
}

func WithBackpressure(m BackpressureMode) Option {
	return func(c *config) { c.backpressure = m }
}

func WithQueueSize(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.queueSize = n
		}
	}
}

func WithBufferSize(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.bufferSize = n
		}
	}
}

func WithMaxBatchSize(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.maxBatchSize = n
		}
	}
}

func WithMaxSegmentSize(n int64) Option {
	return func(c *config) {
		if n > 0 {
			c.maxSegmentSize = n
		}
	}
}

func WithMaxSegmentAge(d time.Duration) Option {
	return func(c *config) { c.maxSegmentAge = d }
}

func WithMaxSegments(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.maxSegments = n
		}
	}
}

func WithRetentionSize(n int64) Option {
	return func(c *config) {
		if n > 0 {
			c.retentionSize = n
		}
	}
}

func WithRetentionAge(d time.Duration) Option {
	return func(c *config) { c.retentionAge = d }
}

func WithPreallocate(v bool) Option {
	return func(c *config) { c.preallocate = v }
}

func WithStartLSN(lsn LSN) Option {
	return func(c *config) { c.startLSN = lsn }
}

func WithCompressor(comp Compressor) Option {
	return func(c *config) { c.compressor = comp }
}

func WithIndex(idx Indexer) Option {
	return func(c *config) { c.indexer = idx }
}

func WithHooks(h Hooks) Option {
	return func(c *config) { c.hooks = h }
}
