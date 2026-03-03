package uewal

import "time"

// SyncMode determines when the writer goroutine calls fsync to ensure
// data durability. Higher durability modes reduce throughput.
type SyncMode int

const (
	// SyncNever disables explicit fsync. Written data resides only in
	// the OS page cache and may be lost on power failure. This mode
	// provides maximum throughput.
	SyncNever SyncMode = iota

	// SyncBatch calls fsync after every write batch. This is the
	// strongest durability guarantee: each batch is durable before
	// the writer proceeds to the next.
	SyncBatch

	// SyncInterval calls fsync at a regular time interval (configured
	// via [WithSyncInterval]). This balances throughput and durability
	// by amortizing fsync costs over multiple batches.
	SyncInterval
)

// BackpressureMode determines the behavior of [WAL.Append] when the
// internal write queue is full.
type BackpressureMode int

const (
	// BlockMode blocks the calling goroutine until space is available
	// in the write queue. This is the default mode.
	BlockMode BackpressureMode = iota

	// DropMode silently drops events when the queue is full and
	// increments [Stats.Drops]. The [Hooks.OnDrop] callback is fired.
	// Append still returns the assigned LSN without error.
	DropMode

	// ErrorMode returns [ErrQueueFull] immediately when the queue is full.
	ErrorMode
)

const (
	defaultQueueSize    = 4096
	defaultBufferSize   = 64 * 1024
	defaultSyncInterval = 100 * time.Millisecond
)

// Compressor provides optional payload compression for batch frames.
//
// When set via [WithCompressor], the writer compresses the records region
// of each batch frame before writing. The compressed flag is stored in the
// batch header Flags field, so the decoder knows whether to decompress.
//
// Implementations must be safe for concurrent use if the WAL is used from
// multiple goroutines (the Compressor is called from the single writer
// goroutine, so concurrent safety is not strictly required, but recommended
// for reusability). Buffer management (e.g. sync.Pool) is the
// implementation's responsibility.
type Compressor interface {
	Compress(src []byte) ([]byte, error)
	Decompress(src []byte) ([]byte, error)
}

// Indexer receives notifications when events are persisted to storage.
//
// The WAL calls OnAppend from the writer goroutine after a batch has been
// successfully written to storage. Panics in OnAppend are recovered and
// silently discarded. The indexer must not block for extended periods.
//
// Use [WithIndex] to attach an Indexer to the WAL.
type Indexer interface {
	OnAppend(lsn LSN, meta []byte, offset int64)
}

// config holds all WAL configuration. Assembled via functional [Option] values
// passed to [Open]. Not exported; users interact through Option functions.
type config struct {
	storage      Storage
	compressor   Compressor
	indexer      Indexer
	syncMode     SyncMode
	syncInterval time.Duration
	backpressure BackpressureMode
	queueSize    int
	bufferSize   int
	hooks        Hooks
}

func defaultConfig() config {
	return config{
		syncMode:     SyncNever,
		syncInterval: defaultSyncInterval,
		backpressure: BlockMode,
		queueSize:    defaultQueueSize,
		bufferSize:   defaultBufferSize,
	}
}

// Option configures the WAL. Options are passed to [Open] and applied
// in order. The last value wins for each setting.
type Option func(*config)

// WithStorage sets a custom [Storage] backend.
// If not set, the default [FileStorage] backed by [os.File] is used.
func WithStorage(s Storage) Option {
	return func(c *config) { c.storage = s }
}

// WithSyncMode sets the durability mode. See [SyncMode] for available modes.
func WithSyncMode(m SyncMode) Option {
	return func(c *config) { c.syncMode = m }
}

// WithSyncInterval sets the fsync interval for [SyncInterval] mode.
// Ignored when SyncMode is not SyncInterval. Default: 100ms.
func WithSyncInterval(d time.Duration) Option {
	return func(c *config) { c.syncInterval = d }
}

// WithBackpressure sets the backpressure strategy when the write queue is
// full. See [BackpressureMode] for available modes. Default: [BlockMode].
func WithBackpressure(m BackpressureMode) Option {
	return func(c *config) { c.backpressure = m }
}

// WithQueueSize sets the write queue capacity (number of batches).
// Values ≤ 0 are silently ignored. Default: 4096.
func WithQueueSize(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.queueSize = n
		}
	}
}

// WithBufferSize sets the initial encoder buffer size in bytes.
// The buffer grows dynamically as needed. Values ≤ 0 are silently ignored.
// Default: 64 KiB.
func WithBufferSize(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.bufferSize = n
		}
	}
}

// WithCompressor sets an optional [Compressor] for payload compression.
// When set, batch frame records are compressed before writing and
// decompressed transparently during replay.
func WithCompressor(comp Compressor) Option {
	return func(c *config) { c.compressor = comp }
}

// WithIndex sets an optional [Indexer] that is notified after each event
// is persisted. See [Indexer] for details.
func WithIndex(idx Indexer) Option {
	return func(c *config) { c.indexer = idx }
}

// WithHooks sets observability hooks. See [Hooks] for available callbacks.
func WithHooks(h Hooks) Option {
	return func(c *config) { c.hooks = h }
}
