package uewal

import "time"

// SyncMode determines when the writer goroutine calls fsync.
type SyncMode int

const (
	// SyncNever disables explicit fsync; relies on OS page cache only.
	SyncNever SyncMode = iota
	// SyncBatch calls fsync after every write batch.
	SyncBatch
	// SyncInterval calls fsync at regular time intervals.
	SyncInterval
	// SyncCount calls fsync after every N batches processed by the writer.
	SyncCount
	// SyncSize calls fsync after every N bytes written to the segment file.
	SyncSize
)

// BackpressureMode determines behavior when the write queue is full.
type BackpressureMode int

const (
	// BlockMode blocks the caller until queue space is available.
	BlockMode BackpressureMode = iota
	// DropMode silently drops the write, fires [Hooks.OnDrop], and returns
	// LSN 0 with a nil error. Callers should check for LSN == 0 to detect drops.
	DropMode
	// ErrorMode returns ErrQueueFull immediately.
	ErrorMode
)

const (
	defaultQueueSize      = 4096
	defaultBufferSize     = 64 << 10         // 64 KB
	defaultSyncInterval   = 100 * time.Millisecond
	defaultMaxSegmentSize = 256 << 20        // 256 MB
	defaultMaxBatchSize   = 4 << 20          // 4 MB
)

// Compressor provides optional compression for batch records region.
// The header remains plaintext; CRC covers the header and the
// (possibly compressed) records region.
type Compressor interface {
	Compress(src []byte) ([]byte, error)
	Decompress(src []byte) ([]byte, error)
}

// ScratchCompressor extends [Compressor] with scratch-buffer methods that
// avoid per-call allocations. If a Compressor also implements this interface,
// the WAL will prefer CompressTo/DecompressTo on the hot path.
type ScratchCompressor interface {
	Compressor
	CompressTo(dst, src []byte) ([]byte, error)
	DecompressTo(dst, src []byte) ([]byte, error)
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
//
// The Indexer is not notified when segments are deleted (retention,
// compaction, or explicit [WAL.DeleteBefore]/[WAL.DeleteOlderThan]).
// To keep an external index consistent, also set [Hooks.OnDelete] to
// remove stale entries when segments are removed.
type Indexer interface {
	OnAppend(info IndexInfo)
}

// config holds all WAL configuration assembled via [Option] values.
type config struct {
	// Durability
	syncMode     SyncMode
	syncInterval time.Duration
	syncCount    int
	syncSize     uint64

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
	compressor     Compressor
	indexer        Indexer
	hooks          Hooks
	storageFactory StorageFactory
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

// WithSyncMode sets the fsync strategy.
func WithSyncMode(m SyncMode) Option {
	return func(c *config) { c.syncMode = m }
}

// WithSyncInterval sets the fsync interval for SyncInterval mode.
func WithSyncInterval(d time.Duration) Option {
	return func(c *config) { c.syncInterval = d }
}

// WithBackpressure sets behavior when the write queue is full.
func WithBackpressure(m BackpressureMode) Option {
	return func(c *config) { c.backpressure = m }
}

// WithQueueSize sets the write queue capacity.
func WithQueueSize(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.queueSize = n
		}
	}
}

// WithBufferSize sets the internal write-buffer size in bytes.
func WithBufferSize(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.bufferSize = n
		}
	}
}

// WithMaxBatchSize sets the maximum batch size in bytes.
func WithMaxBatchSize(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.maxBatchSize = n
		}
	}
}

// WithMaxSegmentSize sets the segment rotation threshold in bytes.
func WithMaxSegmentSize(n int64) Option {
	return func(c *config) {
		if n > 0 {
			c.maxSegmentSize = n
		}
	}
}

// WithMaxSegmentAge sets the maximum segment age before rotation.
func WithMaxSegmentAge(d time.Duration) Option {
	return func(c *config) { c.maxSegmentAge = d }
}

// WithMaxSegments sets the maximum number of segments to retain.
func WithMaxSegments(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.maxSegments = n
		}
	}
}

// WithRetentionSize sets the maximum total size of retained segments.
func WithRetentionSize(n int64) Option {
	return func(c *config) {
		if n > 0 {
			c.retentionSize = n
		}
	}
}

// WithRetentionAge sets the maximum age of retained segments.
func WithRetentionAge(d time.Duration) Option {
	return func(c *config) { c.retentionAge = d }
}

// WithPreallocate enables preallocation of segment files.
func WithPreallocate(v bool) Option {
	return func(c *config) { c.preallocate = v }
}

// WithStartLSN sets the starting LSN for a new WAL.
func WithStartLSN(lsn LSN) Option {
	return func(c *config) { c.startLSN = lsn }
}

// WithSyncCount sets SyncMode to [SyncCount] and configures fsync
// to trigger after every n batches processed by the writer.
func WithSyncCount(n int) Option {
	return func(c *config) {
		c.syncMode = SyncCount
		if n > 0 {
			c.syncCount = n
		}
	}
}

// WithSyncSize sets SyncMode to [SyncSize] and configures fsync
// to trigger after every n bytes written to the segment file.
func WithSyncSize(n uint64) Option {
	return func(c *config) {
		c.syncMode = SyncSize
		if n > 0 {
			c.syncSize = n
		}
	}
}

// WithCompressor sets the batch compression implementation.
func WithCompressor(comp Compressor) Option {
	return func(c *config) { c.compressor = comp }
}

// WithIndex sets the indexer for per-event append notifications.
func WithIndex(idx Indexer) Option {
	return func(c *config) { c.indexer = idx }
}

// WithHooks sets lifecycle and drop hooks.
func WithHooks(h Hooks) Option {
	return func(c *config) { c.hooks = h }
}

// StorageFactory creates a [Storage] backend for the given file path.
// Returning a nil Storage is not allowed and will cause a panic.
type StorageFactory func(path string) (Storage, error)

// WithStorageFactory sets a custom [Storage] backend factory.
// If not set, the WAL uses [NewFileStorage] (os.File-backed).
//
// This enables in-memory backends, encrypted storage, or direct I/O
// implementations without modifying the WAL core.
func WithStorageFactory(f StorageFactory) Option {
	return func(c *config) { c.storageFactory = f }
}
