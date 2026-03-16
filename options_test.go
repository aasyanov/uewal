package uewal

import (
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	c := defaultConfig()
	if c.syncMode != SyncNever {
		t.Errorf("syncMode = %v, want SyncNever", c.syncMode)
	}
	if c.backpressure != BlockMode {
		t.Errorf("backpressure = %v, want BlockMode", c.backpressure)
	}
	if c.queueSize != 4096 {
		t.Errorf("queueSize = %d, want 4096", c.queueSize)
	}
	if c.bufferSize != 64<<10 {
		t.Errorf("bufferSize = %d, want 64KB", c.bufferSize)
	}
	if c.maxSegmentSize != 256<<20 {
		t.Errorf("maxSegmentSize = %d, want 256MB", c.maxSegmentSize)
	}
	if c.maxBatchSize != 4<<20 {
		t.Errorf("maxBatchSize = %d, want 4MB", c.maxBatchSize)
	}
}

func TestWithSyncMode(t *testing.T) {
	c := defaultConfig()
	WithSyncMode(SyncBatch)(&c)
	if c.syncMode != SyncBatch {
		t.Errorf("syncMode = %v, want SyncBatch", c.syncMode)
	}
}

func TestWithSyncInterval(t *testing.T) {
	c := defaultConfig()
	d := 200 * time.Millisecond
	WithSyncInterval(d)(&c)
	if c.syncInterval != d {
		t.Errorf("syncInterval = %v, want %v", c.syncInterval, d)
	}
}

func TestWithBackpressure(t *testing.T) {
	c := defaultConfig()
	WithBackpressure(DropMode)(&c)
	if c.backpressure != DropMode {
		t.Errorf("backpressure = %v, want DropMode", c.backpressure)
	}
}

func TestWithQueueSize(t *testing.T) {
	c := defaultConfig()
	WithQueueSize(8192)(&c)
	if c.queueSize != 8192 {
		t.Errorf("queueSize = %d, want 8192", c.queueSize)
	}
	WithQueueSize(0)(&c)
	if c.queueSize != 8192 {
		t.Errorf("queueSize after 0 = %d, want 8192 (ignored)", c.queueSize)
	}
	WithQueueSize(-1)(&c)
	if c.queueSize != 8192 {
		t.Errorf("queueSize after -1 = %d, want 8192 (ignored)", c.queueSize)
	}
}

func TestWithBufferSize(t *testing.T) {
	c := defaultConfig()
	WithBufferSize(128 << 10)(&c)
	if c.bufferSize != 128<<10 {
		t.Errorf("bufferSize = %d, want 128KB", c.bufferSize)
	}
	WithBufferSize(0)(&c)
	if c.bufferSize != 128<<10 {
		t.Errorf("bufferSize after 0 = %d, want 128KB (ignored)", c.bufferSize)
	}
}

func TestWithMaxBatchSize(t *testing.T) {
	c := defaultConfig()
	WithMaxBatchSize(8 << 20)(&c)
	if c.maxBatchSize != 8<<20 {
		t.Errorf("maxBatchSize = %d, want 8MB", c.maxBatchSize)
	}
	WithMaxBatchSize(0)(&c)
	if c.maxBatchSize != 8<<20 {
		t.Errorf("maxBatchSize after 0 = %d, want 8MB (ignored)", c.maxBatchSize)
	}
}

func TestWithMaxSegmentSize(t *testing.T) {
	c := defaultConfig()
	WithMaxSegmentSize(512 << 20)(&c)
	if c.maxSegmentSize != 512<<20 {
		t.Errorf("maxSegmentSize = %d, want 512MB", c.maxSegmentSize)
	}
	WithMaxSegmentSize(0)(&c)
	if c.maxSegmentSize != 512<<20 {
		t.Errorf("maxSegmentSize after 0 = %d, want 512MB (ignored)", c.maxSegmentSize)
	}
}

func TestWithMaxSegmentAge(t *testing.T) {
	c := defaultConfig()
	d := 5 * time.Minute
	WithMaxSegmentAge(d)(&c)
	if c.maxSegmentAge != d {
		t.Errorf("maxSegmentAge = %v, want %v", c.maxSegmentAge, d)
	}
}

func TestWithMaxSegments(t *testing.T) {
	c := defaultConfig()
	WithMaxSegments(10)(&c)
	if c.maxSegments != 10 {
		t.Errorf("maxSegments = %d, want 10", c.maxSegments)
	}
	WithMaxSegments(0)(&c)
	if c.maxSegments != 10 {
		t.Errorf("maxSegments after 0 = %d, want 10 (ignored)", c.maxSegments)
	}
}

func TestWithRetentionSize(t *testing.T) {
	c := defaultConfig()
	WithRetentionSize(1 << 30)(&c)
	if c.retentionSize != 1<<30 {
		t.Errorf("retentionSize = %d, want 1GB", c.retentionSize)
	}
	WithRetentionSize(0)(&c)
	if c.retentionSize != 1<<30 {
		t.Errorf("retentionSize after 0 = %d, want 1GB (ignored)", c.retentionSize)
	}
}

func TestWithRetentionAge(t *testing.T) {
	c := defaultConfig()
	d := 24 * time.Hour
	WithRetentionAge(d)(&c)
	if c.retentionAge != d {
		t.Errorf("retentionAge = %v, want %v", c.retentionAge, d)
	}
}

func TestWithPreallocate(t *testing.T) {
	c := defaultConfig()
	WithPreallocate(true)(&c)
	if !c.preallocate {
		t.Error("preallocate = false, want true")
	}
}

func TestWithStartLSN_Option(t *testing.T) {
	c := defaultConfig()
	WithStartLSN(100)(&c)
	if c.startLSN != 100 {
		t.Errorf("startLSN = %d, want 100", c.startLSN)
	}
}

func TestWithCompressor(t *testing.T) {
	c := defaultConfig()
	comp := &mockCompressor{}
	WithCompressor(comp)(&c)
	if c.compressor != comp {
		t.Error("compressor not set")
	}
}

func TestWithIndex(t *testing.T) {
	c := defaultConfig()
	idx := &mockIndexer{}
	WithIndex(idx)(&c)
	if c.indexer != idx {
		t.Error("indexer not set")
	}
}

func TestWithHooks(t *testing.T) {
	c := defaultConfig()
	h := Hooks{OnStart: func() {}}
	WithHooks(h)(&c)
	if c.hooks.OnStart == nil {
		t.Error("hooks not set")
	}
}

type mockCompressor struct{}

func (m *mockCompressor) Compress(src []byte) ([]byte, error)   { return src, nil }
func (m *mockCompressor) Decompress(src []byte) ([]byte, error) { return src, nil }

type mockIndexer struct{}

func (m *mockIndexer) OnAppend(IndexInfo) {}
