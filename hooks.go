package uewal

import "time"

// Hooks provides observability callbacks for WAL lifecycle events.
//
// All fields are optional: a nil function pointer means no-op.
// Hooks are invoked synchronously within the writer goroutine (for pipeline
// hooks) or the caller goroutine (for lifecycle hooks). They must not block
// for extended periods, as this directly impacts write throughput.
//
// Panics within any hook are recovered and silently discarded. A panicking
// hook never affects WAL consistency or crashes the writer goroutine.
//
// Configure via [WithHooks].
type Hooks struct {
	// OnStart is called once after Open successfully transitions to RUNNING.
	OnStart func()

	// OnShutdownStart is called when Shutdown begins (RUNNING → DRAINING).
	OnShutdownStart func()

	// OnShutdownDone is called when Shutdown completes. The duration
	// parameter measures the total shutdown time from start to close.
	OnShutdownDone func(elapsed time.Duration)

	// BeforeAppend is called before a batch is enqueued to the writer.
	// The Batch pointer allows inspection or mutation of events before
	// they enter the write pipeline (e.g., adding metadata).
	BeforeAppend func(b *Batch)

	// AfterAppend is called by the writer goroutine after a batch has been
	// encoded. lsn is the last LSN in the batch; count is the number of events.
	AfterAppend func(lsn LSN, count int)

	// BeforeWrite is called before a write syscall. size is the buffer
	// length in bytes about to be written.
	BeforeWrite func(size int)

	// AfterWrite is called after a write syscall completes. n is the
	// number of bytes actually written.
	AfterWrite func(n int)

	// BeforeSync is called before an fsync. Only triggered by the writer's
	// automatic sync logic (SyncBatch/SyncInterval), not by explicit Sync calls.
	BeforeSync func()

	// AfterSync is called after an fsync completes. elapsed is the fsync latency.
	AfterSync func(elapsed time.Duration)

	// OnCorruption is called when corruption is detected during recovery
	// or replay. offset is the byte position where the first invalid record
	// was found; the file is truncated to this boundary.
	OnCorruption func(offset int64)

	// OnDrop is called in DropMode when events are discarded because the
	// write queue is full. count is the number of events dropped.
	OnDrop func(count int)
}

// hooksRunner wraps a Hooks value with panic-safe invocation.
// Each method checks for a nil function pointer before calling, and
// recovers from any panic to prevent hook failures from propagating.
type hooksRunner struct {
	h Hooks
}

func (r *hooksRunner) onStart() {
	if r.h.OnStart != nil {
		safeCall(func() { r.h.OnStart() })
	}
}

func (r *hooksRunner) onShutdownStart() {
	if r.h.OnShutdownStart != nil {
		safeCall(func() { r.h.OnShutdownStart() })
	}
}

func (r *hooksRunner) onShutdownDone(d time.Duration) {
	if r.h.OnShutdownDone != nil {
		safeCall(func() { r.h.OnShutdownDone(d) })
	}
}

func (r *hooksRunner) beforeAppend(b *Batch) {
	if r.h.BeforeAppend != nil {
		safeCall(func() { r.h.BeforeAppend(b) })
	}
}

func (r *hooksRunner) afterAppend(lsn LSN, n int) {
	if r.h.AfterAppend != nil {
		safeCall(func() { r.h.AfterAppend(lsn, n) })
	}
}

func (r *hooksRunner) beforeWrite(n int) {
	if r.h.BeforeWrite != nil {
		safeCall(func() { r.h.BeforeWrite(n) })
	}
}

func (r *hooksRunner) afterWrite(n int) {
	if r.h.AfterWrite != nil {
		safeCall(func() { r.h.AfterWrite(n) })
	}
}

func (r *hooksRunner) beforeSync() {
	if r.h.BeforeSync != nil {
		safeCall(func() { r.h.BeforeSync() })
	}
}

func (r *hooksRunner) afterSync(d time.Duration) {
	if r.h.AfterSync != nil {
		safeCall(func() { r.h.AfterSync(d) })
	}
}

func (r *hooksRunner) onCorruption(offset int64) {
	if r.h.OnCorruption != nil {
		safeCall(func() { r.h.OnCorruption(offset) })
	}
}

func (r *hooksRunner) onDrop(count int) {
	if r.h.OnDrop != nil {
		safeCall(func() { r.h.OnDrop(count) })
	}
}

// safeCall invokes fn, recovering from any panic.
func safeCall(fn func()) {
	defer func() { _ = recover() }()
	fn()
}
