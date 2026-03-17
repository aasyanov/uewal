package uewal

import "time"

// Hooks provides observability callbacks for WAL lifecycle events.
// All fields are optional (nil = no-op). Panics are recovered.
//
// Pipeline hooks (AfterAppend through OnDelete) are called in the writer
// goroutine and MUST NOT block for extended periods.
type Hooks struct {
	// Lifecycle — called in caller goroutine.
	OnStart         func()
	OnShutdownStart func()
	OnShutdownDone  func(elapsed time.Duration)

	// Write pipeline — called in writer goroutine, must not block.
	AfterAppend func(firstLSN, lastLSN LSN, count int)
	BeforeWrite func(bytes int)
	AfterWrite  func(bytes int, elapsed time.Duration)
	BeforeSync  func()
	AfterSync   func(bytes int, elapsed time.Duration)

	// Errors — called in writer goroutine.
	OnCorruption func(segmentPath string, offset int64)
	OnDrop       func(count int)

	// Recovery — called during Open, before writer starts.
	OnRecovery func(info RecoveryInfo)

	// Segment lifecycle — called in writer goroutine.
	OnRotation func(sealed SegmentInfo)
	OnDelete   func(deleted SegmentInfo)
}

// RecoveryInfo describes the result of WAL recovery during [Open].
type RecoveryInfo struct {
	SegmentCount   int   // number of segments recovered
	TruncatedBytes int64 // bytes truncated from active segment (corruption cleanup)
	Corrupted      bool  // true if any data corruption was detected
}

// hooksRunner wraps Hooks with panic-safe invocation.
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

func (r *hooksRunner) afterAppend(firstLSN, lastLSN LSN, count int) {
	if r.h.AfterAppend != nil {
		safeCall(func() { r.h.AfterAppend(firstLSN, lastLSN, count) })
	}
}

func (r *hooksRunner) beforeWrite(n int) {
	if r.h.BeforeWrite != nil {
		safeCall(func() { r.h.BeforeWrite(n) })
	}
}

func (r *hooksRunner) afterWrite(n int, d time.Duration) {
	if r.h.AfterWrite != nil {
		safeCall(func() { r.h.AfterWrite(n, d) })
	}
}

func (r *hooksRunner) beforeSync() {
	if r.h.BeforeSync != nil {
		safeCall(func() { r.h.BeforeSync() })
	}
}

func (r *hooksRunner) afterSync(n int, d time.Duration) {
	if r.h.AfterSync != nil {
		safeCall(func() { r.h.AfterSync(n, d) })
	}
}

func (r *hooksRunner) onRecovery(info RecoveryInfo) {
	if r.h.OnRecovery != nil {
		safeCall(func() { r.h.OnRecovery(info) })
	}
}

func (r *hooksRunner) onCorruption(segmentPath string, offset int64) {
	if r.h.OnCorruption != nil {
		safeCall(func() { r.h.OnCorruption(segmentPath, offset) })
	}
}

func (r *hooksRunner) onDrop(count int) {
	if r.h.OnDrop != nil {
		safeCall(func() { r.h.OnDrop(count) })
	}
}

func (r *hooksRunner) onRotation(info SegmentInfo) {
	if r.h.OnRotation != nil {
		safeCall(func() { r.h.OnRotation(info) })
	}
}

func (r *hooksRunner) onDelete(info SegmentInfo) {
	if r.h.OnDelete != nil {
		safeCall(func() { r.h.OnDelete(info) })
	}
}

func safeCall(fn func()) {
	defer func() { _ = recover() }()
	fn()
}
