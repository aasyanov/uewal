package uewal

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestHooksRunnerNilHooks(t *testing.T) {
	r := &hooksRunner{}
	// None of these should panic
	r.onStart()
	r.onShutdownStart()
	r.onShutdownDone(time.Second)
	r.beforeAppend(nil)
	r.afterAppend(1, 1)
	r.beforeWrite(10)
	r.afterWrite(10)
	r.beforeSync()
	r.afterSync(time.Millisecond)
	r.onCorruption(100)
	r.onDrop(5)
}

func TestHooksRunnerInvocation(t *testing.T) {
	var calls atomic.Int32
	r := &hooksRunner{h: Hooks{
		OnStart:         func() { calls.Add(1) },
		OnShutdownStart: func() { calls.Add(1) },
		OnShutdownDone:  func(time.Duration) { calls.Add(1) },
		BeforeAppend:    func(*Batch) { calls.Add(1) },
		AfterAppend:     func(LSN, int) { calls.Add(1) },
		BeforeWrite:     func(int) { calls.Add(1) },
		AfterWrite:      func(int) { calls.Add(1) },
		BeforeSync:      func() { calls.Add(1) },
		AfterSync:       func(time.Duration) { calls.Add(1) },
		OnCorruption:    func(int64) { calls.Add(1) },
		OnDrop:          func(int) { calls.Add(1) },
	}}

	r.onStart()
	r.onShutdownStart()
	r.onShutdownDone(time.Second)
	r.beforeAppend(nil)
	r.afterAppend(1, 1)
	r.beforeWrite(10)
	r.afterWrite(10)
	r.beforeSync()
	r.afterSync(time.Millisecond)
	r.onCorruption(100)
	r.onDrop(5)

	if got := calls.Load(); got != 11 {
		t.Fatalf("expected 11 hook calls, got %d", got)
	}
}

func TestHooksRunnerPanicRecovery(t *testing.T) {
	panicker := func() { panic("test panic") }

	r := &hooksRunner{h: Hooks{
		OnStart:         panicker,
		OnShutdownStart: panicker,
		BeforeAppend:    func(*Batch) { panic("batch panic") },
		AfterAppend:     func(LSN, int) { panic("append panic") },
		BeforeWrite:     func(int) { panic("write panic") },
		AfterWrite:      func(int) { panic("write panic") },
		BeforeSync:      panicker,
		AfterSync:       func(time.Duration) { panic("sync panic") },
		OnCorruption: func(int64) { panic("corruption panic") },
		OnDrop:          func(int) { panic("drop panic") },
	}}

	// None of these should propagate the panic
	r.onStart()
	r.onShutdownStart()
	r.beforeAppend(nil)
	r.afterAppend(1, 1)
	r.beforeWrite(10)
	r.afterWrite(10)
	r.beforeSync()
	r.afterSync(time.Millisecond)
	r.onCorruption(100)
	r.onDrop(5)
}

func TestHooksRunnerParameters(t *testing.T) {
	var gotLSN LSN
	var gotN int
	var gotDuration time.Duration
	var gotOffset int64
	var gotDropCount int

	r := &hooksRunner{h: Hooks{
		AfterAppend:  func(lsn LSN, n int) { gotLSN = lsn; gotN = n },
		AfterSync:    func(d time.Duration) { gotDuration = d },
		OnCorruption: func(off int64) { gotOffset = off },
		OnDrop:       func(n int) { gotDropCount = n },
	}}

	r.afterAppend(42, 5)
	if gotLSN != 42 || gotN != 5 {
		t.Errorf("afterAppend: LSN=%d, n=%d", gotLSN, gotN)
	}

	r.afterSync(123 * time.Millisecond)
	if gotDuration != 123*time.Millisecond {
		t.Errorf("afterSync: duration=%v", gotDuration)
	}

	r.onCorruption(9999)
	if gotOffset != 9999 {
		t.Errorf("onCorruption: offset=%d", gotOffset)
	}

	r.onDrop(7)
	if gotDropCount != 7 {
		t.Errorf("onDrop: count=%d", gotDropCount)
	}
}
