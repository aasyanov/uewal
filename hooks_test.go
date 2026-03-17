package uewal

import (
	"testing"
	"time"
)

func TestHooksRunner_AllFire(t *testing.T) {
	var counts [15]int
	r := &hooksRunner{
		h: Hooks{
			OnStart:         func() { counts[0]++ },
			OnShutdownStart: func() { counts[1]++ },
			OnShutdownDone:  func(time.Duration) { counts[2]++ },
			AfterAppend:     func(LSN, LSN, int) { counts[3]++ },
			BeforeWrite:     func(int) { counts[4]++ },
			AfterWrite:      func(int, time.Duration) { counts[5]++ },
			BeforeSync:      func() { counts[6]++ },
			AfterSync:       func(int, time.Duration) { counts[7]++ },
			OnCorruption:    func(string, int64) { counts[8]++ },
			OnDrop:          func(int) { counts[9]++ },
			OnRotation:      func(SegmentInfo) { counts[10]++ },
			OnDelete:        func(SegmentInfo) { counts[11]++ },
			OnError:         func(error) { counts[12]++ },
			OnImport:        func(LSN, LSN, int) { counts[13]++ },
			OnRecovery:      func(RecoveryInfo) { counts[14]++ },
		},
	}

	r.onStart()
	r.onShutdownStart()
	r.onShutdownDone(time.Second)
	r.afterAppend(1, 2, 2)
	r.beforeWrite(100)
	r.afterWrite(100, time.Millisecond)
	r.beforeSync()
	r.afterSync(100, time.Millisecond)
	r.onCorruption("/path/to/seg.wal", 42)
	r.onDrop(5)
	r.onRotation(SegmentInfo{Path: "a.wal", FirstLSN: 1, LastLSN: 10})
	r.onDelete(SegmentInfo{Path: "b.wal", FirstLSN: 1, LastLSN: 10})
	r.onError(ErrShortWrite)
	r.onImport(1, 5, 128)
	r.onRecovery(RecoveryInfo{SegmentCount: 1})

	for i, c := range counts {
		if c != 1 {
			t.Errorf("hook %d fired %d times, want 1", i, c)
		}
	}
}

func TestHooksRunner_NilSafe(t *testing.T) {
	r := &hooksRunner{h: Hooks{}}

	r.onStart()
	r.onShutdownStart()
	r.onShutdownDone(time.Second)
	r.afterAppend(1, 2, 2)
	r.beforeWrite(100)
	r.afterWrite(100, time.Millisecond)
	r.beforeSync()
	r.afterSync(100, time.Millisecond)
	r.onCorruption("/path/to/seg.wal", 42)
	r.onDrop(5)
	r.onRotation(SegmentInfo{})
	r.onDelete(SegmentInfo{})
	r.onError(ErrShortWrite)
	r.onImport(1, 5, 128)
	r.onRecovery(RecoveryInfo{})
}

func TestHooksRunner_PanicRecovery(t *testing.T) {
	var fired bool
	r := &hooksRunner{
		h: Hooks{
			OnStart: func() {
				fired = true
				panic("hook panic")
			},
		},
	}

	r.onStart()
	if !fired {
		t.Error("hook did not fire before panic")
	}
}

func TestHooks_SafeCall_PanicRecovery(t *testing.T) {
	var ran bool
	safeCall(func() {
		ran = true
		panic("test panic")
	})
	if !ran {
		t.Error("fn did not run before panic")
	}
}

func TestSafeCall_NoPanic(t *testing.T) {
	var ran bool
	safeCall(func() {
		ran = true
	})
	if !ran {
		t.Error("fn did not run")
	}
}
