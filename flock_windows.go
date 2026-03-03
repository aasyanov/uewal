//go:build windows

package uewal

import (
	"os"
	"syscall"
	"unsafe"
)

var (
	modkernel32      = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx   = modkernel32.NewProc("LockFileEx")
	procUnlockFileEx = modkernel32.NewProc("UnlockFileEx")
)

const (
	lockfileExclusiveLock   = 0x00000002
	lockfileFailImmediately = 0x00000001
)

// lockFile acquires an exclusive, non-blocking lock on f via LockFileEx.
// Returns an error if the file is already locked by another process.
func lockFile(f *os.File) (fileLock, error) {
	var ol syscall.Overlapped
	handle := syscall.Handle(f.Fd())
	flags := uint32(lockfileExclusiveLock | lockfileFailImmediately)

	r1, _, err := procLockFileEx.Call(
		uintptr(handle),
		uintptr(flags),
		0,
		1, 0,
		uintptr(unsafe.Pointer(&ol)),
	)
	if r1 == 0 {
		return fileLock{}, err
	}
	return fileLock{f: f}, nil
}

// unlockFile releases the lock previously acquired by lockFile via UnlockFileEx.
func unlockFile(lock fileLock) {
	if lock.f == nil {
		return
	}
	var ol syscall.Overlapped
	handle := syscall.Handle(lock.f.Fd())
	_, _, _ = procUnlockFileEx.Call(
		uintptr(handle),
		0,
		1, 0,
		uintptr(unsafe.Pointer(&ol)),
	)
}
