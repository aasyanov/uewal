//go:build !windows

package uewal

import (
	"os"
	"syscall"
)

// lockFile acquires an exclusive, non-blocking advisory lock on f via flock(2).
// Returns an error if the file is already locked by another process.
func lockFile(f *os.File) (fileLock, error) {
	err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		return fileLock{}, err
	}
	return fileLock{f: f}, nil
}

// unlockFile releases the advisory lock previously acquired by lockFile.
func unlockFile(lock fileLock) {
	if lock.f != nil {
		_ = syscall.Flock(int(lock.f.Fd()), syscall.LOCK_UN)
	}
}
