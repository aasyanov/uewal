package uewal

import "os"

// fileLock represents a platform-specific advisory file lock handle.
// On Unix, the lock is held via flock(2) on the file descriptor.
// On Windows, the lock is held via LockFileEx on the file handle.
//
// The lock prevents multiple WAL instances from opening the same file
// concurrently, which would lead to data corruption.
type fileLock struct {
	f *os.File
}

// lockFile and unlockFile are implemented per-platform:
//   - flock_unix.go   (Linux, macOS, FreeBSD)
//   - flock_windows.go
