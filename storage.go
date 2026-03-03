package uewal

import (
	"os"
	"sync"
)

// Storage defines the interface for WAL persistence backends.
//
// A Storage implementation is used by the writer goroutine for sequential
// writes, by Replay/Iterator for reads, and by recovery for truncation.
// The writer goroutine is the sole writer; reads may occur concurrently
// from other goroutines (e.g., Replay).
//
// Implementations must handle their own thread safety for concurrent
// Read + Write access if the application calls Replay while the WAL
// is running.
type Storage interface {
	// Write appends data to the storage. Implementations must handle
	// positioning (the WAL does not seek before writing). Short writes
	// (n < len(p) with nil error) are retried by the writer.
	Write(p []byte) (n int, err error)

	// Sync ensures all written data is durable on the underlying medium
	// (e.g., fsync for file-based storage).
	Sync() error

	// Close releases all resources held by the storage. After Close,
	// all other methods must return ErrClosed or equivalent.
	Close() error

	// Size returns the current storage size in bytes.
	Size() (int64, error)

	// Truncate shrinks the storage to the given size. Used during
	// recovery to remove corrupted trailing records.
	Truncate(size int64) error

	// ReadAt reads len(p) bytes from the storage starting at byte offset off.
	// Used as a fallback when mmap is not available (custom Storage).
	ReadAt(p []byte, off int64) (n int, err error)
}

// FileStorage is the default Storage implementation backed by [os.File].
//
// It provides file-locking via flock (Unix) or LockFileEx (Windows) to
// prevent concurrent access from multiple WAL instances opening the same
// file. All methods are protected by a mutex for safe concurrent use.
//
// Create instances via [NewFileStorage].
type FileStorage struct {
	mu   sync.Mutex
	f    *os.File
	path string
	lock fileLock
}

// NewFileStorage opens or creates a file at the given path for WAL storage.
//
// The file is opened with O_CREATE|O_RDWR (mode 0644) and immediately
// locked via flock/LockFileEx. If the file is already locked by another
// process or WAL instance, [ErrFileLocked] is returned.
//
// The file pointer is positioned at the end so that Write appends data.
func NewFileStorage(path string) (*FileStorage, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	lock, err := lockFile(f)
	if err != nil {
		f.Close()
		return nil, ErrFileLocked
	}

	if _, err := f.Seek(0, 2); err != nil {
		unlockFile(lock)
		f.Close()
		return nil, err
	}

	return &FileStorage{f: f, path: path, lock: lock}, nil
}

// Path returns the filesystem path of the storage file.
func (fs *FileStorage) Path() string {
	return fs.path
}

// Write appends p to the file. Returns [ErrClosed] if the file has been closed.
func (fs *FileStorage) Write(p []byte) (int, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.f == nil {
		return 0, ErrClosed
	}
	return fs.f.Write(p)
}

// Sync calls fsync on the underlying file. Returns [ErrClosed] if the file
// has been closed.
func (fs *FileStorage) Sync() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.f == nil {
		return ErrClosed
	}
	return fs.f.Sync()
}

// Close releases the file lock and closes the underlying file.
// Idempotent: calling Close on an already-closed FileStorage returns nil.
func (fs *FileStorage) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.f == nil {
		return nil
	}
	unlockFile(fs.lock)
	err := fs.f.Close()
	fs.f = nil
	return err
}

// Size returns the current file size in bytes via Stat.
// Returns [ErrClosed] if the file has been closed.
func (fs *FileStorage) Size() (int64, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.f == nil {
		return 0, ErrClosed
	}
	info, err := fs.f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// Truncate shrinks the file to the given size and repositions the file
// pointer to the end for subsequent appends. Returns [ErrClosed] if
// the file has been closed.
func (fs *FileStorage) Truncate(size int64) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.f == nil {
		return ErrClosed
	}
	if err := fs.f.Truncate(size); err != nil {
		return err
	}
	_, err := fs.f.Seek(0, 2)
	return err
}

// ReadAt reads len(p) bytes from the file starting at byte offset off.
// Returns [ErrClosed] if the file has been closed.
func (fs *FileStorage) ReadAt(p []byte, off int64) (int, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.f == nil {
		return 0, ErrClosed
	}
	return fs.f.ReadAt(p, off)
}
