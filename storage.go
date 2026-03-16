package uewal

import (
	"os"
	"sync"
)

// Storage defines the interface for per-segment WAL persistence backends.
// The writer goroutine is the sole writer; reads may occur concurrently.
type Storage interface {
	Write(p []byte) (n int, err error)
	Sync() error
	Close() error
	Size() (int64, error)
	Truncate(size int64) error
	ReadAt(p []byte, off int64) (n int, err error)
}

// FileStorage is the default Storage implementation backed by [os.File].
// No built-in file locking — directory-level LOCK file is handled by WAL.
type FileStorage struct {
	mu   sync.Mutex
	f    *os.File
	path string
}

// NewFileStorage opens or creates a file at path for segment storage.
// File pointer is positioned at the end for append writes.
func NewFileStorage(path string) (*FileStorage, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	if _, err := f.Seek(0, 2); err != nil {
		f.Close()
		return nil, err
	}

	return &FileStorage{f: f, path: path}, nil
}

func (fs *FileStorage) Path() string { return fs.path }

func (fs *FileStorage) Write(p []byte) (int, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.f == nil {
		return 0, ErrClosed
	}
	return fs.f.Write(p)
}

// WriteNoLock is like Write but skips mutex acquisition.
// Only safe when the caller guarantees exclusive write access (single writer goroutine).
func (fs *FileStorage) WriteNoLock(p []byte) (int, error) {
	return fs.f.Write(p)
}

func (fs *FileStorage) Sync() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.f == nil {
		return ErrClosed
	}
	return fs.f.Sync()
}

// SyncNoLock is like Sync but skips mutex acquisition.
// Only safe when the caller guarantees exclusive access (single writer goroutine).
func (fs *FileStorage) SyncNoLock() error {
	return fs.f.Sync()
}

func (fs *FileStorage) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.f == nil {
		return nil
	}
	err := fs.f.Close()
	fs.f = nil
	return err
}

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

func (fs *FileStorage) ReadAt(p []byte, off int64) (int, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.f == nil {
		return 0, ErrClosed
	}
	return fs.f.ReadAt(p, off)
}
