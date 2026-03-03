//go:build !windows

package uewal

import (
	"fmt"
	"os"
	"syscall"
)

// mmapFile maps the storage into memory for zero-copy reads.
// If s is not a *FileStorage, falls back to readAllFallback.
func mmapFile(s Storage, size int64) ([]byte, error) {
	fs, ok := s.(*FileStorage)
	if !ok {
		return readAllFallback(s, size)
	}
	if fs.f == nil {
		return nil, fmt.Errorf("uewal: file is closed")
	}
	return mmapFd(fs.f, size)
}

// mmapFd maps size bytes of the file into read-only shared memory.
func mmapFd(f *os.File, size int64) ([]byte, error) {
	data, err := syscall.Mmap(int(f.Fd()), 0, int(size),
		syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("uewal: mmap: %w", err)
	}
	return data, nil
}

// munmapFile unmaps a previously mapped region.
func munmapFile(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return syscall.Munmap(data)
}
