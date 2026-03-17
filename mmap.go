package uewal

import "os"

// mmapReader provides zero-copy read access to segment files via memory mapping.
//
// Used by [WAL.Replay] and [Iterator] for high-performance sequential reads.
// Files are mapped via mmap (Unix) or MapViewOfFile (Windows).
type mmapReader struct {
	data []byte
	size int
	f    *os.File // non-nil when opened by mmapByPath; closed on close()
}

// mmapByPath opens a file read-only and maps it into memory.
// The file handle is kept alive until close() is called.
func mmapByPath(path string, size int64) (*mmapReader, error) {
	if size <= 0 {
		return &mmapReader{}, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	data, err := mmapFd(f, size)
	if err != nil {
		f.Close()
		return nil, err
	}
	return &mmapReader{data: data, size: int(size), f: f}, nil
}

// bytes returns the full mapped region as a byte slice.
// Returns nil if the reader is empty or has been closed.
func (r *mmapReader) bytes() []byte {
	return r.data
}

// close unmaps the memory region and releases resources.
// Safe to call multiple times.
func (r *mmapReader) close() error {
	if r.data == nil {
		if r.f != nil {
			err := r.f.Close()
			r.f = nil
			return err
		}
		return nil
	}
	err := munmapFile(r.data)
	r.data = nil
	r.size = 0
	if r.f != nil {
		if cerr := r.f.Close(); err == nil {
			err = cerr
		}
		r.f = nil
	}
	return err
}
