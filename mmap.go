package uewal

// mmapReader provides zero-copy read access to storage via memory mapping.
//
// Used by [WAL.Replay] and [Iterator] for high-performance sequential reads.
// When the underlying [Storage] is a [FileStorage], the file is mapped via
// mmap (Unix) or MapViewOfFile (Windows). For custom Storage implementations,
// a fallback based on [Storage.ReadAt] is used (see readAllFallback).
type mmapReader struct {
	data []byte
	size int
}

// newMmapReader maps the given storage for reading up to size bytes.
// Returns an empty reader (no data) if size is zero or negative.
func newMmapReader(s Storage, size int64) (*mmapReader, error) {
	if size <= 0 {
		return &mmapReader{}, nil
	}

	data, err := mmapFile(s, size)
	if err != nil {
		return nil, err
	}

	return &mmapReader{data: data, size: int(size)}, nil
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
		return nil
	}
	err := munmapFile(r.data)
	r.data = nil
	r.size = 0
	return err
}
