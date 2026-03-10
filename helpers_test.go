package uewal

// memStorage is an in-memory Storage implementation for tests that
// don't need file I/O, mmap, or locking.
type memStorage struct {
	data   []byte
	closed bool
}

func (m *memStorage) Write(p []byte) (int, error) {
	m.data = append(m.data, p...)
	return len(p), nil
}
func (m *memStorage) Sync() error  { return nil }
func (m *memStorage) Close() error { m.closed = true; return nil }
func (m *memStorage) Size() (int64, error) {
	return int64(len(m.data)), nil
}
func (m *memStorage) Truncate(size int64) error {
	m.data = m.data[:size]
	return nil
}
func (m *memStorage) ReadAt(p []byte, off int64) (int, error) {
	n := copy(p, m.data[off:])
	return n, nil
}
