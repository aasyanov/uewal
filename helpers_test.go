package uewal

import (
	"sync"
)

// writeOne is a test helper that writes a single event via batch.
func writeOne(w *WAL, payload, key, meta []byte, opts ...RecordOption) (LSN, error) {
	b := NewBatch(1)
	b.Append(payload, key, meta, opts...)
	return w.Write(b)
}

// memStorage is an in-memory Storage for tests.
type memStorage struct {
	mu   sync.Mutex
	data []byte
}

func (m *memStorage) Write(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = append(m.data, p...)
	return len(p), nil
}

func (m *memStorage) Sync() error { return nil }

func (m *memStorage) Close() error { return nil }

func (m *memStorage) Size() (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return int64(len(m.data)), nil
}

func (m *memStorage) Truncate(size int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if int(size) > len(m.data) {
		return nil
	}
	m.data = m.data[:size]
	return nil
}

func (m *memStorage) ReadAt(p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := copy(p, m.data[off:])
	return n, nil
}

func (m *memStorage) bytes() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(m.data))
	copy(cp, m.data)
	return cp
}
