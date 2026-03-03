package uewal

// readAllFallback is used when mmap is not available — either because
// the Storage is not a [FileStorage] (no file descriptor) or the platform
// lacks mmap support. It reads the entire content via [Storage.ReadAt]
// in a loop, handling partial reads.
//
// The returned byte slice is a regular heap allocation, not memory-mapped.
// Modifications to it do not affect the underlying storage.
func readAllFallback(s Storage, size int64) ([]byte, error) {
	data := make([]byte, size)
	off := int64(0)
	for off < size {
		n, err := s.ReadAt(data[off:], off)
		off += int64(n)
		if err != nil {
			if off >= size {
				break
			}
			return data[:off], err
		}
	}
	return data, nil
}
