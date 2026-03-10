package uewal

// Iterator provides a high-performance, sequential read interface over
// WAL records using mmap for zero-copy access.
//
// Create via [WAL.Iterator]. The caller must call [Iterator.Close] when
// done to release mapped memory.
//
// Usage:
//
//	it, err := w.Iterator(0)
//	if err != nil {
//	    return err
//	}
//	defer it.Close()
//	for it.Next() {
//	    ev := it.Event()
//	    // process ev
//	}
//	if err := it.Err(); err != nil {
//	    // handle corruption
//	}
type Iterator struct {
	reader   *mmapReader
	data     []byte
	decomp   Compressor
	offset   int
	batch    []Event
	batchAt   int
	// decodeBuf is reused across Next calls to avoid per-batch allocation.
	decodeBuf []Event
	event     Event
	err      error
}

// Next advances the iterator to the next event.
//
// Returns true if an event is available (retrieve it via [Iterator.Event]).
// Returns false when there are no more events or an error occurred
// (check [Iterator.Err]).
func (it *Iterator) Next() bool {
	if it.err != nil {
		return false
	}

	// Serve from current decoded batch if possible.
	if it.batchAt < len(it.batch) {
		it.event = it.batch[it.batchAt]
		it.batchAt++
		return true
	}

	// Decode next batch frame.
	if it.offset >= len(it.data) {
		return false
	}

	it.decodeBuf = it.decodeBuf[:0]
	events, next, err := decodeBatchFrameInto(it.data, it.offset, it.decomp, it.decodeBuf)
	if err != nil {
		if err == ErrCRCMismatch {
			it.err = ErrCorrupted
		}
		return false
	}
	it.offset = next
	it.decodeBuf = events
	it.batch = events
	it.batchAt = 0

	if len(events) == 0 {
		return false
	}
	it.event = it.batch[it.batchAt]
	it.batchAt++
	return true
}

// Event returns the current event. Valid only after [Iterator.Next]
// returns true. The returned Event.Payload and Event.Meta are zero-copy
// slices into mapped memory (when uncompressed); copy if the data must
// outlive the iterator.
func (it *Iterator) Event() Event {
	return it.event
}

// Err returns any error that stopped iteration. Returns nil for normal
// end-of-data. Returns [ErrCorrupted] if a CRC mismatch was detected.
func (it *Iterator) Err() error {
	return it.err
}

// Close releases resources held by the iterator (unmaps memory).
// Safe to call multiple times.
func (it *Iterator) Close() error {
	if it.reader != nil {
		return it.reader.close()
	}
	return nil
}

// replayCallback iterates all batch frames, calling fn for each event
// with LSN >= fromLSN.
//
// If a decode error is encountered (corruption), replay stops at the last
// valid batch boundary. The file is truncated to that boundary, and
// stats/hooks are updated. If fn returns a non-nil error, replay stops
// immediately and returns that error without truncation.
//
// Important: fn must copy ev.Payload and ev.Meta if it needs to retain data
// beyond the callback, because they reference mmap'd memory.
func replayCallback(s Storage, fromLSN LSN, fn func(Event) error, decomp Compressor, stats *statsCollector, hooks *hooksRunner) error {
	size, err := s.Size()
	if err != nil {
		return err
	}
	if size == 0 {
		return nil
	}

	reader, err := newMmapReader(s, size)
	if err != nil {
		return err
	}

	data := reader.bytes()
	off := 0
	lastValid := 0
	corrupted := false
	var callbackErr error

	for off < len(data) {
		events, next, decErr := decodeBatchFrame(data, off, decomp)
		if decErr != nil {
			corrupted = true
			break
		}
		lastValid = next
		off = next

		for i := range events {
			if events[i].LSN < fromLSN {
				continue
			}
			if err := fn(events[i]); err != nil {
				callbackErr = err
				break
			}
		}
		if callbackErr != nil {
			break
		}
	}

	reader.close()

	if callbackErr != nil {
		return callbackErr
	}

	if corrupted {
		stats.addCorruption()
		hooks.onCorruption(int64(lastValid))
		if truncErr := s.Truncate(int64(lastValid)); truncErr != nil {
			return truncErr
		}
	}

	return nil
}

// newIterator creates an Iterator that reads from the given storage
// starting from the given LSN. Records with LSN < fromLSN are skipped
// during initialization (seek-forward), not during iteration.
func newIterator(s Storage, fromLSN LSN, decomp Compressor) (*Iterator, error) {
	size, err := s.Size()
	if err != nil {
		return nil, err
	}
	if size == 0 {
		return &Iterator{}, nil
	}

	reader, err := newMmapReader(s, size)
	if err != nil {
		return nil, err
	}

	data := reader.bytes()
	it := &Iterator{
		reader: reader,
		data:   data,
		decomp: decomp,
	}

	// Seek forward: skip entire batches whose last LSN < fromLSN,
	// then skip individual events within the first matching batch.
	if fromLSN > 0 {
		for it.offset < len(data) {
			events, next, err := decodeBatchFrame(data, it.offset, decomp)
			if err != nil {
				break
			}
			if len(events) > 0 && events[len(events)-1].LSN >= fromLSN {
				// This batch contains the target LSN. Skip events before it.
				for i := range events {
					if events[i].LSN >= fromLSN {
						it.batch = events[i:]
						it.batchAt = 0
						it.offset = next
						return it, nil
					}
				}
			}
			it.offset = next
		}
	}

	return it, nil
}
