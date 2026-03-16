package uewal

import "errors"

var errStopReplay = errors.New("stop")

// Iterator provides sequential read access across WAL segments.
// Uses mmap for zero-copy access. Create via [WAL.Iterator] or [WAL.Follow].
// Caller must call Close to release segment references.
type Iterator struct {
	segments []*segment
	mgr      *segmentManager
	segIdx   int
	reader   *mmapReader
	data     []byte
	decomp   Compressor
	offset   int
	batch    []Event
	batchAt  int
	event    Event
	err      error
	fromLSN  LSN
	follow   *followIterator // non-nil for Follow iterators
}

func (it *Iterator) Next() bool {
	if it.follow != nil {
		ev, ok := it.follow.next()
		if ok {
			it.event = ev
		}
		return ok
	}

	for {
		if it.err != nil {
			return false
		}

		if it.batchAt < len(it.batch) {
			it.event = it.batch[it.batchAt]
			it.batchAt++
			return true
		}

		if it.reader != nil && it.offset < len(it.data) {
			events, next, err := decodeBatchFrame(it.data, it.offset, it.decomp)
			if err != nil {
				if !it.advanceSegment() {
					return false
				}
				continue
			}
			it.offset = next
			it.batch = events
			it.batchAt = 0

			if it.fromLSN > 0 {
				for i, ev := range events {
					if ev.LSN >= it.fromLSN {
						it.batch = events[i:]
						it.batchAt = 0
						break
					}
					if i == len(events)-1 {
						it.batch = nil
					}
				}
			}
			continue
		}

		if !it.advanceSegment() {
			return false
		}
	}
}

func (it *Iterator) advanceSegment() bool {
	if it.reader != nil {
		it.reader.close()
		it.reader = nil
		it.data = nil
	}

	it.segIdx++
	if it.segIdx >= len(it.segments) {
		return false
	}

	seg := it.segments[it.segIdx]
	size := seg.size
	if !seg.sealed {
		size = seg.writeOff.Load()
	}
	if size <= 0 {
		return it.advanceSegment()
	}

	reader, err := mmapByPath(seg.path, size)
	if err != nil {
		it.err = err
		return false
	}

	it.reader = reader
	it.data = reader.bytes()
	it.offset = 0
	it.batch = nil
	it.batchAt = 0

	if it.fromLSN > 0 && seg.sealed && seg.sparse.len() > 0 {
		seekOff := seg.sparse.findByLSN(it.fromLSN)
		if seekOff >= 0 {
			it.offset = int(seekOff)
		}
	}

	return true
}

func (it *Iterator) Event() Event {
	return it.event
}

func (it *Iterator) Err() error {
	return it.err
}

func (it *Iterator) Close() error {
	if it.follow != nil {
		err := it.follow.close()
		it.follow = nil
		return err
	}
	var firstErr error
	if it.reader != nil {
		if err := it.reader.close(); err != nil {
			firstErr = err
		}
		it.reader = nil
	}
	if it.mgr != nil && it.segments != nil {
		it.mgr.releaseSegments(it.segments)
		it.segments = nil
	}
	return firstErr
}

// newCrossSegmentIterator creates an iterator spanning multiple segments.
func newCrossSegmentIterator(mgr *segmentManager, fromLSN LSN, decomp Compressor) (*Iterator, error) {
	segments := mgr.acquireSegments(fromLSN)
	if len(segments) == 0 {
		return &Iterator{}, nil
	}

	it := &Iterator{
		segments: segments,
		mgr:      mgr,
		segIdx:   -1,
		decomp:   decomp,
		fromLSN:  fromLSN,
	}

	if !it.advanceSegment() {
		mgr.releaseSegments(segments)
		return &Iterator{}, nil
	}

	return it, nil
}

// replayCallback iterates across segments, calling fn for each event with LSN >= fromLSN.
func replaySegments(mgr *segmentManager, fromLSN LSN, fn func(Event) error, decomp Compressor) error {
	segments := mgr.acquireSegments(fromLSN)
	defer mgr.releaseSegments(segments)

	for _, seg := range segments {
		size := seg.size
		if !seg.sealed {
			size = seg.writeOff.Load()
		}
		if size <= 0 {
			continue
		}

		reader, err := mmapByPath(seg.path, size)
		if err != nil {
			continue
		}

		data := reader.bytes()
		off := 0

		if fromLSN > 0 && seg.sealed && seg.sparse.len() > 0 {
			seekOff := seg.sparse.findByLSN(fromLSN)
			if seekOff >= 0 {
				off = int(seekOff)
			}
		}

		var callbackErr error
		for off < len(data) {
			events, next, decErr := decodeBatchFrame(data, off, decomp)
			if decErr != nil {
				break
			}
			off = next
			for _, ev := range events {
				if ev.LSN < fromLSN {
					continue
				}
				if err := fn(ev); err != nil {
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
			if callbackErr == errStopReplay {
				return nil
			}
			return callbackErr
		}
	}

	return nil
}

// replayBatchesSegments iterates batch frames across segments,
// calling fn with all events of each batch.
func replayBatchesSegments(mgr *segmentManager, fromLSN LSN, fn func([]Event) error, decomp Compressor) error {
	segments := mgr.acquireSegments(fromLSN)
	defer mgr.releaseSegments(segments)

	for _, seg := range segments {
		size := seg.size
		if !seg.sealed {
			size = seg.writeOff.Load()
		}
		if size <= 0 {
			continue
		}

		reader, err := mmapByPath(seg.path, size)
		if err != nil {
			continue
		}

		data := reader.bytes()
		off := 0

		if fromLSN > 0 && seg.sealed && seg.sparse.len() > 0 {
			seekOff := seg.sparse.findByLSN(fromLSN)
			if seekOff >= 0 {
				off = int(seekOff)
			}
		}

		var callbackErr error
		for off < len(data) {
			events, next, decErr := decodeBatchFrame(data, off, decomp)
			if decErr != nil {
				break
			}
			off = next

			if fromLSN > 0 {
				filtered := events[:0]
				for _, ev := range events {
					if ev.LSN >= fromLSN {
						filtered = append(filtered, ev)
					}
				}
				events = filtered
			}

			if len(events) > 0 {
				if err := fn(events); err != nil {
					callbackErr = err
					break
				}
			}
		}

		reader.close()

		if callbackErr != nil {
			return callbackErr
		}
	}

	return nil
}
