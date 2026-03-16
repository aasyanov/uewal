package uewal

import "sync/atomic"

// followIterator is a tail-follow iterator that blocks on Next() when
// it reaches the end of the log, waiting for new data. It automatically
// crosses segment boundaries during rotation.
//
// Create via [WAL.Follow]. Close with Close() to unblock any waiting Next().
type followIterator struct {
	w       *WAL
	decomp  Compressor
	fromLSN LSN

	segments []*segment
	segIdx   int
	reader   *mmapReader
	data     []byte
	offset   int

	batch   []Event
	batchAt int
	event   Event
	err     error

	closed atomic.Int32
	wake   chan struct{}
}

// Follow returns a tail-follow iterator starting from the given LSN.
// Next() blocks when all current data has been consumed, waiting for
// new writes. Call Close() to unblock and stop the iterator.
func (w *WAL) Follow(from LSN) (*Iterator, error) {
	switch w.sm.load() {
	case StateInit:
		return nil, ErrNotRunning
	case StateClosed:
		return nil, ErrClosed
	}

	fi := &followIterator{
		w:       w,
		decomp:  w.cfg.compressor,
		fromLSN: from,
		wake:    make(chan struct{}, 1),
	}

	it := &Iterator{
		decomp:  w.cfg.compressor,
		fromLSN: from,
		follow:  fi,
	}

	return it, nil
}

// next implements the blocking tail-follow logic.
// Returns the next event, or false when closed.
func (fi *followIterator) next() (Event, bool) {
	for {
		if fi.closed.Load() != 0 {
			return Event{}, false
		}

		if fi.batchAt < len(fi.batch) {
			ev := fi.batch[fi.batchAt]
			fi.batchAt++
			fi.fromLSN = ev.LSN + 1
			return ev, true
		}

		if fi.reader != nil && fi.offset < len(fi.data) {
			events, nextOff, err := decodeBatchFrame(fi.data, fi.offset, fi.decomp)
			if err != nil {
				fi.closeReader()
				continue
			}
			fi.offset = nextOff

			if fi.fromLSN > 0 {
				for i, ev := range events {
					if ev.LSN >= fi.fromLSN {
						events = events[i:]
						fi.fromLSN = 0
						goto gotBatch
					}
				}
				continue
			}

		gotBatch:
			if len(events) > 0 {
				fi.batch = events
				fi.batchAt = 0
				continue
			}
			continue
		}

		if fi.tryAdvance() {
			continue
		}

		fi.waitForData()
	}
}

func (fi *followIterator) tryAdvance() bool {
	fi.closeReader()

	if fi.segments != nil {
		for fi.segIdx++; fi.segIdx < len(fi.segments); fi.segIdx++ {
			if fi.openSegment(fi.segments[fi.segIdx]) {
				return true
			}
		}
		fi.w.mgr.releaseSegments(fi.segments)
		fi.segments = nil
	}

	segments := fi.w.mgr.acquireSegments(fi.fromLSN)
	if len(segments) == 0 {
		return false
	}
	fi.segments = segments
	for fi.segIdx = 0; fi.segIdx < len(fi.segments); fi.segIdx++ {
		if fi.openSegment(fi.segments[fi.segIdx]) {
			return true
		}
	}
	fi.w.mgr.releaseSegments(fi.segments)
	fi.segments = nil
	return false
}

func (fi *followIterator) openSegment(seg *segment) bool {
	size := seg.size
	if !seg.sealed {
		size = seg.writeOff.Load()
	}
	if size <= 0 {
		return false
	}

	reader, err := mmapByPath(seg.path, size)
	if err != nil {
		return false
	}
	fi.reader = reader
	fi.data = reader.bytes()
	fi.offset = 0
	fi.batch = nil
	fi.batchAt = 0

	if fi.fromLSN > 0 && seg.sealed && seg.sparse.len() > 0 {
		seekOff := seg.sparse.findByLSN(fi.fromLSN)
		if seekOff >= 0 {
			fi.offset = int(seekOff)
		}
	}
	return true
}

func (fi *followIterator) waitForData() {
	select {
	case _, ok := <-fi.w.writer.newData:
		if !ok {
			fi.closed.Store(1)
		}
	case <-fi.wake:
	}
}

func (fi *followIterator) closeReader() {
	if fi.reader != nil {
		fi.reader.close()
		fi.reader = nil
		fi.data = nil
	}
}

func (fi *followIterator) close() error {
	fi.closed.Store(1)
	select {
	case fi.wake <- struct{}{}:
	default:
	}
	fi.closeReader()
	if fi.segments != nil {
		fi.w.mgr.releaseSegments(fi.segments)
		fi.segments = nil
	}
	return nil
}
