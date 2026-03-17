package uewal

import (
	"errors"
	"fmt"
)

var errStopReplay = errors.New("stop")

// replaySegments iterates across segments, calling fn for each event with LSN >= fromLSN.
func replaySegments(mgr *segmentManager, fromLSN LSN, fn func(Event) error, decomp Compressor) error {
	segments := mgr.acquireSegments(fromLSN)
	defer mgr.releaseSegments(segments)

	decodeBuf := make([]Event, 0, 256)

	for _, seg := range segments {
		reader, cached, err := seg.mmapAcquire()
		if err != nil {
			return fmt.Errorf("%w: %w", ErrMmap, err)
		}
		data := reader.bytes()
		if len(data) == 0 {
			seg.mmapRelease(reader, cached)
			continue
		}

		off := 0
		if fromLSN > 0 {
			off = sparseSeek(&seg.sparse, seg.isSealed(), fromLSN)
		}

		var callbackErr error
		for off < len(data) {
			decodeBuf = decodeBuf[:0]
			events, next, decErr := decodeBatchFrameInto(data, off, decomp, decodeBuf)
			if decErr != nil {
				break
			}
			decodeBuf = events
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

		seg.mmapRelease(reader, cached)

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

	decodeBuf := make([]Event, 0, 256)

	for _, seg := range segments {
		reader, cached, err := seg.mmapAcquire()
		if err != nil {
			return fmt.Errorf("%w: %w", ErrMmap, err)
		}
		data := reader.bytes()
		if len(data) == 0 {
			seg.mmapRelease(reader, cached)
			continue
		}

		off := 0
		if fromLSN > 0 {
			off = sparseSeek(&seg.sparse, seg.isSealed(), fromLSN)
		}

		var callbackErr error
		for off < len(data) {
			decodeBuf = decodeBuf[:0]
			events, next, decErr := decodeBatchFrameInto(data, off, decomp, decodeBuf)
			if decErr != nil {
				break
			}
			decodeBuf = events
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

		seg.mmapRelease(reader, cached)

		if callbackErr != nil {
			if callbackErr == errStopReplay {
				return nil
			}
			return callbackErr
		}
	}

	return nil
}
