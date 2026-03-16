package uewal

import "errors"

var errStopReplay = errors.New("stop")

// replaySegments iterates across segments, calling fn for each event with LSN >= fromLSN.
func replaySegments(mgr *segmentManager, fromLSN LSN, fn func(Event) error, decomp Compressor) error {
	segments := mgr.acquireSegments(fromLSN)
	defer mgr.releaseSegments(segments)

	for _, seg := range segments {
		size := seg.sizeAt.Load()
		if !seg.isSealed() {
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

		if fromLSN > 0 && seg.isSealed() && seg.sparse.len() > 0 {
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
		size := seg.sizeAt.Load()
		if !seg.isSealed() {
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

		if fromLSN > 0 && seg.isSealed() && seg.sparse.len() > 0 {
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
