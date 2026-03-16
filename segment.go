package uewal

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

func segmentName(firstLSN LSN) string {
	return fmt.Sprintf("%020d.wal", firstLSN)
}

func segmentIdxName(firstLSN LSN) string {
	return fmt.Sprintf("%020d.idx", firstLSN)
}

func parseSegmentLSN(name string) (LSN, bool) {
	if !strings.HasSuffix(name, ".wal") {
		return 0, false
	}
	base := strings.TrimSuffix(name, ".wal")
	lsn, err := strconv.ParseUint(base, 10, 64)
	if err != nil || lsn == 0 {
		return 0, false
	}
	return lsn, true
}

// SegmentInfo describes a WAL segment file.
// Used in [Hooks] callbacks and [SnapshotController.Segments].
type SegmentInfo struct {
	Path           string
	FirstLSN       LSN
	LastLSN        LSN
	FirstTimestamp int64
	LastTimestamp   int64
	Size           int64
	CreatedAt      int64 // UnixNano
	Sealed         bool
}

// segment represents a single segment file with metadata,
// sparse index, and reference counting for safe deletion.
//
// Immutable fields (path, firstLSN, createdAt) are set once at creation.
// All other metadata fields use atomics for concurrent access by the writer
// goroutine and iterator/Segments() callers.
type segment struct {
	path      string // immutable
	firstLSN  LSN    // immutable
	createdAt int64  // immutable

	firstTSv atomic.Int64
	sealedAt atomic.Bool
	lastLSNv atomic.Uint64
	lastTSv  atomic.Int64
	sizeAt   atomic.Int64

	storage  Storage
	sparse   sparseIndex
	refs     atomic.Int32
	writeOff atomic.Int64 // valid data boundary, updated by writer
}

func (s *segment) info() SegmentInfo {
	return SegmentInfo{
		Path:           s.path,
		FirstLSN:       s.firstLSN,
		LastLSN:        s.loadLastLSN(),
		FirstTimestamp: s.firstTSv.Load(),
		LastTimestamp:   s.lastTSv.Load(),
		Size:           s.sizeAt.Load(),
		CreatedAt:      s.createdAt,
		Sealed:         s.isSealed(),
	}
}

func (s *segment) isSealed() bool        { return s.sealedAt.Load() }
func (s *segment) loadLastLSN() LSN      { return s.lastLSNv.Load() }
func (s *segment) storeLastLSN(v LSN)    { s.lastLSNv.Store(v) }
func (s *segment) storeLastTS(v int64)   { s.lastTSv.Store(v) }
func (s *segment) storeSize(v int64)     { s.sizeAt.Store(v) }

func (s *segment) ref()        { s.refs.Add(1) }
func (s *segment) unref()      { s.refs.Add(-1) }
func (s *segment) inUse() bool { return s.refs.Load() > 0 }

// seal marks the segment read-only, persists the sparse index,
// and truncates the file to the actual data size.
func (s *segment) seal(dir string, actualSize int64) error {
	s.sizeAt.Store(actualSize)
	s.sealedAt.Store(true)

	if s.storage != nil {
		if err := s.storage.Sync(); err != nil {
			return err
		}
		if err := s.storage.Truncate(actualSize); err != nil {
			return err
		}
	}

	idxPath := filepath.Join(dir, segmentIdxName(s.firstLSN))
	return writeSparseIndex(idxPath, &s.sparse)
}

func (s *segment) deleteFiles(dir string) {
	if s.storage != nil {
		s.storage.Close()
		s.storage = nil
	}
	os.Remove(filepath.Join(dir, segmentName(s.firstLSN)))
	os.Remove(filepath.Join(dir, segmentIdxName(s.firstLSN)))
}

func createSegment(dir string, firstLSN LSN, preallocSize int64, maxSegmentSize int64, sf StorageFactory) (*segment, error) {
	name := segmentName(firstLSN)
	path := filepath.Join(dir, name)

	var storage Storage
	var err error
	if sf != nil {
		storage, err = sf(path)
	} else {
		storage, err = NewFileStorage(path)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrCreateSegment, name, err)
	}

	if preallocSize > 0 {
		if fs, ok := storage.(*FileStorage); ok && fs.f != nil {
			_ = fs.f.Truncate(preallocSize)
			_, _ = fs.f.Seek(0, 0)
		}
	}

	seg := &segment{
		path:      path,
		firstLSN:  firstLSN,
		createdAt: time.Now().UnixNano(),
		storage:   storage,
	}
	seg.sparse.ensureCapacity(estimateSparseCapacity(maxSegmentSize))
	return seg, nil
}

// scanSegment reads a segment file and rebuilds metadata + sparse index
// by scanning batch headers via mmap. Returns on first invalid frame.
func scanSegment(dir string, firstLSN LSN) (*segment, error) {
	name := segmentName(firstLSN)
	path := filepath.Join(dir, name)

	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	size := fi.Size()

	seg := &segment{
		path:      path,
		firstLSN:  firstLSN,
		createdAt: fi.ModTime().UnixNano(),
	}

	if size == 0 {
		return seg, nil
	}

	reader, err := mmapByPath(path, size)
	if err != nil {
		return nil, err
	}
	data := reader.bytes()
	defer reader.close()

	seg.sparse.ensureCapacity(estimateSparseCapacity(size))

	off := 0
	for off < len(data) {
		info, scanErr := scanBatchFrame(data, off)
		if scanErr != nil {
			break
		}
		if info.count > 0 {
			batchLast := info.firstLSN + uint64(info.count) - 1
			if batchLast > seg.loadLastLSN() {
				seg.storeLastLSN(batchLast)
			}
			if seg.firstTSv.Load() == 0 {
				seg.firstTSv.Store(info.timestamp)
			}
			seg.storeLastTS(info.timestamp)
		}
		seg.sparse.append(sparseEntry{
			FirstLSN:  info.firstLSN,
			Offset:    int64(off),
			Timestamp: info.timestamp,
		})
		off = info.frameEnd
	}

	seg.storeSize(int64(off))
	seg.sparse.publish()
	return seg, nil
}
