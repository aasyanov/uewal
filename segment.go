package uewal

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	idxExt          = ".idx"
	lockFileName    = "LOCK"
	segmentNameFmt  = "%020d" + walExt
	idxNameFmt      = "%020d" + idxExt
	defaultFileMode = os.FileMode(0644)
	defaultDirMode  = os.FileMode(0755)
	manifestTmpExt  = ".tmp"
)

func segmentName(firstLSN LSN) string {
	return fmt.Sprintf(segmentNameFmt, firstLSN)
}

func segmentIdxName(firstLSN LSN) string {
	return fmt.Sprintf(idxNameFmt, firstLSN)
}

func parseSegmentLSN(name string) (LSN, bool) {
	if !strings.HasSuffix(name, walExt) {
		return 0, false
	}
	base := strings.TrimSuffix(name, walExt)
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

	// Cached mmap reader for sealed segments. Protected by cachedMu.
	cachedMu     sync.Mutex
	cachedReader *mmapReader
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

// mmapAcquire returns an mmap reader for the segment.
// For sealed segments the reader is cached and must NOT be closed by the caller.
// For active segments a fresh reader is created; the caller must close it via mmapRelease.
// The second return value indicates whether the reader is cached.
func (s *segment) mmapAcquire() (*mmapReader, bool, error) {
	size := s.sizeAt.Load()
	if !s.isSealed() {
		size = s.writeOff.Load()
	}
	if size <= 0 {
		return &mmapReader{}, false, nil
	}

	if s.isSealed() {
		s.cachedMu.Lock()
		if s.cachedReader != nil {
			r := s.cachedReader
			s.cachedMu.Unlock()
			return r, true, nil
		}
		r, err := mmapByPath(s.path, size)
		if err != nil {
			s.cachedMu.Unlock()
			return nil, false, err
		}
		s.cachedReader = r
		s.cachedMu.Unlock()
		return r, true, nil
	}

	r, err := mmapByPath(s.path, size)
	if err != nil {
		return nil, false, err
	}
	return r, false, nil
}

// mmapRelease releases an mmap reader obtained via mmapAcquire.
// Cached readers (from sealed segments) are retained; uncached ones are closed.
func (s *segment) mmapRelease(r *mmapReader, cached bool) {
	if !cached && r != nil {
		r.close()
	}
}

// closeCache releases the cached mmap reader if any.
func (s *segment) closeCache() {
	s.cachedMu.Lock()
	if s.cachedReader != nil {
		s.cachedReader.close()
		s.cachedReader = nil
	}
	s.cachedMu.Unlock()
}

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
	s.closeCache()
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
			_, _ = fs.f.Seek(0, io.SeekStart)
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
