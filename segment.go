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

// segment represents a single segment file with metadata,
// sparse index, and reference counting for safe deletion.
type segment struct {
	path     string
	firstLSN LSN
	lastLSN  LSN
	firstTS  int64
	lastTS   int64
	size     int64
	createdAt int64
	sealed   bool

	storage  Storage
	sparse   sparseIndex
	refs     atomic.Int32
	writeOff atomic.Int64 // valid data boundary, updated by writer
}

func (s *segment) info() SegmentInfo {
	return SegmentInfo{
		Path:           s.path,
		FirstLSN:       s.firstLSN,
		LastLSN:        s.lastLSN,
		FirstTimestamp: s.firstTS,
		LastTimestamp:   s.lastTS,
		Size:           s.size,
		CreatedAt:      s.createdAt,
		Sealed:         s.sealed,
	}
}

func (s *segment) ref()        { s.refs.Add(1) }
func (s *segment) unref()      { s.refs.Add(-1) }
func (s *segment) inUse() bool { return s.refs.Load() > 0 }

// seal marks the segment read-only, persists the sparse index,
// and truncates the file to the actual data size.
func (s *segment) seal(dir string, actualSize int64) error {
	s.sealed = true
	s.size = actualSize

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

func createSegment(dir string, firstLSN LSN, preallocSize int64) (*segment, error) {
	name := segmentName(firstLSN)
	path := filepath.Join(dir, name)

	storage, err := NewFileStorage(path)
	if err != nil {
		return nil, fmt.Errorf("uewal: create segment %s: %w", name, err)
	}

	if preallocSize > 0 && storage.f != nil {
		storage.f.Truncate(preallocSize)
		storage.f.Seek(0, 0)
	}

	return &segment{
		path:      path,
		firstLSN:  firstLSN,
		createdAt: time.Now().UnixNano(),
		storage:   storage,
	}, nil
}

// scanSegment reads a segment file and rebuilds metadata + sparse index
// by scanning batch headers. Returns on first invalid frame.
func scanSegment(dir string, firstLSN LSN) (*segment, error) {
	name := segmentName(firstLSN)
	path := filepath.Join(dir, name)

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	seg := &segment{
		path:     path,
		firstLSN: firstLSN,
	}

	info, _ := os.Stat(path)
	if info != nil {
		seg.createdAt = info.ModTime().UnixNano()
	}

	off := 0
	for off < len(data) {
		fi, scanErr := scanBatchFrame(data, off)
		if scanErr != nil {
			break
		}
		if fi.count > 0 {
			batchLast := fi.firstLSN + uint64(fi.count) - 1
			if batchLast > seg.lastLSN {
				seg.lastLSN = batchLast
			}
			if seg.firstTS == 0 {
				seg.firstTS = fi.timestamp
			}
			seg.lastTS = fi.timestamp
		}
		seg.sparse.append(sparseEntry{
			FirstLSN:  fi.firstLSN,
			Offset:    int64(off),
			Timestamp: fi.timestamp,
		})
		off = fi.frameEnd
	}

	seg.size = int64(off)
	return seg, nil
}
