package uewal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const walExt = ".wal"

// segmentManager coordinates segment lifecycle: creation, rotation,
// retention, and lookup. All mutation happens under mu.Lock (brief).
// Readers acquire mu.RLock for consistent segment list snapshots.
type segmentManager struct {
	mu       sync.RWMutex
	dir      string
	cfg      config
	hooks    *hooksRunner
	segments []*segment
}

// openSegmentManager recovers or creates the segment directory.
// Returns the manager, recovered firstLSN, and recovered lastLSN.
func openSegmentManager(dir string, cfg config, hooks *hooksRunner, stats *statsCollector) (*segmentManager, LSN, LSN, error) {
	m := &segmentManager{dir: dir, cfg: cfg, hooks: hooks}
	firstLSN, lastLSN, err := m.recover(stats)
	if err != nil {
		return nil, 0, 0, err
	}
	return m, firstLSN, lastLSN, nil
}

func (m *segmentManager) active() *segment {
	return m.segments[len(m.segments)-1]
}

func (m *segmentManager) recover(stats *statsCollector) (LSN, LSN, error) {
	mf, err := readManifest(m.dir)
	if err == nil {
		return m.recoverFromManifest(mf, stats)
	}
	return m.recoverByScan(stats)
}

func (m *segmentManager) recoverFromManifest(mf *manifest, stats *statsCollector) (LSN, LSN, error) {
	m.segments = make([]*segment, 0, len(mf.entries))

	for _, e := range mf.entries {
		path := filepath.Join(m.dir, segmentName(e.firstLSN))
		if _, err := os.Stat(path); err != nil {
			continue
		}
		seg := &segment{
			path:      path,
			firstLSN:  e.firstLSN,
			createdAt: e.createdAt,
		}
		seg.firstTSv.Store(e.firstTS)
		seg.storeLastLSN(e.lastLSN)
		seg.lastTSv.Store(e.lastTS)
		seg.storeSize(e.size)
		if e.sealed {
			seg.sealedAt.Store(true)
			idxPath := filepath.Join(m.dir, segmentIdxName(e.firstLSN))
			if si, idxErr := readSparseIndex(idxPath); idxErr == nil {
				seg.sparse.adoptFrom(si)
			}
		}
		m.segments = append(m.segments, seg)
	}

	if len(m.segments) == 0 {
		firstLSN, err := m.createFirstSegment()
		return firstLSN, 0, err
	}

	active := m.segments[len(m.segments)-1]
	if !active.isSealed() {
		activeLast, err := m.validateActiveSegment(active, stats)
		if err != nil {
			return 0, 0, err
		}
		lastLSN := mf.lastLSN
		if activeLast > lastLSN {
			lastLSN = activeLast
		}
		return m.segments[0].firstLSN, lastLSN, nil
	}

	nextLSN := mf.lastLSN + 1
	seg, err := m.newSegment(nextLSN)
	if err != nil {
		return 0, 0, err
	}
	m.segments = append(m.segments, seg)
	return m.segments[0].firstLSN, mf.lastLSN, nil
}

func (m *segmentManager) recoverByScan(stats *statsCollector) (LSN, LSN, error) {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return 0, 0, fmt.Errorf("%w: %w", ErrScanDir, err)
	}

	var walFiles []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == walExt {
			walFiles = append(walFiles, e.Name())
		}
	}
	sort.Strings(walFiles)

	if len(walFiles) == 0 {
		firstLSN, err := m.createFirstSegment()
		return firstLSN, 0, err
	}

	m.segments = make([]*segment, 0, len(walFiles))
	var lastLSN LSN

	for i, name := range walFiles {
		fLSN, ok := parseSegmentLSN(name)
		if !ok {
			continue
		}
		seg, scanErr := scanSegment(m.dir, fLSN)
		if scanErr != nil {
			continue
		}
		if i < len(walFiles)-1 {
			seg.sealedAt.Store(true)
			idxPath := filepath.Join(m.dir, segmentIdxName(fLSN))
			_ = writeSparseIndex(idxPath, &seg.sparse)
		}
		if seg.loadLastLSN() > lastLSN {
			lastLSN = seg.loadLastLSN()
		}
		m.segments = append(m.segments, seg)
	}

	if len(m.segments) == 0 {
		firstLSN, err := m.createFirstSegment()
		return firstLSN, 0, err
	}

	active := m.segments[len(m.segments)-1]
	if !active.isSealed() {
		validLSN, err := m.validateActiveSegment(active, stats)
		if err != nil {
			return 0, 0, err
		}
		if validLSN > lastLSN {
			lastLSN = validLSN
		}
	}

	mf := buildManifest(m.segments, lastLSN)
	_ = writeManifest(m.dir, mf)

	firstLSN := m.segments[0].firstLSN
	return firstLSN, lastLSN, nil
}

func (m *segmentManager) createFirstSegment() (LSN, error) {
	firstLSN := LSN(1)
	if m.cfg.startLSN > 0 {
		firstLSN = m.cfg.startLSN
	}
	seg, err := m.newSegment(firstLSN)
	if err != nil {
		return 0, err
	}
	m.segments = append(m.segments, seg)
	return firstLSN, nil
}

func (m *segmentManager) newSegment(firstLSN LSN) (*segment, error) {
	var prealloc int64
	if m.cfg.preallocate {
		prealloc = m.cfg.maxSegmentSize
	}
	return createSegment(m.dir, firstLSN, prealloc, m.cfg.maxSegmentSize, m.cfg.storageFactory)
}

func (m *segmentManager) validateActiveSegment(seg *segment, stats *statsCollector) (LSN, error) {
	fi, err := os.Stat(seg.path)
	if err != nil {
		return 0, err
	}
	fileSize := fi.Size()
	if fileSize == 0 {
		var storage Storage
		var openErr error
		if m.cfg.storageFactory != nil {
			storage, openErr = m.cfg.storageFactory(seg.path)
		} else {
			storage, openErr = NewFileStorage(seg.path)
		}
		if openErr != nil {
			return 0, openErr
		}
		seg.storage = storage
		seg.storeSize(0)
		seg.writeOff.Store(0)
		return 0, nil
	}

	reader, err := mmapByPath(seg.path, fileSize)
	if err != nil {
		return 0, err
	}
	data := reader.bytes()

	off := 0
	var lastLSN LSN
	lastValid := 0
	corrupted := false

	seg.sparse.reset()
	seg.sparse.ensureCapacity(estimateSparseCapacity(fileSize))

	for off < len(data) {
		info, scanErr := scanBatchFrame(data, off)
		if scanErr != nil {
			if off < len(data) {
				corrupted = true
			}
			break
		}
		if info.count > 0 {
			batchLast := info.firstLSN + uint64(info.count) - 1
			if batchLast > lastLSN {
				lastLSN = batchLast
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
		lastValid = info.frameEnd
		off = info.frameEnd
	}

	reader.close()

	if corrupted {
		stats.addCorruption()
		m.hooks.onCorruption(seg.path, int64(lastValid))
	}

	var storage Storage
	var openErr error
	if m.cfg.storageFactory != nil {
		storage, openErr = m.cfg.storageFactory(seg.path)
	} else {
		storage, openErr = NewFileStorage(seg.path)
	}
	if openErr != nil {
		return 0, openErr
	}
	if err := storage.Truncate(int64(lastValid)); err != nil {
		storage.Close()
		return 0, err
	}
	seg.storage = storage
	seg.storeLastLSN(lastLSN)
	seg.storeSize(int64(lastValid))
	seg.writeOff.Store(int64(lastValid))
	seg.sparse.publish()

	return lastLSN, nil
}

// rotate seals the active segment, creates a new one, runs retention,
// and persists the manifest. Called from the writer goroutine.
func (m *segmentManager) rotate(lastLSN LSN, writeOffset int64) (*segment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	active := m.segments[len(m.segments)-1]

	active.storeLastLSN(lastLSN)
	active.storeSize(writeOffset)
	if err := active.seal(m.dir, writeOffset); err != nil {
		return nil, fmt.Errorf("uewal: seal segment: %w", err)
	}
	if active.storage != nil {
		active.storage.Close()
		active.storage = nil
	}
	m.hooks.onRotation(active.info())

	nextLSN := lastLSN + 1
	newSeg, err := m.newSegment(nextLSN)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCreateSegment, err)
	}
	m.segments = append(m.segments, newSeg)

	m.applyRetention()

	mf := buildManifest(m.segments, lastLSN)
	_ = writeManifest(m.dir, mf)

	return newSeg, nil
}

func (m *segmentManager) applyRetention() {
	if len(m.segments) <= 1 {
		return
	}

	now := time.Now()
	totalSize := m.totalSizeUnsafe()
	segCount := len(m.segments)

	kept := make([]*segment, 0, len(m.segments))
	for i, seg := range m.segments {
		if i == len(m.segments)-1 {
			kept = append(kept, seg)
			continue
		}
		if seg.inUse() {
			kept = append(kept, seg)
			continue
		}

		shouldDelete := false
		if m.cfg.maxSegments > 0 && segCount > m.cfg.maxSegments {
			shouldDelete = true
		}
		if m.cfg.retentionAge > 0 && seg.createdAt > 0 {
			if now.Sub(time.Unix(0, seg.createdAt)) > m.cfg.retentionAge {
				shouldDelete = true
			}
		}
		if m.cfg.retentionSize > 0 && totalSize > m.cfg.retentionSize {
			shouldDelete = true
		}

		if shouldDelete {
			seg.deleteFiles(m.dir)
			m.hooks.onDelete(seg.info())
			segCount--
			totalSize -= seg.sizeAt.Load()
		} else {
			kept = append(kept, seg)
		}
	}
	m.segments = kept
}

func (m *segmentManager) totalSizeUnsafe() int64 {
	var total int64
	for _, s := range m.segments {
		total += s.sizeAt.Load()
	}
	return total
}

// acquireSegments returns segments containing LSN >= fromLSN with ref
// counts incremented. Caller must call releaseSegments when done.
func (m *segmentManager) acquireSegments(fromLSN LSN) []*segment {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.segments) == 0 {
		return nil
	}

	startIdx := 0
	if fromLSN > 0 {
		for i := len(m.segments) - 1; i >= 0; i-- {
			if m.segments[i].firstLSN <= fromLSN {
				startIdx = i
				break
			}
		}
	}

	result := make([]*segment, 0, len(m.segments)-startIdx)
	for i := startIdx; i < len(m.segments); i++ {
		seg := m.segments[i]
		seg.ref()
		result = append(result, seg)
	}
	return result
}

func (m *segmentManager) releaseSegments(segs []*segment) {
	for _, s := range segs {
		s.unref()
	}
}

// segmentsSnapshot returns a copy of SegmentInfo for all segments.
func (m *segmentManager) segmentsSnapshot() []SegmentInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]SegmentInfo, len(m.segments))
	for i, s := range m.segments {
		out[i] = s.info()
	}
	return out
}

func (m *segmentManager) totalSize() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.totalSizeUnsafe()
}

func (m *segmentManager) segmentCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.segments)
}

// persistManifest writes the current state to manifest.bin.
func (m *segmentManager) persistManifest(lastLSN LSN) {
	m.mu.RLock()
	mf := buildManifest(m.segments, lastLSN)
	m.mu.RUnlock()
	_ = writeManifest(m.dir, mf)
}

// findSealed returns a sealed segment by firstLSN, or nil.
func (m *segmentManager) findSealed(firstLSN LSN) *segment {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, seg := range m.segments {
		if seg.firstLSN == firstLSN && seg.isSealed() {
			return seg
		}
	}
	return nil
}

// insertSealed adds a sealed segment to the segment list in sorted order.
func (m *segmentManager) insertSealed(firstLSN, lastLSN LSN, firstTS, lastTS, size int64, path string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	seg := &segment{
		path:      path,
		firstLSN:  firstLSN,
		createdAt: firstTS,
	}
	seg.firstTSv.Store(firstTS)
	seg.storeLastLSN(lastLSN)
	seg.lastTSv.Store(lastTS)
	seg.storeSize(size)
	seg.sealedAt.Store(true)

	idxPath := filepath.Join(m.dir, segmentIdxName(firstLSN))
	if si, err := readSparseIndex(idxPath); err == nil {
		seg.sparse.adoptFrom(si)
	}

	// Insert in sorted order among sealed segments.
	// Active segment is always last; never insert after it.
	sealedCount := len(m.segments)
	if sealedCount > 0 && !m.segments[sealedCount-1].isSealed() {
		sealedCount--
	}
	pos := sealedCount
	for i := 0; i < sealedCount; i++ {
		if m.segments[i].firstLSN > firstLSN {
			pos = i
			break
		}
	}

	updated := make([]*segment, 0, len(m.segments)+1)
	updated = append(updated, m.segments[:pos]...)
	updated = append(updated, seg)
	updated = append(updated, m.segments[pos:]...)
	m.segments = updated
}

// deleteBefore removes sealed segments whose LastLSN < lsn.
// Skips segments with active iterators. Never deletes the active segment.
func (m *segmentManager) deleteBefore(lsn LSN, hooks hooksRunner) {
	m.mu.Lock()
	defer m.mu.Unlock()

	kept := make([]*segment, 0, len(m.segments))
	for i, seg := range m.segments {
		if i == len(m.segments)-1 {
			kept = append(kept, seg)
			continue
		}
		if seg.inUse() || seg.loadLastLSN() >= lsn {
			kept = append(kept, seg)
			continue
		}
		seg.deleteFiles(m.dir)
		hooks.onDelete(seg.info())
	}
	m.segments = kept
}

// deleteOlderThan removes sealed segments whose LastTimestamp < ts.
// Skips segments with active iterators. Never deletes the active segment.
func (m *segmentManager) deleteOlderThan(ts int64, hooks hooksRunner) {
	m.mu.Lock()
	defer m.mu.Unlock()

	kept := make([]*segment, 0, len(m.segments))
	for i, seg := range m.segments {
		if i == len(m.segments)-1 {
			kept = append(kept, seg)
			continue
		}
		if seg.inUse() || seg.lastTSv.Load() >= ts {
			kept = append(kept, seg)
			continue
		}
		seg.deleteFiles(m.dir)
		hooks.onDelete(seg.info())
	}
	m.segments = kept
}

// closeActive syncs and closes the active segment's storage.
func (m *segmentManager) closeActive() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.segments) == 0 {
		return nil
	}
	active := m.segments[len(m.segments)-1]
	if active.storage != nil {
		if err := active.storage.Sync(); err != nil {
			return err
		}
		return active.storage.Close()
	}
	return nil
}
