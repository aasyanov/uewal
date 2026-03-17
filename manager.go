package uewal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const walExt = ".wal"

// segmentManager coordinates segment lifecycle: creation, rotation,
// retention, and lookup. All mutation happens under mu.Lock (brief).
// Readers acquire mu.RLock for consistent segment list snapshots.
type segmentManager struct {
	mu          sync.RWMutex
	dir         string
	cfg         config
	hooks       *hooksRunner
	stats       *statsCollector
	segments    []*segment
	manifestBuf []byte // reused buffer for manifest serialization
}

// openSegmentManager recovers or creates the segment directory.
// Returns the manager, recovered firstLSN, recovered lastLSN, and recovery info.
func openSegmentManager(dir string, cfg config, hooks *hooksRunner, stats *statsCollector) (*segmentManager, LSN, LSN, RecoveryInfo, error) {
	m := &segmentManager{dir: dir, cfg: cfg, hooks: hooks, stats: stats}
	firstLSN, lastLSN, ri, err := m.recover(stats)
	if err != nil {
		return nil, 0, 0, RecoveryInfo{}, err
	}
	return m, firstLSN, lastLSN, ri, nil
}

func (m *segmentManager) active() *segment {
	return m.segments[len(m.segments)-1]
}

func (m *segmentManager) recover(stats *statsCollector) (LSN, LSN, RecoveryInfo, error) {
	mf, err := readManifest(m.dir)
	if err == nil {
		return m.recoverFromManifest(mf, stats)
	}
	return m.recoverByScan(stats)
}

func (m *segmentManager) recoverFromManifest(mf *manifest, stats *statsCollector) (LSN, LSN, RecoveryInfo, error) {
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
		}
		m.segments = append(m.segments, seg)
	}

	m.removeOrphanFiles()

	// Load sparse indexes and pre-warm mmap caches for sealed segments concurrently.
	if len(m.segments) > 1 {
		var wg sync.WaitGroup
		for _, seg := range m.segments {
			if !seg.isSealed() {
				continue
			}
			wg.Add(1)
			go func(s *segment) {
				defer wg.Done()
				idxPath := filepath.Join(m.dir, segmentIdxName(s.firstLSN))
				if si, err := readSparseIndex(idxPath); err == nil {
					s.sparse.adoptFrom(si)
				}
				s.warmCache()
			}(seg)
		}
		wg.Wait()
	}

	ri := RecoveryInfo{SegmentCount: len(m.segments)}

	if len(m.segments) == 0 {
		firstLSN, err := m.createFirstSegment()
		return firstLSN, 0, ri, err
	}

	active := m.segments[len(m.segments)-1]
	if !active.isSealed() {
		activeLast, truncated, corrupted, err := m.validateActiveSegment(active, stats)
		if err != nil {
			return 0, 0, ri, err
		}
		ri.TruncatedBytes = truncated
		ri.Corrupted = corrupted
		lastLSN := mf.lastLSN
		if activeLast > lastLSN {
			lastLSN = activeLast
		}
		return m.segments[0].firstLSN, lastLSN, ri, nil
	}

	nextLSN := mf.lastLSN + 1
	seg, err := m.newSegment(nextLSN)
	if err != nil {
		return 0, 0, ri, err
	}
	m.segments = append(m.segments, seg)
	return m.segments[0].firstLSN, mf.lastLSN, ri, nil
}

func (m *segmentManager) recoverByScan(stats *statsCollector) (LSN, LSN, RecoveryInfo, error) {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return 0, 0, RecoveryInfo{}, fmt.Errorf("%w: %w", ErrScanDir, err)
	}

	var walFiles []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == walExt {
			walFiles = append(walFiles, e.Name())
		}
	}
	sort.Strings(walFiles)

	if len(walFiles) == 0 {
		firstLSN, createErr := m.createFirstSegment()
		return firstLSN, 0, RecoveryInfo{}, createErr
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
		firstLSN, createErr := m.createFirstSegment()
		return firstLSN, 0, RecoveryInfo{}, createErr
	}

	var ri RecoveryInfo
	ri.SegmentCount = len(m.segments)

	active := m.segments[len(m.segments)-1]
	if !active.isSealed() {
		validLSN, truncated, corrupted, valErr := m.validateActiveSegment(active, stats)
		if valErr != nil {
			return 0, 0, ri, valErr
		}
		ri.TruncatedBytes = truncated
		ri.Corrupted = corrupted
		if validLSN > lastLSN {
			lastLSN = validLSN
		}
	}

	mf := buildManifest(m.segments, lastLSN)
	m.manifestBuf = mf.marshalInto(m.manifestBuf)
	_ = writeManifestBytes(m.dir, m.manifestBuf)

	firstLSN := m.segments[0].firstLSN
	return firstLSN, lastLSN, ri, nil
}

// removeOrphanFiles deletes .wal, .idx, and .tmp files that are not tracked
// by the manifest. This handles the case where ImportSegment or
// writeSparseIndex wrote a file but crashed before the manifest was persisted.
func (m *segmentManager) removeOrphanFiles() {
	known := make(map[LSN]struct{}, len(m.segments))
	for _, seg := range m.segments {
		known[seg.firstLSN] = struct{}{}
	}

	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		ext := filepath.Ext(name)

		// Remove stale .tmp files from interrupted atomic writes.
		if ext == manifestTmpExt {
			os.Remove(filepath.Join(m.dir, name))
			continue
		}

		if ext == walExt {
			fLSN, ok := parseSegmentLSN(name)
			if !ok {
				continue
			}
			if _, tracked := known[fLSN]; tracked {
				continue
			}
			os.Remove(filepath.Join(m.dir, name))
			os.Remove(filepath.Join(m.dir, segmentIdxName(fLSN)))
			continue
		}

		// Remove orphan .idx files whose .wal is not tracked.
		if ext == idxExt {
			walName := strings.TrimSuffix(name, idxExt) + walExt
			fLSN, ok := parseSegmentLSN(walName)
			if !ok {
				continue
			}
			if _, tracked := known[fLSN]; !tracked {
				os.Remove(filepath.Join(m.dir, name))
			}
		}
	}
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

func (m *segmentManager) validateActiveSegment(seg *segment, stats *statsCollector) (LSN, int64, bool, error) {
	// Open storage first — single file open for both validation and subsequent writes.
	var storage Storage
	var openErr error
	if m.cfg.storageFactory != nil {
		storage, openErr = m.cfg.storageFactory(seg.path)
	} else {
		storage, openErr = NewFileStorage(seg.path)
	}
	if openErr != nil {
		return 0, 0, false, openErr
	}

	fi, statErr := os.Stat(seg.path)
	if statErr != nil {
		storage.Close()
		return 0, 0, false, statErr
	}
	fileSize := fi.Size()
	if fileSize == 0 {
		seg.storage = storage
		seg.storeSize(0)
		seg.writeOff.Store(0)
		return 0, 0, false, nil
	}

	// Memory-map the file for validation scan instead of os.ReadFile.
	// This avoids a fileSize-sized heap allocation during recovery.
	reader, mmapErr := mmapByPath(seg.path, fileSize)
	if mmapErr != nil {
		storage.Close()
		return 0, 0, false, mmapErr
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

	truncated := fileSize - int64(lastValid)
	if corrupted {
		stats.addCorruption()
		m.hooks.onCorruption(seg.path, int64(lastValid))
	}

	if truncErr := storage.Truncate(int64(lastValid)); truncErr != nil {
		storage.Close()
		return 0, 0, false, truncErr
	}
	seg.storage = storage
	seg.storeLastLSN(lastLSN)
	seg.storeSize(int64(lastValid))
	seg.writeOff.Store(int64(lastValid))
	seg.sparse.publish()

	return lastLSN, truncated, corrupted, nil
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
	m.manifestBuf = mf.marshalInto(m.manifestBuf)
	_ = writeManifestBytes(m.dir, m.manifestBuf)

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
			sz := seg.sizeAt.Load()
			seg.deleteFiles(m.dir)
			m.hooks.onDelete(seg.info())
			if m.stats != nil {
				m.stats.addRetentionDeleted(1, uint64(sz))
			}
			segCount--
			totalSize -= sz
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
// Sealed segments have their mmap caches pre-warmed concurrently.
func (m *segmentManager) acquireSegments(fromLSN LSN) []*segment {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.segments) == 0 {
		return nil
	}

	startIdx := 0
	if fromLSN > 0 {
		n := len(m.segments)
		idx := sort.Search(n, func(i int) bool {
			return m.segments[i].firstLSN > fromLSN
		})
		if idx > 0 {
			startIdx = idx - 1
		}
	}

	count := len(m.segments) - startIdx
	result := getSegSlice(count)

	// Count sealed segments that need warming (no cached reader yet).
	needWarm := 0
	for i := startIdx; i < len(m.segments); i++ {
		m.segments[i].ref()
		result = append(result, m.segments[i])
		if m.segments[i].isSealed() {
			m.segments[i].cachedMu.Lock()
			cold := m.segments[i].cachedReader == nil
			m.segments[i].cachedMu.Unlock()
			if cold {
				needWarm++
			}
		}
	}

	if needWarm > 1 {
		var wg sync.WaitGroup
		for _, seg := range result {
			if !seg.isSealed() {
				continue
			}
			seg.cachedMu.Lock()
			cold := seg.cachedReader == nil
			seg.cachedMu.Unlock()
			if !cold {
				continue
			}
			wg.Add(1)
			go func(s *segment) {
				defer wg.Done()
				s.warmCache()
			}(seg)
		}
		wg.Wait()
	} else if needWarm == 1 {
		for _, seg := range result {
			if seg.isSealed() {
				seg.warmCache()
				break
			}
		}
	}

	return result
}

var segSlicePool = sync.Pool{
	New: func() any {
		s := make([]*segment, 0, 16)
		return &s
	},
}

func getSegSlice(hint int) []*segment {
	sp := segSlicePool.Get().(*[]*segment)
	s := (*sp)[:0]
	if cap(s) < hint {
		segSlicePool.Put(sp)
		return make([]*segment, 0, hint)
	}
	return s
}

func putSegSlice(s []*segment) {
	if cap(s) > 256 {
		return
	}
	clear(s)
	s = s[:0]
	segSlicePool.Put(&s)
}

func (m *segmentManager) releaseSegments(segs []*segment) {
	for _, s := range segs {
		s.unref()
	}
	putSegSlice(segs)
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
	m.mu.Lock()
	mf := buildManifest(m.segments, lastLSN)
	m.manifestBuf = mf.marshalInto(m.manifestBuf)
	m.mu.Unlock()
	_ = writeManifestBytes(m.dir, m.manifestBuf)
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
		sz := seg.sizeAt.Load()
		seg.deleteFiles(m.dir)
		hooks.onDelete(seg.info())
		if m.stats != nil {
			m.stats.addRetentionDeleted(1, uint64(sz))
		}
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
		sz := seg.sizeAt.Load()
		seg.deleteFiles(m.dir)
		hooks.onDelete(seg.info())
		if m.stats != nil {
			m.stats.addRetentionDeleted(1, uint64(sz))
		}
	}
	m.segments = kept
}

// closeActive syncs and closes the active segment's storage.
func (m *segmentManager) closeActive() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, seg := range m.segments {
		seg.closeCache()
	}
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
