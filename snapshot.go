package uewal

// SnapshotController provides read access and compaction control
// during a [WAL.Snapshot] callback. Writes continue concurrently;
// the controller sees a consistent view of sealed segments plus a
// frozen snapshot of the active segment at the time Snapshot was called.
type SnapshotController struct {
	w            *WAL
	checkpoint   LSN
	checkpointTS int64
}

// Iterator returns an iterator over all events in the WAL.
func (c *SnapshotController) Iterator() (*Iterator, error) {
	return newCrossSegmentIterator(c.w.mgr, 0, c.w.cfg.compressor)
}

// IteratorFrom returns an iterator starting from the given LSN.
func (c *SnapshotController) IteratorFrom(lsn LSN) (*Iterator, error) {
	return newCrossSegmentIterator(c.w.mgr, lsn, c.w.cfg.compressor)
}

// Segments returns information about all current segments.
func (c *SnapshotController) Segments() []SegmentInfo {
	return c.w.mgr.segmentsSnapshot()
}

// Checkpoint marks the given LSN as the latest snapshot point.
// Compact will only delete sealed segments entirely before this LSN.
func (c *SnapshotController) Checkpoint(lsn LSN) {
	c.checkpoint = lsn
}

// CheckpointOlderThan marks a timestamp-based compaction point.
// [Compact] will delete sealed segments whose LastTimestamp < ts.
func (c *SnapshotController) CheckpointOlderThan(ts int64) {
	c.checkpointTS = ts
}

// Compact deletes sealed segments matching the checkpoint criteria.
// LSN-based checkpoint ([Checkpoint]) and timestamp-based checkpoint
// ([CheckpointOlderThan]) can both be set; both are applied.
// The active segment is never deleted. Segments with active iterators
// are skipped.
func (c *SnapshotController) Compact() error {
	if c.checkpoint == 0 && c.checkpointTS == 0 {
		return nil
	}
	if c.checkpoint > 0 {
		c.w.mgr.deleteBefore(c.checkpoint, c.w.hooks)
	}
	if c.checkpointTS > 0 {
		c.w.mgr.deleteOlderThan(c.checkpointTS, c.w.hooks)
	}
	c.w.mgr.persistManifest(c.w.lsn.current())
	return nil
}
