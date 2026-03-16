package uewal

// SnapshotController provides read access and compaction control
// during a [WAL.Snapshot] callback. Writes continue concurrently;
// the controller sees a consistent view of sealed segments plus a
// frozen snapshot of the active segment at the time Snapshot was called.
type SnapshotController struct {
	w          *WAL
	checkpoint LSN
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

// Compact deletes sealed segments whose LastLSN < checkpoint.
// The active segment is never deleted. Segments with active iterators
// are skipped. Returns nil if no checkpoint was set.
func (c *SnapshotController) Compact() error {
	if c.checkpoint == 0 {
		return nil
	}
	c.w.mgr.deleteBefore(c.checkpoint, c.w.hooks)
	c.w.mgr.persistManifest(c.w.lsn.current())
	return nil
}
