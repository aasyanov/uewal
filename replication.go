package uewal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aasyanov/uewal/internal/crc"
)

// OpenSegment opens a sealed segment for raw reading (segment shipping).
// The returned io.ReadCloser provides the raw .wal file bytes.
// Returns ErrSegmentNotFound if the segment does not exist or is active.
func (w *WAL) OpenSegment(firstLSN LSN) (io.ReadCloser, SegmentInfo, error) {
	switch w.sm.load() {
	case StateInit:
		return nil, SegmentInfo{}, ErrNotRunning
	case StateClosed:
		return nil, SegmentInfo{}, ErrClosed
	}

	seg := w.mgr.findSealed(firstLSN)
	if seg == nil {
		return nil, SegmentInfo{}, ErrSegmentNotFound
	}

	f, err := os.Open(seg.path)
	if err != nil {
		return nil, SegmentInfo{}, fmt.Errorf("%w: %w", ErrSegmentNotFound, err)
	}
	return f, seg.info(), nil
}

// ImportBatch imports a raw batch frame from a primary.
// Validates Magic and CRC. LSNs are taken from the frame header
// (not generated). The write is serialized through the writer
// goroutine to avoid races with concurrent Append calls.
func (w *WAL) ImportBatch(frame []byte) error {
	if err := w.sm.mustBeRunning(); err != nil {
		return err
	}

	if len(frame) < batchOverhead {
		return ErrImportInvalid
	}
	if frame[0] != batchMagic[0] || frame[1] != batchMagic[1] || frame[2] != batchMagic[2] || frame[3] != batchMagic[3] {
		return ErrImportInvalid
	}

	totalSize := binary.LittleEndian.Uint32(frame[batchOffSize:batchHeaderLen])
	if int(totalSize) > len(frame) || totalSize < uint32(batchOverhead) {
		return ErrImportInvalid
	}
	frameData := frame[:totalSize]

	crcOff := int(totalSize) - batchTrailerLen
	storedCRC := binary.LittleEndian.Uint32(frameData[crcOff:])
	computedCRC := crc.Checksum(frameData[:crcOff])
	if storedCRC != computedCRC {
		return ErrImportInvalid
	}

	count := binary.LittleEndian.Uint16(frameData[batchOffCount:batchOffLSN])
	if count == 0 {
		return ErrImportInvalid
	}
	firstLSN := binary.LittleEndian.Uint64(frameData[batchOffLSN:batchOffTS])
	lastLSN := firstLSN + uint64(count) - 1

	frameCopy := make([]byte, len(frameData))
	copy(frameCopy, frameData)

	barrier := make(chan struct{})
	wb := writeBatch{
		barrier:     barrier,
		importFrame: frameCopy,
	}
	if !w.queue.enqueue(wb) {
		return ErrClosed
	}
	<-barrier

	if err := w.writer.writeErr(); err != nil {
		return err
	}
	for {
		cur := w.lsn.current()
		if lastLSN <= cur {
			break
		}
		if w.lsn.val.CompareAndSwap(cur, lastLSN) {
			break
		}
	}
	w.stats.storeLSN(w.lsn.current())
	return nil
}

// ImportSegment imports a sealed segment file from a primary.
// Validates internal batch CRCs. The file is copied to the WAL
// directory and registered in the manifest.
func (w *WAL) ImportSegment(path string) error {
	if err := w.sm.mustBeRunning(); err != nil {
		return err
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("uewal: import read: %w", err)
	}

	if len(data) == 0 {
		return ErrImportInvalid
	}

	off := 0
	var firstLSN, lastLSN LSN
	var firstTS, lastTS int64
	batchCount := 0

	for off < len(data) {
		info, scanErr := scanBatchFrame(data, off)
		if scanErr != nil {
			return ErrImportInvalid
		}
		if batchCount == 0 {
			firstLSN = info.firstLSN
			firstTS = info.timestamp
		}
		if info.count > 0 {
			batchLast := info.firstLSN + uint64(info.count) - 1
			if batchLast > lastLSN {
				lastLSN = batchLast
			}
		}
		lastTS = info.timestamp
		batchCount++
		off = info.frameEnd
	}

	if firstLSN == 0 {
		return ErrImportInvalid
	}

	destName := segmentName(firstLSN)
	destPath := filepath.Join(w.dir, destName)
	if err := os.WriteFile(destPath, data, defaultFileMode); err != nil {
		return fmt.Errorf("%w: %w", ErrImportWrite, err)
	}

	idxPath := filepath.Join(w.dir, segmentIdxName(firstLSN))
	si := buildImportSparseIndex(data)
	_ = writeSparseIndex(idxPath, si)

	w.mgr.insertSealed(firstLSN, lastLSN, firstTS, lastTS, int64(len(data)), destPath)

	// Advance LSN counter if imported segment extends beyond current.
	for {
		cur := w.lsn.current()
		if lastLSN <= cur {
			break
		}
		if w.lsn.val.CompareAndSwap(cur, lastLSN) {
			break
		}
	}

	w.stats.storeLSN(w.lsn.current())
	w.stats.storeFirstLSN(firstLSN)
	w.mgr.persistManifest(w.lsn.current())

	return nil
}

func buildImportSparseIndex(data []byte) *sparseIndex {
	si := &sparseIndex{}
	off := 0
	for off < len(data) {
		info, err := scanBatchFrame(data, off)
		if err != nil {
			break
		}
		si.append(sparseEntry{
			FirstLSN:  info.firstLSN,
			Offset:    int64(off),
			Timestamp: info.timestamp,
		})
		off = info.frameEnd
	}
	return si
}
