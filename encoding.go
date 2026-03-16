package uewal

import (
	"encoding/binary"
	"fmt"

	"github.com/aasyanov/uewal/internal/crc"
)

// Wire format v1 — batch frame layout:
//
//	┌──────────────────────────────────────────────────┐
//	│ BatchHeader                             28B      │
//	│   Magic        4B  "EWAL"                        │
//	│   Version      1B  =1                            │
//	│   Flags        1B  feature flags                 │
//	│   RecordCount  2B  uint16                        │
//	│   FirstLSN     8B  uint64 (8B aligned)           │
//	│   Timestamp    8B  int64 UnixNano (8B aligned)   │
//	│   BatchSize    4B  uint32 total frame incl. CRC  │
//	├──────────────────────────────────────────────────┤
//	│ Records region (possibly compressed):            │
//	│   [Timestamp 8B]  ← only if per_record_ts=1     │
//	│   KeyLen     2B                                  │
//	│   MetaLen    2B                                  │
//	│   PayloadLen 4B                                  │
//	│   Key        KeyLen bytes                        │
//	│   Meta       MetaLen bytes                       │
//	│   Payload    PayloadLen bytes                    │
//	├──────────────────────────────────────────────────┤
//	│ CRC32C                                   4B      │
//	└──────────────────────────────────────────────────┘
//
// CRC-32C covers bytes [0 .. BatchSize-5].
// All multi-byte integers are little-endian.

var batchMagic = [4]byte{'E', 'W', 'A', 'L'}

const (
	batchVersion    uint8 = 1
	batchHeaderLen        = 28
	batchTrailerLen       = 4
	batchOverhead         = batchHeaderLen + batchTrailerLen // 32

	flagCompressed  uint8 = 1 << 0
	flagPerRecordTS uint8 = 1 << 1
)

// recordFixedLen returns the fixed overhead per record.
func recordFixedLen(perRecTS bool) int {
	if perRecTS {
		return 16 // Timestamp 8B + KeyLen 2B + MetaLen 2B + PayloadLen 4B
	}
	return 8 // KeyLen 2B + MetaLen 2B + PayloadLen 4B
}

func recordWireSize(r *record, perRecTS bool) int {
	return recordFixedLen(perRecTS) + len(r.key) + len(r.meta) + len(r.payload)
}

func recordsRegionSize(recs []record, perRecTS bool) int {
	n := 0
	for i := range recs {
		n += recordWireSize(&recs[i], perRecTS)
	}
	return n
}

// uniformTimestamp returns true if all records share the same timestamp.
func uniformTimestamp(recs []record) bool {
	if len(recs) <= 1 {
		return true
	}
	ts := recs[0].timestamp
	for i := 1; i < len(recs); i++ {
		if recs[i].timestamp != ts {
			return false
		}
	}
	return true
}

// encodeRecordsRegion writes records into dst. Returns bytes written.
func encodeRecordsRegion(dst []byte, recs []record, perRecTS bool) int {
	off := 0
	for i := range recs {
		r := &recs[i]
		if perRecTS {
			binary.LittleEndian.PutUint64(dst[off:], uint64(r.timestamp))
			off += 8
		}
		binary.LittleEndian.PutUint16(dst[off:], uint16(len(r.key)))
		binary.LittleEndian.PutUint16(dst[off+2:], uint16(len(r.meta)))
		binary.LittleEndian.PutUint32(dst[off+4:], uint32(len(r.payload)))
		off += 8
		if len(r.key) > 0 {
			off += copy(dst[off:], r.key)
		}
		if len(r.meta) > 0 {
			off += copy(dst[off:], r.meta)
		}
		off += copy(dst[off:], r.payload)
	}
	return off
}

// encodeBatchFrame writes a complete batch frame into dst.
// Returns the final frame slice and total size. dst must be large enough
// for the uncompressed frame; if compression enlarges the output the
// function falls back to uncompressed (auto-bypass).
func encodeBatchFrame(dst []byte, recs []record, firstLSN LSN, comp Compressor, noCompress bool) ([]byte, int, error) {
	return encodeBatchFrameEx(dst, recs, firstLSN, comp, noCompress, -1, false)
}

// encodeBatchFrameEx is like encodeBatchFrame but accepts pre-computed values
// to avoid redundant passes over recs. Pass recRegionSizeHint=-1 to compute.
func encodeBatchFrameEx(dst []byte, recs []record, firstLSN LSN, comp Compressor, noCompress bool, recRegionSizeHint int, perRecTSKnown bool) ([]byte, int, error) {
	var perRecTS bool
	if recRegionSizeHint >= 0 {
		perRecTS = perRecTSKnown
	} else {
		perRecTS = !uniformTimestamp(recs)
	}
	batchTS := recs[0].timestamp

	var recRegionSize int
	if recRegionSizeHint >= 0 {
		recRegionSize = recRegionSizeHint
	} else {
		recRegionSize = recordsRegionSize(recs, perRecTS)
	}
	uncompressedTotal := batchOverhead + recRegionSize

	// Ensure dst fits uncompressed frame.
	if cap(dst) < uncompressedTotal {
		dst = make([]byte, uncompressedTotal)
	} else {
		dst = dst[:uncompressedTotal]
	}

	// Encode records region after header.
	encodeRecordsRegion(dst[batchHeaderLen:], recs, perRecTS)
	recordsData := dst[batchHeaderLen : batchHeaderLen+recRegionSize]

	var flags uint8
	if perRecTS {
		flags |= flagPerRecordTS
	}

	finalRecords := recordsData

	if comp != nil && !noCompress {
		compressed, err := comp.Compress(recordsData)
		if err != nil {
			return nil, 0, err
		}
		if len(compressed) < len(recordsData) {
			flags |= flagCompressed
			finalRecords = compressed
		}
	}

	totalSize := batchHeaderLen + len(finalRecords) + batchTrailerLen

	// Re-allocate if compression made it larger (shouldn't happen with auto-bypass, but safety).
	if len(dst) < totalSize {
		newDst := make([]byte, totalSize)
		copy(newDst, dst[:batchHeaderLen])
		dst = newDst
	} else {
		dst = dst[:totalSize]
	}

	// Write header.
	copy(dst[0:4], batchMagic[:])
	dst[4] = batchVersion
	dst[5] = flags
	binary.LittleEndian.PutUint16(dst[6:8], uint16(len(recs)))
	binary.LittleEndian.PutUint64(dst[8:16], firstLSN)
	binary.LittleEndian.PutUint64(dst[16:24], uint64(batchTS))
	binary.LittleEndian.PutUint32(dst[24:28], uint32(totalSize))

	// Copy compressed records into position if needed.
	if flags&flagCompressed != 0 {
		copy(dst[batchHeaderLen:], finalRecords)
	}

	// CRC covers header + records.
	crcData := dst[:batchHeaderLen+len(finalRecords)]
	checksum := crc.Checksum(crcData)
	binary.LittleEndian.PutUint32(dst[totalSize-batchTrailerLen:], checksum)

	return dst[:totalSize], totalSize, nil
}

// batchFrameInfo holds header fields extracted by scanBatchFrame.
type batchFrameInfo struct {
	firstLSN  LSN
	timestamp int64
	count     uint16
	flags     uint8
	frameSize int
	frameEnd  int // absolute offset past frame
}

// scanBatchFrame validates a batch frame header and CRC without decoding
// individual records. Used for recovery and sparse index building.
func scanBatchFrame(data []byte, off int) (batchFrameInfo, error) {
	var info batchFrameInfo
	if off+batchHeaderLen > len(data) {
		return info, ErrInvalidRecord
	}

	if data[off] != 'E' || data[off+1] != 'W' || data[off+2] != 'A' || data[off+3] != 'L' {
		return info, ErrInvalidRecord
	}

	ver := data[off+4]
	if ver == 0 || ver > batchVersion {
		return info, ErrInvalidRecord
	}

	info.flags = data[off+5]
	info.count = binary.LittleEndian.Uint16(data[off+6 : off+8])
	info.firstLSN = binary.LittleEndian.Uint64(data[off+8 : off+16])
	info.timestamp = int64(binary.LittleEndian.Uint64(data[off+16 : off+24]))
	totalSize := binary.LittleEndian.Uint32(data[off+24 : off+28])

	info.frameSize = int(totalSize)
	info.frameEnd = off + int(totalSize)

	if info.frameEnd > len(data) || totalSize < uint32(batchOverhead) {
		return info, ErrInvalidRecord
	}

	crcOff := info.frameEnd - batchTrailerLen
	storedCRC := binary.LittleEndian.Uint32(data[crcOff:info.frameEnd])
	computedCRC := crc.Checksum(data[off:crcOff])
	if storedCRC != computedCRC {
		return info, ErrCRCMismatch
	}

	return info, nil
}

// decodeBatchFrame reads one batch frame from data at off.
// Returns decoded events, offset past the frame, and any error.
// When not compressed, Event fields are zero-copy sub-slices of data.
func decodeBatchFrame(data []byte, off int, decomp Compressor) ([]Event, int, error) {
	return decodeBatchFrameInto(data, off, decomp, nil)
}

// decodeBatchFrameInto is like decodeBatchFrame but reuses buf capacity.
func decodeBatchFrameInto(data []byte, off int, decomp Compressor, buf []Event) ([]Event, int, error) {
	info, err := scanBatchFrame(data, off)
	if err != nil {
		return buf, off, err
	}

	crcOff := info.frameEnd - batchTrailerLen
	recordsData := data[off+batchHeaderLen : crcOff]

	if info.flags&flagCompressed != 0 {
		if decomp == nil {
			return buf, off, ErrCompressorRequired
		}
		decompressed, derr := decomp.Decompress(recordsData)
		if derr != nil {
			return buf, off, fmt.Errorf("%w: %w", ErrDecompress, derr)
		}
		recordsData = decompressed
	}

	perRecTS := info.flags&flagPerRecordTS != 0

	if buf == nil {
		buf = make([]Event, 0, info.count)
	}

	rOff := 0
	lsn := info.firstLSN
	fixedLen := recordFixedLen(perRecTS)

	for i := uint16(0); i < info.count; i++ {
		if rOff+fixedLen > len(recordsData) {
			return buf, off, ErrInvalidRecord
		}

		var ts int64
		if perRecTS {
			ts = int64(binary.LittleEndian.Uint64(recordsData[rOff:]))
			rOff += 8
		} else {
			ts = info.timestamp
		}

		keyLen := binary.LittleEndian.Uint16(recordsData[rOff:])
		rOff += 2
		metaLen := binary.LittleEndian.Uint16(recordsData[rOff:])
		rOff += 2
		payloadLen := binary.LittleEndian.Uint32(recordsData[rOff:])
		rOff += 4

		recEnd := rOff + int(keyLen) + int(metaLen) + int(payloadLen)
		if recEnd > len(recordsData) {
			return buf, off, ErrInvalidRecord
		}

		var key []byte
		if keyLen > 0 {
			key = recordsData[rOff : rOff+int(keyLen)]
			rOff += int(keyLen)
		}
		var meta []byte
		if metaLen > 0 {
			meta = recordsData[rOff : rOff+int(metaLen)]
			rOff += int(metaLen)
		}
		payload := recordsData[rOff : rOff+int(payloadLen)]
		rOff += int(payloadLen)

		buf = append(buf, Event{
			LSN:       lsn,
			Timestamp: ts,
			Key:       key,
			Meta:      meta,
			Payload:   payload,
		})
		lsn++
	}

	return buf, info.frameEnd, nil
}

// decodeAllBatches reads all batch frames sequentially from data.
// Stops at the first invalid frame. Returns valid events and last valid offset.
func decodeAllBatches(data []byte, decomp Compressor) ([]Event, int, error) {
	var all []Event
	off := 0
	for off < len(data) {
		events, next, err := decodeBatchFrame(data, off, decomp)
		if err != nil {
			return all, off, err
		}
		all = append(all, events...)
		off = next
	}
	return all, off, nil
}

// encoder is a reusable buffer for encoding batch frames.
// Not safe for concurrent use.
type encoder struct {
	buf []byte
}

func newEncoder(bufSize int) *encoder {
	return &encoder{buf: make([]byte, 0, bufSize)}
}

func (e *encoder) reset() {
	e.buf = e.buf[:0]
}

// encodeBatch appends a batch frame to the encoder buffer.
func (e *encoder) encodeBatch(recs []record, firstLSN LSN, comp Compressor, noCompress bool) error {
	perRecTS := !uniformTimestamp(recs)
	recRegSize := recordsRegionSize(recs, perRecTS)
	need := batchOverhead + recRegSize
	e.grow(need)
	off := len(e.buf)
	e.buf = e.buf[:off+need]

	frame, n, err := encodeBatchFrameEx(e.buf[off:off+need], recs, firstLSN, comp, noCompress, recRegSize, perRecTS)
	if err != nil {
		e.buf = e.buf[:off]
		return err
	}

	if len(frame) > 0 && (len(e.buf) < off+n || &frame[0] != &e.buf[off]) {
		// Compression caused different buffer or size change.
		e.buf = e.buf[:off]
		e.grow(n)
		e.buf = e.buf[:off+n]
		copy(e.buf[off:], frame)
	} else {
		e.buf = e.buf[:off+n]
	}
	return nil
}

func (e *encoder) grow(n int) {
	if cap(e.buf)-len(e.buf) >= n {
		return
	}
	newCap := 2*cap(e.buf) + n
	newBuf := make([]byte, len(e.buf), newCap)
	copy(newBuf, e.buf)
	e.buf = newBuf
}

func (e *encoder) bytes() []byte {
	return e.buf
}

func (e *encoder) len() int {
	return len(e.buf)
}
