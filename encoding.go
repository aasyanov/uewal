package uewal

import (
	"encoding/binary"
	"io"

	"github.com/aasyanov/uewal/internal/crc"
)

// Batch frame wire format (v2):
//
//	┌──────────────────────────────────────────────────┐
//	│ Magic        4 bytes   "UWAL"                    │
//	│ Version      2 bytes   (2)                       │
//	│ Flags        2 bytes   (bit 0 = compressed)      │
//	│ RecordCount  4 bytes                             │
//	│ FirstLSN     8 bytes                             │
//	│ BatchSize    4 bytes   (total frame incl. CRC)   │
//	├──────────────────────────────────────────────────┤
//	│ Records region (possibly compressed):            │
//	│   Record 0:                                      │
//	│     PayloadLen  4 bytes                          │
//	│     MetaLen     2 bytes                          │
//	│     Meta        MetaLen bytes                    │
//	│     Payload     PayloadLen bytes                 │
//	│   Record 1: ...                                  │
//	├──────────────────────────────────────────────────┤
//	│ CRC32C       4 bytes   (covers Magic..records)   │
//	└──────────────────────────────────────────────────┘
//
// CRC-32C covers all bytes from Magic through the last record byte,
// exclusive of the CRC field itself.
// When compression is enabled (Flags bit 0 set), the records region is
// compressed. CRC covers the compressed bytes.
// All multi-byte integers are little-endian.

var batchMagic = [4]byte{'U', 'W', 'A', 'L'}

const (
	batchVersion   uint16 = 2
	batchHeaderLen        = 4 + 2 + 2 + 4 + 8 + 4 // 24 bytes
	batchTrailerLen       = 4                       // CRC32
	batchOverhead         = batchHeaderLen + batchTrailerLen

	flagCompressed uint16 = 1 << 0
)

// recordDataSize returns the encoded size of one record's data portion
// (PayloadLen + MetaLen + Meta + Payload).
func recordDataSize(ev *Event) int {
	return 4 + 2 + len(ev.Meta) + len(ev.Payload)
}

// batchFrameSize returns the total batch frame size for a set of events.
func batchFrameSize(events []Event) int {
	n := batchOverhead
	for i := range events {
		n += recordDataSize(&events[i])
	}
	return n
}

// encodeRecordsRegion writes the records portion (without header/CRC)
// into dst. Returns number of bytes written.
func encodeRecordsRegion(dst []byte, events []Event) int {
	off := 0
	for i := range events {
		binary.LittleEndian.PutUint32(dst[off:], uint32(len(events[i].Payload)))
		off += 4
		binary.LittleEndian.PutUint16(dst[off:], uint16(len(events[i].Meta)))
		off += 2
		off += copy(dst[off:], events[i].Meta)
		off += copy(dst[off:], events[i].Payload)
	}
	return off
}

// encodeBatchFrame writes a complete batch frame into dst.
// If comp is non-nil, the records region is compressed and the
// compressed flag is set. Returns the total frame size written
// and the assembled frame in the returned byte slice (which may
// differ from dst if the buffer was too small for compressed output).
func encodeBatchFrame(dst []byte, events []Event, firstLSN LSN, comp Compressor) ([]byte, int, error) {
	count := len(events)

	// Encode records region into a temp area after the header.
	recordsStart := batchHeaderLen
	recordsLen := encodeRecordsRegion(dst[recordsStart:], events)
	recordsData := dst[recordsStart : recordsStart+recordsLen]

	var flags uint16
	finalRecords := recordsData

	if comp != nil {
		compressed, err := comp.Compress(recordsData)
		if err != nil {
			return nil, 0, err
		}
		flags |= flagCompressed
		finalRecords = compressed
	}

	totalSize := batchHeaderLen + len(finalRecords) + batchTrailerLen

	// Ensure dst is large enough for the final frame.
	if len(dst) < totalSize {
		newDst := make([]byte, totalSize)
		dst = newDst
	}

	// Write header.
	copy(dst[0:4], batchMagic[:])
	binary.LittleEndian.PutUint16(dst[4:6], batchVersion)
	binary.LittleEndian.PutUint16(dst[6:8], flags)
	binary.LittleEndian.PutUint32(dst[8:12], uint32(count))
	binary.LittleEndian.PutUint64(dst[12:20], firstLSN)
	binary.LittleEndian.PutUint32(dst[20:24], uint32(totalSize))

	// Copy compressed records (always copy to ensure correct position).
	if comp != nil {
		copy(dst[batchHeaderLen:], finalRecords)
	}

	// CRC covers header + records.
	crcData := dst[:batchHeaderLen+len(finalRecords)]
	checksum := crc.Checksum(crcData)
	binary.LittleEndian.PutUint32(dst[batchHeaderLen+len(finalRecords):], checksum)

	return dst[:totalSize], totalSize, nil
}

// decodeBatchFrame reads one batch frame from data starting at off.
// Returns the decoded events, the byte offset past this frame, and any error.
// Event Payload and Meta are sub-slices of data (zero-copy) when not compressed.
// When compressed, Payload and Meta are from a heap-allocated decompressed buffer.
func decodeBatchFrame(data []byte, off int, decomp Compressor) ([]Event, int, error) {
	if off+batchHeaderLen > len(data) {
		return nil, off, ErrInvalidRecord
	}

	if data[off] != batchMagic[0] || data[off+1] != batchMagic[1] ||
		data[off+2] != batchMagic[2] || data[off+3] != batchMagic[3] {
		return nil, off, ErrInvalidRecord
	}

	ver := binary.LittleEndian.Uint16(data[off+4 : off+6])
	if ver != batchVersion {
		return nil, off, ErrInvalidRecord
	}

	flags := binary.LittleEndian.Uint16(data[off+6 : off+8])
	count := binary.LittleEndian.Uint32(data[off+8 : off+12])
	firstLSN := binary.LittleEndian.Uint64(data[off+12 : off+20])
	totalSize := binary.LittleEndian.Uint32(data[off+20 : off+24])

	frameEnd := off + int(totalSize)
	if frameEnd > len(data) || totalSize < uint32(batchOverhead) {
		return nil, off, ErrInvalidRecord
	}

	// Verify CRC.
	crcOff := frameEnd - batchTrailerLen
	storedCRC := binary.LittleEndian.Uint32(data[crcOff:frameEnd])
	computedCRC := crc.Checksum(data[off:crcOff])
	if storedCRC != computedCRC {
		return nil, off, ErrCRCMismatch
	}

	// Extract records region.
	recordsData := data[off+batchHeaderLen : crcOff]

	if flags&flagCompressed != 0 {
		if decomp == nil {
			return nil, off, ErrCompressorRequired
		}
		decompressed, err := decomp.Decompress(recordsData)
		if err != nil {
			return nil, off, ErrInvalidRecord
		}
		recordsData = decompressed
	}

	// Decode individual records from the records region.
	events := make([]Event, 0, count)
	rOff := 0
	lsn := firstLSN
	for i := uint32(0); i < count; i++ {
		if rOff+6 > len(recordsData) {
			return nil, off, ErrInvalidRecord
		}
		payloadLen := binary.LittleEndian.Uint32(recordsData[rOff : rOff+4])
		metaLen := binary.LittleEndian.Uint16(recordsData[rOff+4 : rOff+6])
		rOff += 6

		recEnd := rOff + int(metaLen) + int(payloadLen)
		if recEnd > len(recordsData) {
			return nil, off, ErrInvalidRecord
		}

		var meta []byte
		if metaLen > 0 {
			meta = recordsData[rOff : rOff+int(metaLen)]
			rOff += int(metaLen)
		}

		payload := recordsData[rOff : rOff+int(payloadLen)]
		rOff += int(payloadLen)

		events = append(events, Event{LSN: lsn, Meta: meta, Payload: payload})
		lsn++
	}

	return events, frameEnd, nil
}

// decodeAllBatches reads all batch frames sequentially from data.
// Stops at the first invalid or truncated frame. Returns all valid events
// and the byte offset of the last valid boundary.
func decodeAllBatches(data []byte, decomp Compressor) ([]Event, int, error) { //nolint:unparam // decomp is nil in current callers but part of the decode contract
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

// encoder is a reusable buffer for encoding batch frames before
// a single write to storage. Not safe for concurrent use.
type encoder struct {
	buf []byte
}

// newEncoder creates an encoder with the given initial buffer capacity.
func newEncoder(bufSize int) *encoder {
	return &encoder{buf: make([]byte, 0, bufSize)}
}

// reset discards all buffered data, retaining the underlying allocation.
func (e *encoder) reset() {
	e.buf = e.buf[:0]
}

// encodeBatch appends a complete batch frame to the encoder buffer.
func (e *encoder) encodeBatch(events []Event, firstLSN LSN, comp Compressor) error {
	need := batchFrameSize(events)
	e.grow(need)
	off := len(e.buf)
	e.buf = e.buf[:off+need]

	frame, n, err := encodeBatchFrame(e.buf[off:], events, firstLSN, comp)
	if err != nil {
		e.buf = e.buf[:off]
		return err
	}

	// If compression caused a reallocation, copy the frame into our buffer.
	if &frame[0] != &e.buf[off] {
		e.grow(n - need)
		e.buf = e.buf[:off+n]
		copy(e.buf[off:], frame)
	} else {
		e.buf = e.buf[:off+n]
	}
	return nil
}

// grow ensures the buffer has at least n bytes of remaining capacity.
func (e *encoder) grow(n int) {
	if cap(e.buf)-len(e.buf) >= n {
		return
	}
	newCap := 2*cap(e.buf) + n
	newBuf := make([]byte, len(e.buf), newCap)
	copy(newBuf, e.buf)
	e.buf = newBuf
}

// bytes returns the buffered data as a byte slice.
func (e *encoder) bytes() []byte {
	return e.buf
}

// writeTo writes the buffered data to w.
func (e *encoder) writeTo(w io.Writer) (int, error) {
	return w.Write(e.buf)
}
