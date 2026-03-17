package uewal

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/aasyanov/uewal/internal/crc"
)

// Manifest binary format:
//
//	[Version 1B][SegCount 4B][LastLSN 8B]
//	[SegEntry × SegCount]
//	[CRC32C 4B]
//
// SegEntry (57 bytes):
//
//	FirstLSN 8B, LastLSN 8B, Size 8B, CreatedAt 8B,
//	FirstTimestamp 8B, LastTimestamp 8B, Sealed 1B
const (
	manifestVersion    = 1
	manifestHeaderSize = 1 + 4 + 8 // version + segCount + lastLSN
	manifestEntrySize  = 8 + 8 + 8 + 8 + 8 + 8 + 1
	manifestCRCSize    = 4
	manifestFile       = "manifest.bin"
)

type manifestEntry struct {
	firstLSN  LSN
	lastLSN   LSN
	size      int64
	createdAt int64
	firstTS   int64
	lastTS    int64
	sealed    bool
}

type manifest struct {
	entries []manifestEntry
	lastLSN LSN
}

func (m *manifest) marshal() []byte {
	return m.marshalInto(nil)
}

// marshalInto serializes the manifest, reusing buf if large enough.
func (m *manifest) marshalInto(buf []byte) []byte {
	dataLen := manifestHeaderSize + len(m.entries)*manifestEntrySize
	need := dataLen + manifestCRCSize
	if cap(buf) < need {
		buf = make([]byte, need)
	} else {
		buf = buf[:need]
	}

	buf[0] = manifestVersion
	binary.LittleEndian.PutUint32(buf[1:], uint32(len(m.entries)))
	binary.LittleEndian.PutUint64(buf[5:], m.lastLSN)

	off := manifestHeaderSize
	for _, e := range m.entries {
		binary.LittleEndian.PutUint64(buf[off:], e.firstLSN)
		binary.LittleEndian.PutUint64(buf[off+8:], e.lastLSN)
		binary.LittleEndian.PutUint64(buf[off+16:], uint64(e.size))
		binary.LittleEndian.PutUint64(buf[off+24:], uint64(e.createdAt))
		binary.LittleEndian.PutUint64(buf[off+32:], uint64(e.firstTS))
		binary.LittleEndian.PutUint64(buf[off+40:], uint64(e.lastTS))
		if e.sealed {
			buf[off+48] = 1
		} else {
			buf[off+48] = 0
		}
		off += manifestEntrySize
	}

	checksum := crc.Checksum(buf[:dataLen])
	binary.LittleEndian.PutUint32(buf[dataLen:], checksum)
	return buf
}

func unmarshalManifest(data []byte) (*manifest, error) {
	if len(data) < manifestHeaderSize+manifestCRCSize {
		return nil, ErrManifestTruncated
	}

	version := data[0]
	if version != manifestVersion {
		return nil, fmt.Errorf("%w: %d", ErrManifestVersion, version)
	}

	segCount := binary.LittleEndian.Uint32(data[1:])
	lastLSN := binary.LittleEndian.Uint64(data[5:])

	expectedLen := manifestHeaderSize + int(segCount)*manifestEntrySize + manifestCRCSize
	if len(data) < expectedLen {
		return nil, ErrManifestTruncated
	}

	dataLen := expectedLen - manifestCRCSize
	storedCRC := binary.LittleEndian.Uint32(data[dataLen:])
	computedCRC := crc.Checksum(data[:dataLen])
	if storedCRC != computedCRC {
		return nil, ErrCRCMismatch
	}

	entries := make([]manifestEntry, segCount)
	off := manifestHeaderSize
	for i := range entries {
		entries[i] = manifestEntry{
			firstLSN:  binary.LittleEndian.Uint64(data[off:]),
			lastLSN:   binary.LittleEndian.Uint64(data[off+8:]),
			size:      int64(binary.LittleEndian.Uint64(data[off+16:])),
			createdAt: int64(binary.LittleEndian.Uint64(data[off+24:])),
			firstTS:   int64(binary.LittleEndian.Uint64(data[off+32:])),
			lastTS:    int64(binary.LittleEndian.Uint64(data[off+40:])),
			sealed:    data[off+48] == 1,
		}
		off += manifestEntrySize
	}

	return &manifest{entries: entries, lastLSN: lastLSN}, nil
}

// writeManifest writes the manifest atomically using write-to-temp + rename.
func writeManifest(dir string, m *manifest) error {
	return writeManifestBytes(dir, m.marshal())
}

// writeManifestBytes writes pre-serialized manifest bytes atomically.
// The data is written to a temp file, fsynced, and then renamed over
// the target to ensure crash-safe persistence on all platforms.
func writeManifestBytes(dir string, data []byte) error {
	target := filepath.Join(dir, manifestFile)
	tmp := target + manifestTmpExt

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, defaultFileMode)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrManifestWrite, err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("%w: %w", ErrManifestWrite, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("%w: %w", ErrManifestWrite, err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("%w: %w", ErrManifestWrite, err)
	}

	if err := os.Rename(tmp, target); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("%w: %w", ErrManifestWrite, err)
	}
	return nil
}

// readManifest reads and validates the manifest from disk.
func readManifest(dir string) (*manifest, error) {
	data, err := os.ReadFile(filepath.Join(dir, manifestFile))
	if err != nil {
		return nil, err
	}
	return unmarshalManifest(data)
}

// buildManifest creates a manifest from a sorted slice of segments.
func buildManifest(segments []*segment, lastLSN LSN) *manifest {
	m := &manifest{
		entries: make([]manifestEntry, len(segments)),
		lastLSN: lastLSN,
	}
	for i, s := range segments {
		m.entries[i] = manifestEntry{
			firstLSN:  s.firstLSN,
			lastLSN:   s.loadLastLSN(),
			size:      s.sizeAt.Load(),
			createdAt: s.createdAt,
			firstTS:   s.firstTSv.Load(),
			lastTS:    s.lastTSv.Load(),
			sealed:    s.isSealed(),
		}
	}
	return m
}
