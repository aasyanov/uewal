package uewal

import "errors"

// Sentinel errors returned by WAL operations.
// All errors are created via [errors.New] and can be compared with == or [errors.Is].
//
// Wrappable sentinels: callers can test with [errors.Is]. Internal code wraps
// OS/IO errors via fmt.Errorf("%w: %w", ErrXxx, origErr) to preserve both
// the sentinel identity and the underlying cause.
var (
	// Lifecycle
	ErrClosed       = errors.New("uewal: WAL is closed")
	ErrDraining     = errors.New("uewal: WAL is draining")
	ErrNotRunning   = errors.New("uewal: WAL is not running")
	ErrInvalidState = errors.New("uewal: invalid state transition")

	// Write path
	ErrQueueFull    = errors.New("uewal: write queue is full")
	ErrEmptyBatch   = errors.New("uewal: empty batch")
	ErrBatchTooLarge = errors.New("uewal: batch exceeds MaxBatchSize")
	ErrShortWrite   = errors.New("uewal: short write")

	// Data integrity
	ErrCorrupted    = errors.New("uewal: data corruption detected")
	ErrCRCMismatch  = errors.New("uewal: CRC mismatch")
	ErrInvalidRecord = errors.New("uewal: invalid record")
	ErrInvalidLSN   = errors.New("uewal: invalid LSN")
	ErrLSNOutOfRange = errors.New("uewal: LSN out of range")

	// Compression
	ErrCompressorRequired = errors.New("uewal: compressor required for compressed data")
	ErrDecompress         = errors.New("uewal: decompression failed")

	// Directory / lock
	ErrDirectoryLocked = errors.New("uewal: directory is locked by another instance")
	ErrCreateDir       = errors.New("uewal: create directory failed")
	ErrLockFile        = errors.New("uewal: open lock file failed")

	// I/O
	ErrSync  = errors.New("uewal: sync failed")
	ErrMmap  = errors.New("uewal: mmap failed")

	// Segment
	ErrSegmentNotFound = errors.New("uewal: segment not found")
	ErrCreateSegment   = errors.New("uewal: create segment failed")
	ErrSealSegment     = errors.New("uewal: seal segment failed")
	ErrScanDir         = errors.New("uewal: scan directory failed")

	// Manifest
	ErrManifestTruncated = errors.New("uewal: manifest truncated")
	ErrManifestVersion   = errors.New("uewal: unsupported manifest version")
	ErrManifestWrite     = errors.New("uewal: manifest write failed")

	// Replication / import
	ErrImportInvalid = errors.New("uewal: import data invalid")
	ErrImportRead    = errors.New("uewal: import read failed")
	ErrImportWrite   = errors.New("uewal: import write failed")
)

// syncErr wraps a sync error without fmt.Errorf allocation.
type syncErr struct {
	cause error
}

func (e *syncErr) Error() string { return "uewal: sync: " + e.cause.Error() }
func (e *syncErr) Unwrap() error { return e.cause }
func (e *syncErr) Is(target error) bool { return target == ErrSync }
