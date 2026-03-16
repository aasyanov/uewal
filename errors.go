package uewal

import "errors"

// Sentinel errors returned by WAL operations.
// All errors are created via [errors.New] and can be compared with == or [errors.Is].
var (
	ErrClosed             = errors.New("uewal: WAL is closed")
	ErrDraining           = errors.New("uewal: WAL is draining")
	ErrNotRunning         = errors.New("uewal: WAL is not running")
	ErrCorrupted          = errors.New("uewal: data corruption detected")
	ErrQueueFull          = errors.New("uewal: write queue is full")
	ErrDirectoryLocked    = errors.New("uewal: directory is locked by another instance")
	ErrInvalidLSN         = errors.New("uewal: invalid LSN")
	ErrShortWrite         = errors.New("uewal: short write")
	ErrInvalidRecord      = errors.New("uewal: invalid record")
	ErrCRCMismatch        = errors.New("uewal: CRC mismatch")
	ErrInvalidState       = errors.New("uewal: invalid state transition")
	ErrEmptyBatch         = errors.New("uewal: empty batch")
	ErrCompressorRequired = errors.New("uewal: compressor required for compressed data")
	ErrLSNOutOfRange      = errors.New("uewal: LSN out of range")
	ErrBatchTooLarge      = errors.New("uewal: batch exceeds MaxBatchSize")
	ErrSegmentNotFound    = errors.New("uewal: segment not found")
	ErrImportInvalid      = errors.New("uewal: import data invalid (magic/CRC mismatch)")
)
