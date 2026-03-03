package uewal

import "errors"

// Sentinel errors returned by WAL operations.
//
// All errors in this package are created via [errors.New] and can be
// compared directly with == or used with [errors.Is].
var (
	// ErrClosed is returned when an operation is attempted on a closed WAL
	// or when the write queue is closed during shutdown.
	ErrClosed = errors.New("uewal: WAL is closed")

	// ErrDraining is returned by [WAL.Append] when the WAL is in
	// [StateDraining] (graceful shutdown in progress).
	ErrDraining = errors.New("uewal: WAL is draining")

	// ErrNotRunning is returned when an operation requires [StateRunning]
	// but the WAL is in [StateInit] or another non-running state.
	ErrNotRunning = errors.New("uewal: WAL is not running")

	// ErrCorrupted is returned by [Iterator.Err] when a CRC mismatch
	// is detected during iteration.
	ErrCorrupted = errors.New("uewal: data corruption detected")

	// ErrQueueFull is returned by [WAL.Append] in [ErrorMode] when the
	// write queue has no available capacity.
	ErrQueueFull = errors.New("uewal: write queue is full")

	// ErrFileLocked is returned by [NewFileStorage] when the WAL file
	// is already locked by another process or WAL instance.
	ErrFileLocked = errors.New("uewal: file is locked by another instance")

	// ErrInvalidLSN is returned when an invalid LSN value is provided
	// to an operation that requires a valid sequence number.
	ErrInvalidLSN = errors.New("uewal: invalid LSN")

	// ErrShortWrite is returned by the writer when a [Storage.Write]
	// call returns n=0 without an error, which would cause an infinite
	// retry loop.
	ErrShortWrite = errors.New("uewal: short write")

	// ErrInvalidRecord is returned by decodeRecord when a record header
	// is truncated or the version byte is unsupported.
	ErrInvalidRecord = errors.New("uewal: invalid record")

	// ErrCRCMismatch is returned by decodeRecord when the stored CRC-32C
	// does not match the computed checksum over the record data.
	ErrCRCMismatch = errors.New("uewal: CRC mismatch")

	// ErrInvalidState is returned when an illegal lifecycle transition
	// is attempted (e.g., CLOSED → RUNNING).
	ErrInvalidState = errors.New("uewal: invalid state transition")

	// ErrEmptyBatch is returned by [WAL.Append] or [WAL.AppendBatch]
	// when zero events are submitted.
	ErrEmptyBatch = errors.New("uewal: empty batch")

	// ErrCompressorRequired is returned when a compressed batch frame is
	// encountered during decode but no [Compressor] was provided.
	ErrCompressorRequired = errors.New("uewal: compressor required for compressed data")
)
