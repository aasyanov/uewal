package uewal

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestErrors_AllDistinct(t *testing.T) {
	sentinels := []error{
		ErrClosed, ErrDraining, ErrNotRunning, ErrInvalidState,
		ErrQueueFull, ErrEmptyBatch, ErrBatchTooLarge, ErrShortWrite,
		ErrCorrupted, ErrCRCMismatch, ErrInvalidRecord, ErrInvalidLSN, ErrLSNOutOfRange,
		ErrCompressorRequired, ErrDecompress,
		ErrDirectoryLocked, ErrCreateDir, ErrLockFile,
		ErrSync, ErrMmap,
		ErrSegmentNotFound, ErrCreateSegment, ErrSealSegment, ErrScanDir,
		ErrManifestTruncated, ErrManifestVersion, ErrManifestWrite,
		ErrImportInvalid, ErrImportRead, ErrImportWrite,
	}

	for i, a := range sentinels {
		if a == nil {
			t.Fatalf("sentinel %d is nil", i)
		}
		for j := i + 1; j < len(sentinels); j++ {
			if a == sentinels[j] {
				t.Fatalf("sentinels %d and %d are identical: %v", i, j, a)
			}
		}
	}
}

func TestErrors_HavePrefix(t *testing.T) {
	sentinels := []error{
		ErrClosed, ErrDraining, ErrNotRunning, ErrInvalidState,
		ErrQueueFull, ErrEmptyBatch, ErrBatchTooLarge, ErrShortWrite,
		ErrCorrupted, ErrCRCMismatch, ErrInvalidRecord, ErrInvalidLSN, ErrLSNOutOfRange,
		ErrCompressorRequired, ErrDecompress,
		ErrDirectoryLocked, ErrCreateDir, ErrLockFile,
		ErrSync, ErrMmap,
		ErrSegmentNotFound, ErrCreateSegment, ErrSealSegment, ErrScanDir,
		ErrManifestTruncated, ErrManifestVersion, ErrManifestWrite,
		ErrImportInvalid, ErrImportRead, ErrImportWrite,
	}

	for _, e := range sentinels {
		if !strings.HasPrefix(e.Error(), "uewal: ") {
			t.Errorf("error %q missing 'uewal: ' prefix", e.Error())
		}
	}
}

func TestErrors_WrappedIsMatch(t *testing.T) {
	original := fmt.Errorf("disk full")
	wrapped := fmt.Errorf("%w: %w", ErrSync, original)

	if !errors.Is(wrapped, ErrSync) {
		t.Fatal("wrapped error should match ErrSync")
	}
	if !errors.Is(wrapped, original) {
		t.Fatal("wrapped error should match original")
	}
}

func TestErrors_DoubleWrappedIsMatch(t *testing.T) {
	ioErr := fmt.Errorf("permission denied")
	wrapped := fmt.Errorf("%w: %w", ErrCreateSegment, ioErr)
	outer := fmt.Errorf("open failed: %w", wrapped)

	if !errors.Is(outer, ErrCreateSegment) {
		t.Fatal("double-wrapped should match ErrCreateSegment")
	}
	if !errors.Is(outer, ioErr) {
		t.Fatal("double-wrapped should match original IO error")
	}
}
