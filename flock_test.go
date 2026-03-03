package uewal

import (
	"path/filepath"
	"testing"
)

func TestFileLockPreventsDuplicateOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lock.wal")
	s1, err := NewFileStorage(path)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}

	_, err = NewFileStorage(path)
	if err != ErrFileLocked {
		s1.Close()
		t.Fatalf("expected ErrFileLocked, got %v", err)
	}

	s1.Close()

	s2, err := NewFileStorage(path)
	if err != nil {
		t.Fatalf("open after close: %v", err)
	}
	s2.Close()
}

func TestFileLockReleaseOnClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lock_release.wal")

	for i := 0; i < 5; i++ {
		s, err := NewFileStorage(path)
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		s.Close()
	}
}
