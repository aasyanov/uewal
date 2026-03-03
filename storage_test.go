package uewal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// RunStorageContractTests runs the standard contract test suite against any
// Storage implementation. External implementors can use this to verify
// their custom backends.
func RunStorageContractTests(t *testing.T, factory func(t *testing.T) Storage) {
	t.Helper()

	t.Run("WriteAndReadAt", func(t *testing.T) {
		s := factory(t)
		defer s.Close()

		data := []byte("hello world")
		n, err := s.Write(data)
		if err != nil {
			t.Fatalf("Write error: %v", err)
		}
		if n != len(data) {
			t.Fatalf("Write n=%d, want %d", n, len(data))
		}

		buf := make([]byte, len(data))
		_, err = s.ReadAt(buf, 0)
		if err != nil {
			t.Fatalf("ReadAt error: %v", err)
		}
		if !bytes.Equal(buf, data) {
			t.Fatalf("ReadAt=%q, want %q", buf, data)
		}
	})

	t.Run("Size", func(t *testing.T) {
		s := factory(t)
		defer s.Close()

		size, err := s.Size()
		if err != nil {
			t.Fatalf("Size error: %v", err)
		}
		if size != 0 {
			t.Fatalf("initial Size=%d, want 0", size)
		}

		s.Write([]byte("12345"))
		size, err = s.Size()
		if err != nil {
			t.Fatalf("Size error: %v", err)
		}
		if size != 5 {
			t.Fatalf("Size=%d, want 5", size)
		}
	})

	t.Run("Truncate", func(t *testing.T) {
		s := factory(t)
		defer s.Close()

		s.Write([]byte("hello world"))
		if err := s.Truncate(5); err != nil {
			t.Fatalf("Truncate error: %v", err)
		}
		size, _ := s.Size()
		if size != 5 {
			t.Fatalf("Size after truncate=%d, want 5", size)
		}

		buf := make([]byte, 5)
		s.ReadAt(buf, 0)
		if string(buf) != "hello" {
			t.Fatalf("data after truncate=%q, want %q", buf, "hello")
		}
	})

	t.Run("Sync", func(t *testing.T) {
		s := factory(t)
		defer s.Close()

		s.Write([]byte("data"))
		if err := s.Sync(); err != nil {
			t.Fatalf("Sync error: %v", err)
		}
	})

	t.Run("MultipleWrites", func(t *testing.T) {
		s := factory(t)
		defer s.Close()

		s.Write([]byte("aaa"))
		s.Write([]byte("bbb"))
		s.Write([]byte("ccc"))

		size, _ := s.Size()
		if size != 9 {
			t.Fatalf("Size=%d, want 9", size)
		}

		buf := make([]byte, 9)
		s.ReadAt(buf, 0)
		if string(buf) != "aaabbbccc" {
			t.Fatalf("data=%q, want %q", buf, "aaabbbccc")
		}
	})

	t.Run("ReadAtOffset", func(t *testing.T) {
		s := factory(t)
		defer s.Close()

		s.Write([]byte("0123456789"))

		buf := make([]byte, 3)
		n, err := s.ReadAt(buf, 5)
		if err != nil {
			t.Fatalf("ReadAt error: %v", err)
		}
		if n != 3 {
			t.Fatalf("ReadAt n=%d, want 3", n)
		}
		if string(buf) != "567" {
			t.Fatalf("ReadAt=%q, want %q", buf, "567")
		}
	})

	t.Run("TruncateAndAppend", func(t *testing.T) {
		s := factory(t)
		defer s.Close()

		s.Write([]byte("aaabbbccc"))
		s.Truncate(3)
		s.Write([]byte("ddd"))

		size, _ := s.Size()
		if size != 6 {
			t.Fatalf("Size=%d, want 6", size)
		}
		buf := make([]byte, 6)
		s.ReadAt(buf, 0)
		if string(buf) != "aaaddd" {
			t.Fatalf("data=%q, want %q", buf, "aaaddd")
		}
	})
}

func TestFileStorageContract(t *testing.T) {
	RunStorageContractTests(t, func(t *testing.T) Storage {
		t.Helper()
		path := filepath.Join(t.TempDir(), "test.wal")
		s, err := NewFileStorage(path)
		if err != nil {
			t.Fatalf("NewFileStorage: %v", err)
		}
		return s
	})
}

func TestFileStoragePath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	s, err := NewFileStorage(path)
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	defer s.Close()

	if s.Path() != path {
		t.Fatalf("Path()=%q, want %q", s.Path(), path)
	}
}

func TestFileStorageCloseIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	s, err := NewFileStorage(path)
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestFileStorageFileLocking(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	s1, err := NewFileStorage(path)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	defer s1.Close()

	_, err = NewFileStorage(path)
	if err != ErrFileLocked {
		t.Fatalf("second open: got %v, want ErrFileLocked", err)
	}
}

func TestFileStorageCreatesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "new.wal")
	s, err := NewFileStorage(path)
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	s.Close()

	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatal("file should exist after NewFileStorage")
	}
}

func TestFileStoragePersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "persist.wal")

	s, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	s.Write([]byte("persistent data"))
	s.Sync()
	s.Close()

	s2, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	buf := make([]byte, 15)
	s2.ReadAt(buf, 0)
	if string(buf) != "persistent data" {
		t.Fatalf("data=%q, want %q", buf, "persistent data")
	}
}

func TestFileStorageOperationsAfterClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "after_close.wal")
	s, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	s.Write([]byte("data"))
	s.Close()

	if _, err := s.Write([]byte("more")); err != ErrClosed {
		t.Errorf("Write after close: got %v, want ErrClosed", err)
	}
	if err := s.Sync(); err != ErrClosed {
		t.Errorf("Sync after close: got %v, want ErrClosed", err)
	}
	if _, err := s.Size(); err != ErrClosed {
		t.Errorf("Size after close: got %v, want ErrClosed", err)
	}
	if err := s.Truncate(0); err != ErrClosed {
		t.Errorf("Truncate after close: got %v, want ErrClosed", err)
	}
	buf := make([]byte, 4)
	if _, err := s.ReadAt(buf, 0); err != ErrClosed {
		t.Errorf("ReadAt after close: got %v, want ErrClosed", err)
	}
}

func TestFileStorageTruncateThenAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trunc_append.wal")
	s, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	s.Write([]byte("hello world"))
	if err := s.Truncate(5); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	// After truncate, new writes must append at the truncated position.
	s.Write([]byte(" there"))
	size, _ := s.Size()
	if size != 11 {
		t.Fatalf("size=%d, want 11", size)
	}

	buf := make([]byte, 11)
	s.ReadAt(buf, 0)
	if string(buf) != "hello there" {
		t.Fatalf("data=%q, want %q", buf, "hello there")
	}
}

func TestFileStorageTruncateToZeroAndWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trunc_zero.wal")
	s, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	s.Write([]byte("garbage"))
	if err := s.Truncate(0); err != nil {
		t.Fatalf("Truncate(0): %v", err)
	}

	size, _ := s.Size()
	if size != 0 {
		t.Fatalf("size after truncate(0)=%d, want 0", size)
	}

	s.Write([]byte("fresh"))
	size, _ = s.Size()
	if size != 5 {
		t.Fatalf("size after fresh write=%d, want 5", size)
	}

	buf := make([]byte, 5)
	s.ReadAt(buf, 0)
	if string(buf) != "fresh" {
		t.Fatalf("data=%q, want %q", buf, "fresh")
	}
}
