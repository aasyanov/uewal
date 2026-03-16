package uewal

import (
	"path/filepath"
	"testing"
)

func TestFileStorage_WriteRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	fs, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	data := []byte("hello world")
	n, err := fs.Write(data)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(data) {
		t.Fatalf("wrote %d, want %d", n, len(data))
	}

	buf := make([]byte, len(data))
	n, err = fs.ReadAt(buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "hello world" {
		t.Fatalf("read %q", buf[:n])
	}
}

func TestFileStorage_Sync(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	fs, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	if err := fs.Sync(); err != nil {
		t.Fatal(err)
	}
}

func TestFileStorage_Size(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	fs, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	size, err := fs.Size()
	if err != nil {
		t.Fatal(err)
	}
	if size != 0 {
		t.Fatalf("empty file size=%d", size)
	}

	fs.Write([]byte("12345"))
	size, err = fs.Size()
	if err != nil {
		t.Fatal(err)
	}
	if size != 5 {
		t.Fatalf("size=%d, want 5", size)
	}
}

func TestFileStorage_Truncate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	fs, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	fs.Write([]byte("hello world"))
	if err := fs.Truncate(5); err != nil {
		t.Fatal(err)
	}

	size, _ := fs.Size()
	if size != 5 {
		t.Fatalf("after truncate size=%d, want 5", size)
	}
}

func TestFileStorage_Close(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	fs, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = fs.Write([]byte("x"))
	if err != ErrClosed {
		t.Fatalf("Write after close: %v, want ErrClosed", err)
	}
}

func TestFileStorage_DoubleClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	fs, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	fs.Close()
	if err := fs.Close(); err != nil {
		t.Fatalf("double close: %v", err)
	}
}

func TestFileStorage_Path(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	fs, err := NewFileStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	if fs.Path() != path {
		t.Fatalf("Path()=%q, want %q", fs.Path(), path)
	}
}
