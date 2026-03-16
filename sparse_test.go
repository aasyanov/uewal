package uewal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSparseIndex_AppendAndFind(t *testing.T) {
	si := &sparseIndex{}

	si.append(sparseEntry{FirstLSN: 1, Offset: 0, Timestamp: 1000})
	si.append(sparseEntry{FirstLSN: 10, Offset: 500, Timestamp: 2000})
	si.append(sparseEntry{FirstLSN: 20, Offset: 1200, Timestamp: 3000})
	si.append(sparseEntry{FirstLSN: 30, Offset: 2000, Timestamp: 4000})

	if si.len() != 4 {
		t.Fatalf("len: %d", si.len())
	}

	tests := []struct {
		lsn    LSN
		expect int64
	}{
		{0, 0},    // before first → start from first
		{1, 0},    // exact match first
		{5, 0},    // between 1 and 10 → batch at offset 0
		{10, 500}, // exact match second
		{15, 500}, // between 10 and 20 → batch at offset 500
		{25, 1200},
		{30, 2000},
		{99, 2000}, // past last → last batch
	}
	for _, tt := range tests {
		off := si.findByLSN(tt.lsn)
		if off != tt.expect {
			t.Errorf("findByLSN(%d): got %d, want %d", tt.lsn, off, tt.expect)
		}
	}
}

func TestSparseIndex_FindByTimestamp(t *testing.T) {
	si := &sparseIndex{}
	si.append(sparseEntry{FirstLSN: 1, Offset: 0, Timestamp: 1000})
	si.append(sparseEntry{FirstLSN: 10, Offset: 500, Timestamp: 2000})
	si.append(sparseEntry{FirstLSN: 20, Offset: 1200, Timestamp: 3000})

	tests := []struct {
		ts     int64
		expect int64
	}{
		{500, 0},     // before first → first batch
		{1000, 0},    // exact first
		{1500, 500},  // between first and second → second
		{2000, 500},  // exact second
		{3000, 1200}, // exact third
		{5000, -1},   // past all → -1
	}
	for _, tt := range tests {
		off := si.findByTimestamp(tt.ts)
		if off != tt.expect {
			t.Errorf("findByTimestamp(%d): got %d, want %d", tt.ts, off, tt.expect)
		}
	}
}

func TestSparseIndex_MarshalUnmarshal(t *testing.T) {
	original := &sparseIndex{}
	original.append(sparseEntry{FirstLSN: 1, Offset: 0, Timestamp: 1000})
	original.append(sparseEntry{FirstLSN: 100, Offset: 5000, Timestamp: 2000})
	original.append(sparseEntry{FirstLSN: 200, Offset: 12000, Timestamp: 3000})

	data := original.marshal()
	restored, err := unmarshalSparseIndex(data)
	if err != nil {
		t.Fatal(err)
	}

	if restored.len() != original.len() {
		t.Fatalf("len: %d, want %d", restored.len(), original.len())
	}
	for i := 0; i < original.len(); i++ {
		if restored.entries[i] != original.entries[i] {
			t.Fatalf("entry %d mismatch", i)
		}
	}
}

func TestSparseIndex_CRCMismatch(t *testing.T) {
	si := &sparseIndex{}
	si.append(sparseEntry{FirstLSN: 1, Offset: 0, Timestamp: 1000})

	data := si.marshal()
	data[0] ^= 0xFF // corrupt data

	_, err := unmarshalSparseIndex(data)
	if err != ErrCRCMismatch {
		t.Fatalf("expected ErrCRCMismatch, got %v", err)
	}
}

func TestSparseIndex_PersistToFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.idx")

	original := &sparseIndex{}
	original.append(sparseEntry{FirstLSN: 42, Offset: 100, Timestamp: 9999})

	if err := writeSparseIndex(path, original); err != nil {
		t.Fatal(err)
	}

	restored, err := readSparseIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	if restored.len() != 1 || restored.entries[0].FirstLSN != 42 {
		t.Fatalf("unexpected entries: %v", restored.entries)
	}
}

func TestSparseIndex_Empty(t *testing.T) {
	si := &sparseIndex{}
	if si.findByLSN(1) != -1 {
		t.Fatal("expected -1 for empty index")
	}
	if si.findByTimestamp(1) != -1 {
		t.Fatal("expected -1 for empty index")
	}
	if si.lastLSN() != 0 {
		t.Fatal("expected 0")
	}
	if si.lastTimestamp() != 0 {
		t.Fatal("expected 0")
	}
	if si.firstTimestamp() != 0 {
		t.Fatal("expected 0")
	}

	data := si.marshal()
	restored, err := unmarshalSparseIndex(data)
	if err != nil {
		t.Fatal(err)
	}
	if restored.len() != 0 {
		t.Fatal("expected empty")
	}
}

func TestSparseIndex_Reset(t *testing.T) {
	si := &sparseIndex{}
	si.append(sparseEntry{FirstLSN: 1, Offset: 0, Timestamp: 100})
	si.append(sparseEntry{FirstLSN: 2, Offset: 100, Timestamp: 200})
	si.reset()
	if si.len() != 0 {
		t.Fatalf("expected 0 after reset, got %d", si.len())
	}
}

func TestSparseIndex_ReadMissingFile(t *testing.T) {
	_, err := readSparseIndex(filepath.Join(t.TempDir(), "nonexistent.idx"))
	if !os.IsNotExist(err) {
		t.Fatalf("expected not-exist error, got %v", err)
	}
}
