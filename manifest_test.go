package uewal

import (
	"path/filepath"
	"testing"
)

func TestManifest_MarshalUnmarshal(t *testing.T) {
	m := &manifest{
		lastLSN: 500,
		entries: []manifestEntry{
			{firstLSN: 1, lastLSN: 100, size: 4096, createdAt: 1000, firstTS: 10, lastTS: 20, sealed: true},
			{firstLSN: 101, lastLSN: 200, size: 8192, createdAt: 2000, firstTS: 30, lastTS: 40, sealed: true},
			{firstLSN: 201, lastLSN: 500, size: 2048, createdAt: 3000, firstTS: 50, lastTS: 60, sealed: false},
		},
	}

	data := m.marshal()
	restored, err := unmarshalManifest(data)
	if err != nil {
		t.Fatal(err)
	}

	if restored.lastLSN != m.lastLSN {
		t.Fatalf("lastLSN: %d, want %d", restored.lastLSN, m.lastLSN)
	}
	if len(restored.entries) != len(m.entries) {
		t.Fatalf("entries: %d, want %d", len(restored.entries), len(m.entries))
	}
	for i, e := range restored.entries {
		want := m.entries[i]
		if e.firstLSN != want.firstLSN || e.lastLSN != want.lastLSN ||
			e.size != want.size || e.sealed != want.sealed ||
			e.createdAt != want.createdAt || e.firstTS != want.firstTS || e.lastTS != want.lastTS {
			t.Fatalf("entry %d mismatch: got %+v, want %+v", i, e, want)
		}
	}
}

func TestManifest_CRCMismatch(t *testing.T) {
	m := &manifest{lastLSN: 10, entries: []manifestEntry{
		{firstLSN: 1, lastLSN: 10, size: 100, sealed: true},
	}}

	data := m.marshal()
	data[5] ^= 0xFF

	_, err := unmarshalManifest(data)
	if err == nil {
		t.Fatal("expected CRC error")
	}
}

func TestManifest_AtomicWriteRead(t *testing.T) {
	dir := t.TempDir()

	m := &manifest{
		lastLSN: 42,
		entries: []manifestEntry{
			{firstLSN: 1, lastLSN: 42, size: 1024, sealed: false, createdAt: 9999},
		},
	}

	if err := writeManifest(dir, m); err != nil {
		t.Fatal(err)
	}

	restored, err := readManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if restored.lastLSN != 42 {
		t.Fatalf("lastLSN: %d", restored.lastLSN)
	}
	if len(restored.entries) != 1 || restored.entries[0].firstLSN != 1 {
		t.Fatalf("unexpected entries: %+v", restored.entries)
	}
}

func TestManifest_Empty(t *testing.T) {
	m := &manifest{lastLSN: 0, entries: nil}

	data := m.marshal()
	restored, err := unmarshalManifest(data)
	if err != nil {
		t.Fatal(err)
	}
	if restored.lastLSN != 0 || len(restored.entries) != 0 {
		t.Fatal("unexpected non-empty manifest")
	}
}

func TestManifest_ReadMissing(t *testing.T) {
	_, err := readManifest(filepath.Join(t.TempDir(), "nodir"))
	if err == nil {
		t.Fatal("expected error for missing manifest")
	}
}

func TestManifest_Truncated(t *testing.T) {
	m := &manifest{lastLSN: 10, entries: []manifestEntry{
		{firstLSN: 1, lastLSN: 10, size: 100, sealed: true},
	}}
	data := m.marshal()

	_, err := unmarshalManifest(data[:len(data)-10])
	if err == nil {
		t.Fatal("expected error for truncated manifest")
	}
}
