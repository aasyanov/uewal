package crc

import (
	"hash/crc32"
	"testing"
)

func TestChecksum_Empty(t *testing.T) {
	got := Checksum(nil)
	if got != 0 {
		t.Fatalf("Checksum(nil) = %d, want 0", got)
	}
	got = Checksum([]byte{})
	if got != 0 {
		t.Fatalf("Checksum([]) = %d, want 0", got)
	}
}

func TestChecksum_KnownValue(t *testing.T) {
	data := []byte("hello world")
	got := Checksum(data)
	want := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
	if got != want {
		t.Fatalf("Checksum(%q) = %#x, want %#x", data, got, want)
	}
}

func TestChecksum_SingleByte(t *testing.T) {
	data := []byte{0x42}
	got := Checksum(data)
	want := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
	if got != want {
		t.Fatalf("Checksum([0x42]) = %#x, want %#x", got, want)
	}
}

func TestChecksum_DifferentData(t *testing.T) {
	a := Checksum([]byte("aaa"))
	b := Checksum([]byte("bbb"))
	if a == b {
		t.Fatal("different data should produce different checksums")
	}
}

func TestChecksum_Deterministic(t *testing.T) {
	data := []byte("deterministic check")
	c1 := Checksum(data)
	c2 := Checksum(data)
	if c1 != c2 {
		t.Fatal("same data should produce same checksum")
	}
}

func TestChecksum_UsesCastagnoli(t *testing.T) {
	data := []byte("polynomial check")
	got := Checksum(data)
	ieee := crc32.ChecksumIEEE(data)
	if got == ieee {
		t.Fatal("CRC-32C should differ from CRC-32 IEEE for typical data")
	}
}

func TestChecksum_LargeData(t *testing.T) {
	data := make([]byte, 1<<20) // 1 MB
	for i := range data {
		data[i] = byte(i)
	}
	got := Checksum(data)
	want := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
	if got != want {
		t.Fatalf("large data: got %#x, want %#x", got, want)
	}
}

// --- Benchmarks ---

func BenchmarkChecksum_64B(b *testing.B) {
	data := make([]byte, 64)
	b.SetBytes(64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Checksum(data)
	}
}

func BenchmarkChecksum_256B(b *testing.B) {
	data := make([]byte, 256)
	b.SetBytes(256)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Checksum(data)
	}
}

func BenchmarkChecksum_1KB(b *testing.B) {
	data := make([]byte, 1<<10)
	b.SetBytes(1 << 10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Checksum(data)
	}
}

func BenchmarkChecksum_4KB(b *testing.B) {
	data := make([]byte, 4<<10)
	b.SetBytes(4 << 10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Checksum(data)
	}
}

func BenchmarkChecksum_64KB(b *testing.B) {
	data := make([]byte, 64<<10)
	b.SetBytes(64 << 10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Checksum(data)
	}
}

func BenchmarkChecksum_1MB(b *testing.B) {
	data := make([]byte, 1<<20)
	b.SetBytes(1 << 20)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Checksum(data)
	}
}

func BenchmarkChecksum_BatchHeader(b *testing.B) {
	data := make([]byte, 28)
	b.SetBytes(28)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Checksum(data)
	}
}

func BenchmarkChecksum_TypicalBatch(b *testing.B) {
	data := make([]byte, 8192)
	b.SetBytes(8192)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Checksum(data)
	}
}
