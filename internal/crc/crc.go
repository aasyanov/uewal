// Package crc provides CRC-32C (Castagnoli) checksum computation.
//
// CRC-32C uses the Castagnoli polynomial, which has hardware acceleration
// on modern CPUs via SSE4.2 (x86) and CRC instructions (ARM). This makes
// it significantly faster than the standard CRC-32 (IEEE) polynomial for
// data integrity checks.
//
// This package is internal to uewal and not intended for external use.
package crc

import "hash/crc32"

// table is the pre-computed CRC-32C lookup table using the Castagnoli polynomial.
var table = crc32.MakeTable(crc32.Castagnoli)

// Checksum returns the CRC-32C checksum of data.
func Checksum(data []byte) uint32 {
	return crc32.Checksum(data, table)
}

// Update returns the result of adding the bytes in data to the running crc.
// This allows incremental checksum computation over multiple data segments.
func Update(crc uint32, data []byte) uint32 {
	return crc32.Update(crc, table, data)
}
