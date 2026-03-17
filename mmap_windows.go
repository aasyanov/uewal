//go:build windows

package uewal

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// mmapFd maps size bytes of the file into read-only memory using
// the Windows CreateFileMapping / MapViewOfFile API.
//
// The mapping handle is closed immediately after MapViewOfFile succeeds;
// the mapping remains valid until UnmapViewOfFile is called.
func mmapFd(f *os.File, size int64) ([]byte, error) {
	handle := syscall.Handle(f.Fd())

	mapHandle, err := syscall.CreateFileMapping(handle, nil,
		syscall.PAGE_READONLY, uint32(size>>32), uint32(size), nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrMmap, err)
	}

	ptr, err := syscall.MapViewOfFile(mapHandle, syscall.FILE_MAP_READ,
		0, 0, uintptr(size))
	if err != nil {
		_ = syscall.CloseHandle(mapHandle)
		return nil, fmt.Errorf("%w: %w", ErrMmap, err)
	}

	_ = syscall.CloseHandle(mapHandle)

	// MapViewOfFile returns a uintptr that must be converted to a byte slice.
	// The uintptr→unsafe.Pointer conversion is safe here because ptr is a
	// valid address returned by the kernel. go vet flags this pattern;
	// suppress with nolint since there is no alternative for syscall results.
	return unsafe.Slice((*byte)(unsafe.Pointer(ptr)), int(size)), nil //nolint:govet // uintptr from MapViewOfFile is a valid kernel address
}

// munmapFile unmaps a previously mapped region via UnmapViewOfFile.
func munmapFile(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return syscall.UnmapViewOfFile(uintptr(unsafe.Pointer(&data[0])))
}
