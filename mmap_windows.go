//go:build windows

package uewal

import (
	"fmt"
	"os"
	"reflect"
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

	// Build []byte from the mapped address. We use reflect.SliceHeader
	// because go vet prohibits direct uintptr→unsafe.Pointer conversion
	// outside syscall call expressions (unsafeptr check). This is the
	// same approach used in Go's own x/sys/windows mmap implementation.
	// reflect.SliceHeader is deprecated (SA1019) but remains the only
	// way to satisfy go vet on Windows; suppress staticcheck/govet.
	var data []byte
	//nolint:staticcheck // SA1019: reflect.SliceHeader required because go vet forbids uintptr→unsafe.Pointer
	//lint:ignore SA1019 go vet forbids unsafe.Pointer(uintptr)
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	sh.Data = ptr
	sh.Len = int(size)
	sh.Cap = int(size)
	return data, nil
}

// munmapFile unmaps a previously mapped region via UnmapViewOfFile.
func munmapFile(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	//nolint:staticcheck // SA1019: mirrors mmapFd
	//lint:ignore SA1019 mirrors mmapFd
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	return syscall.UnmapViewOfFile(sh.Data)
}
