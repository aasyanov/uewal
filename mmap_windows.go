//go:build windows

package uewal

import (
	"fmt"
	"os"
	"reflect"
	"syscall"
	"unsafe"
)

// mmapFile maps the storage into memory for zero-copy reads.
// If s is not a *FileStorage, falls back to readAllFallback.
func mmapFile(s Storage, size int64) ([]byte, error) {
	fs, ok := s.(*FileStorage)
	if !ok {
		return readAllFallback(s, size)
	}
	if fs.f == nil {
		return nil, fmt.Errorf("uewal: file is closed")
	}
	return mmapFd(fs.f, size)
}

// mmapFd maps size bytes of the file into read-only memory using
// the Windows CreateFileMapping / MapViewOfFile API.
//
// The mapping handle is closed immediately after MapViewOfFile succeeds;
// the mapping remains valid until UnmapViewOfFile is called.
//
// reflect.SliceHeader is used because MapViewOfFile returns a uintptr, and
// go vet prohibits direct uintptr→unsafe.Pointer conversion outside syscall
// expressions. This is the same pattern used by Go's own syscall.Mmap on Windows.
func mmapFd(f *os.File, size int64) ([]byte, error) {
	handle := syscall.Handle(f.Fd())

	mapHandle, err := syscall.CreateFileMapping(handle, nil,
		syscall.PAGE_READONLY, uint32(size>>32), uint32(size), nil)
	if err != nil {
		return nil, fmt.Errorf("uewal: CreateFileMapping: %w", err)
	}

	ptr, err := syscall.MapViewOfFile(mapHandle, syscall.FILE_MAP_READ,
		0, 0, uintptr(size))
	if err != nil {
		_ = syscall.CloseHandle(mapHandle)
		return nil, fmt.Errorf("uewal: MapViewOfFile: %w", err)
	}

	_ = syscall.CloseHandle(mapHandle)

	var data []byte
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&data)) //nolint:staticcheck // reflect.SliceHeader is required here; go vet forbids direct uintptr→unsafe.Pointer from MapViewOfFile
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
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&data)) //nolint:staticcheck // mirrors mmapFd; required for safe uintptr recovery
	return syscall.UnmapViewOfFile(sh.Data)
}
