//go:build windows

package utils

import (
	"syscall"
	"unsafe"
)

// Win系统下获取指定目录所在磁盘的剩余空间大小
func availableDiskSizeWin(dirPath string) (uint64, error) {
	// 加载 kernel32.dll
	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	// 获取 GetDiskFreeSpaceExW 函数
	procGetDiskFreeSpaceExW := kernel32.NewProc("GetDiskFreeSpaceExW")

	// 将路径转换为 UTF-16
	pathPtr, err := syscall.UTF16PtrFromString(dirPath)
	if err != nil {
		return 0, err
	}

	var freeBytesAvailable, _, _ int64

	// 调用 GetDiskFreeSpaceExW
	ret, _, err := procGetDiskFreeSpaceExW.Call(
		uintptr(unsafe.Pointer(pathPtr)),
		uintptr(unsafe.Pointer(&freeBytesAvailable)),
		0,
		0,
	)

	if ret == 0 {
		return 0, err
	}

	return uint64(freeBytesAvailable), nil
}
