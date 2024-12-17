package utils

import (
	"os"
	"path/filepath"
	"syscall"
	"unsafe"
)

// DirSize 获取指定目录的空间大小
func DirSize(dirPath string) (int64, error) {
	var size int64
	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

// AvailableDiskSizeUnix AvailableDiskSize 获取磁盘剩余空间大小 Linux/Mac系统
func AvailableDiskSizeUnix() (uint64, error) {
	//wd, err := syscall.Getwd()
	//if err != nil {
	//	return 0, err
	//}
	//var stat syscall.Statfs_t
	//if err = syscall.Statfs(wd, &stat); err != nil {
	//	return 0, err
	//}
	//return stat.Bavail * uint64(stat.Bsize), nil
	return 0, nil
}

// AvailableDiskSizeWin AvailableDiskSize 获取指定目录所在磁盘的剩余空间大小 Win系统
func AvailableDiskSizeWin(dirPath string) (uint64, error) {
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
