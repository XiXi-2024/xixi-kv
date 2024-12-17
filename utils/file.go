package utils

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
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

// CopyDir 拷贝 src 目录到 dest 目录, 排除指定列表中的文件
func CopyDir(src, dest string, exclude []string) error {
	// 目标目录不存在则创建
	if _, err := os.Stat(dest); os.IsNotExist(err) {
		if err := os.MkdirAll(dest, os.ModePerm); err != nil {
			return err
		}
	}

	// 递归遍历源目录中的所有文件和子目录
	return filepath.Walk(src, func(path string, info fs.FileInfo, err error) error {
		// 从源路径中去除源目录前缀获取相对路径
		fileName := strings.Replace(path, src, "", 1)
		if fileName == "" {
			// 如果相对路径为空, 即当前路径就是源目录本身, 则跳过
			return nil
		}

		// 判断当前文件或目录是否在排除列表中
		for _, e := range exclude {
			// 模式匹配
			matched, err := filepath.Match(e, info.Name())
			if err != nil {
				return err
			}
			if matched {
				return nil
			}
		}

		if info.IsDir() {
			return os.MkdirAll(filepath.Join(dest, fileName), info.Mode())
		}

		data, err := os.ReadFile(filepath.Join(src, fileName))
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dest, fileName), data, info.Mode())
	})
}
