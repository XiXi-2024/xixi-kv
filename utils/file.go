package utils

import (
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// AvailableDiskSize 获取磁盘剩余空间大小
// 当为linux/mac系统时允许 dirPath 为 ""
func AvailableDiskSize(dirPath string) (uint64, error) {
	if runtime.GOOS == "windows" {
		return availableDiskSizeWin(dirPath)
	}
	return availableDiskSizeUnix()
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
