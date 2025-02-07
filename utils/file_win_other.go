//go:build !windows

package utils

// linux/mac系统下的空实现
func availableDiskSizeWin(dirPath string) (uint64, error) {
	return 0, nil
}
