//go:build windows

package utils

// win系统下的空实现
func availableDiskSizeUnix() (uint64, error) {
	return 0, nil
}
