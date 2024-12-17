package utils

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDirSize(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask-go-dirSize")
	//dir := "C:\\Users\\acer\\AppData\\Local\\Temp\\bitcask-go-stat2414673852"
	dirSize, err := DirSize(dir)
	//t.Log(dir)
	//t.Log(dirSize)
	assert.Nil(t, err)
	assert.True(t, dirSize > 0)
}

func TestAvailableDiskSizeWin(t *testing.T) {
	dir, _ := os.MkdirTemp("", "data")
	size, err := AvailableDiskSizeWin(dir)
	t.Log(size / 1024 / 1024 / 1024)
	assert.Nil(t, err)
	assert.True(t, size > 0)
}
