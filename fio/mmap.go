package fio

import (
	"golang.org/x/exp/mmap"
	"os"
)

// MMap 内存文件映射 IO 实现
type MMap struct {
	readerAt *mmap.ReaderAt
}

func NewMMapIOManager(filename string) (*MMap, error) {
	// 文件不存在则创建
	_, err := os.OpenFile(filename, os.O_CREATE, DataFilePerm)
	if err != nil {
		return nil, err
	}
	readerAt, err := mmap.Open(filename)
	if err != nil {
		return nil, err
	}
	return &MMap{readerAt: readerAt}, nil
}

func (mmap *MMap) Read(b []byte, offset int64) (int, error) {
	return mmap.readerAt.ReadAt(b, offset)
}

// todo 扩展点：补充其他实现
func (mmap *MMap) Write([]byte) (int, error) {
	// 无需实现
	panic("not implemented")
}

func (mmap *MMap) Sync() error {
	// 无需实现
	panic("not implemented")
}

func (mmap *MMap) Close() error {
	return mmap.readerAt.Close()
}

func (mmap *MMap) Size() (int64, error) {
	return int64(mmap.readerAt.Len()), nil
}
