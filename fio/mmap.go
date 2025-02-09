package fio

import (
	"github.com/edsrzf/mmap-go"
	"os"
)

// todo bug：应与用户配置的最大容量相等
const dataFileSize = 256 * 1024

// MMap 内存文件映射 IO 实现
type MMap struct {
	file   *os.File
	data   mmap.MMap
	offset int64
}

func NewMMap(fileName string) (*MMap, error) {
	fd, err := os.OpenFile(
		fileName,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		DataFilePerm,
	)
	if err != nil {
		return nil, err
	}
	info, err := fd.Stat()
	if err != nil {
		return nil, err
	}
	offset := info.Size()
	err = fd.Truncate(dataFileSize)
	if err != nil {
		return nil, err
	}
	data, err := mmap.Map(fd, mmap.RDWR, 0)
	m := &MMap{
		file:   fd,
		data:   data,
		offset: offset,
	}
	return m, err
}

func (mmap *MMap) Read(b []byte, offset int64) (int, error) {
	copy(b, mmap.data[offset:])
	return len(b), nil
}

func (mmap *MMap) Write(b []byte) (int, error) {
	copy(mmap.data[mmap.offset:], b)
	mmap.offset += int64(len(b))
	return len(b), nil
}

func (mmap *MMap) Sync() error {
	err := mmap.data.Flush()
	return err
}

func (mmap *MMap) Close() error {
	err := mmap.data.Flush()
	if err != nil {
		return err
	}
	err = mmap.data.Unmap()
	if err != nil {
		return err
	}
	// 关闭文件前将其大小修改为真实大小
	err = mmap.file.Truncate(mmap.offset)
	if err != nil {
		return err
	}
	err = mmap.file.Close()
	return err
}

func (mmap *MMap) Size() (int64, error) {
	return mmap.offset, nil
}
