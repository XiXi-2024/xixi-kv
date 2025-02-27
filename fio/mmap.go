package fio

import (
	"errors"
	"github.com/edsrzf/mmap-go"
	"io"
	"os"
)

var ErrFileHasBeenClosed = errors.New("file has been closed")

// 选择 mmap 类型 IO 实现时处理数据量不应过大, 且数据文件容量配置参数难以传递
// 故设置映射空间为 512MB, 由上层保证该 IO 实现下的数据文件容量小于等于512MB
// 批处理不适用mmap实现, 大规模数据写入会导致 mmap 变慢
const dataFileSize = 512 * 1024 * 1024

// MMap 内存文件映射 IO 实现
// todo 扩展点：预读机制
// todo 扩展点：批量读写机制
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
	if mmap.file == nil {
		return 0, ErrFileHasBeenClosed
	}
	// 计算实际可读取的字节数
	bytes := min(len(b), int(mmap.offset-offset))
	if bytes == 0 {
		return 0, io.EOF
	}
	copy(b[:bytes], mmap.data[offset:])
	return bytes, nil
}

func (mmap *MMap) Write(b []byte) (int, error) {
	if mmap.file == nil {
		return 0, ErrFileHasBeenClosed
	}
	copy(mmap.data[mmap.offset:], b)
	mmap.offset += int64(len(b))
	return len(b), nil
}

func (mmap *MMap) Sync() error {
	if mmap.file == nil {
		return ErrFileHasBeenClosed
	}
	err := mmap.data.Flush()
	return err
}

func (mmap *MMap) Close() error {
	if mmap.file == nil {
		return ErrFileHasBeenClosed
	}
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
	if mmap.file == nil {
		return 0, ErrFileHasBeenClosed
	}
	// 通过维护 offset 实现, 故不允许运行中更换 IO 实现
	return mmap.offset, nil
}
