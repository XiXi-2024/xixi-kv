package fio

import "os"

// FileIO 标准系统文件 IO 实现
type FileIO struct {
	fd *os.File // 系统文件描述符
}

// NewFileIOManager 创建 FileIO 实例
func NewFileIOManager(fileName string) (*FileIO, error) {
	// 打开文件, 不存在则创建
	fd, err := os.OpenFile(
		fileName,
		// 不存在则创建, 读写模式打开文件, 追加形式写入
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		// 文件权限
		DataFilePerm,
	)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: fd}, nil
}

func (fio *FileIO) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

func (fio *FileIO) Write(b []byte) (int, error) {
	return fio.fd.Write(b)
}

func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

func (fio *FileIO) Close() error {
	return fio.fd.Close()
}

func (fio *FileIO) Size() (int64, error) {
	stat, err := fio.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
