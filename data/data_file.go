package data

import "github.com/XiXi-2024/xixi-bitcask-kv/fio"

const DataFileNameSuffix = ".data"

// DataFile 数据文件
type DataFile struct {
	FileId    uint32        // 文件id
	WriteOff  int64         // 文件已写入偏移 作为活跃文件时使用
	IoManager fio.IOManager // io 读写管理
}

// OpenDataFile 获取数据文件
func OpenDataFile(path string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

// ReadLogRecord 读取当前文件的日志记录
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	return nil, 0, nil
}

// Write 向当前文件写入编码后的数据
func (df *DataFile) Write(buf []byte) error {
	return nil
}

// Sync 当前文件持久化
func (df *DataFile) Sync() error {
	return nil
}
