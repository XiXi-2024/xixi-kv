package data

import (
	"errors"
	"fmt"
	"github.com/XiXi-2024/xixi-bitcask-kv/fio"
	"hash/crc32"
	"io"
	"path/filepath"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value, log record maybe corrupted")
)

const (
	// DataFileNameSuffix 数据文件后缀
	DataFileNameSuffix = ".data"

	// HintFileName Hint文件名称
	HintFileName = "hint-index"

	// MergeFinishedFileName merge完成标识文件名称
	MergeFinishedFileName = "merge-finished"

	// SeqNoFileName 事务序列号文件名称
	SeqNoFileName = "merge-finished"
)

// DataFile 数据文件
type DataFile struct {
	FileId    uint32        // 文件id
	WriteOff  int64         // 文件已写入偏移量, 用于活跃文件写入
	IoManager fio.IOManager // IO读写管理
}

// OpenDataFile 打开数据文件并构造 DataFile 实例
func OpenDataFile(dirPath string, fileId uint32, ioType fio.FileIOType) (*DataFile, error) {
	fileName := GetDataFileName(dirPath, fileId)
	return newDataFile(fileName, fileId, ioType)
}

// OpenHintFile 打开 Hint 索引文件并构造 DataFile 实例
func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return newDataFile(fileName, 0, fio.StandardFIO)
}

// OpenMergeFinishedFile 打开 merge 完成标识文件并构造 DataFile 实例
func OpenMergeFinishedFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFinishedFileName)
	return newDataFile(fileName, 0, fio.StandardFIO)
}

// OpenSeqNoFile 打开事务序列号文件并构造 DataFile 实例
func OpenSeqNoFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, SeqNoFileName)
	return newDataFile(fileName, 0, fio.StandardFIO)
}

// GetDataFileName 获取完整数据文件名称
func GetDataFileName(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
}

// 根据完整文件名称打开文件并构造 DataFile 实例
func newDataFile(fileName string, fileId uint32, ioType fio.FileIOType) (*DataFile, error) {
	// 获取该文件的 IO 管理实例
	ioManager, err := fio.NewIOManager(fileName, ioType)
	if err != nil {
		return nil, err
	}

	// 构造该文件的 DataFile 实例并返回
	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil
}

// ReadLogRecord 对当前数据文件从指定偏移量开始读取一条日志记录
// 返回一个 LogRecord 实例和对应字节长度
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	// 获取当前文件总长度
	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}
	// 以固定最大长度读取 Header 头部
	var headerBytes int64 = maxLogRecordHeaderSize
	// 如果固定最大长度超过文件剩余长度则读取剩余长度数据, 避免 EOF
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	// 解码
	header, headerSize := decodeLogRecordHeader(headerBuf)
	// 已读取到文件末尾, 无数据返回 EOF 错误
	// todo 是否无效判断？
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	// 获取 key 和 value 长度
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	// 计算日志记录总长度
	var recordSize = headerSize + keySize + valueSize

	// 构建 logRecord 实例
	logRecord := &LogRecord{Type: header.recordType}
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	// 校验数据完整性, 生成 CRC 值进行比较
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}

	return logRecord, recordSize, nil
}

// Write 文件写入
func (df *DataFile) Write(buf []byte) error {
	n, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	// 更新写入偏移量
	df.WriteOff += int64(n)
	return nil
}

// WriteHintRecord 写入构建索引相关信息数据
func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	// 转换为对应的 LogRecord 实例进行写入
	record := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
	}
	encRecord, _ := EncodeLogRecord(record)
	return df.Write(encRecord)
}

// Sync 文件持久化
func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

// Close 文件关闭
func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

// SetIOManager 设置数据文件的 IO 管理实现
func (df *DataFile) SetIOManager(dirPath string, ioType fio.FileIOType) error {
	// 关闭原文件
	if err := df.IoManager.Close(); err != nil {
		return err
	}
	// 重新打开文件
	ioManager, err := fio.NewIOManager(GetDataFileName(dirPath, df.FileId), ioType)
	if err != nil {
		return err
	}
	df.IoManager = ioManager
	return nil
}

// 对当前数据文件从 offset 开始读取 n 个字节
func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IoManager.Read(b, offset)
	return b, err
}
