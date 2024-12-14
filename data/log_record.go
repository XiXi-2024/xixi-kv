package data

import (
	"encoding/binary"
	"hash/crc32"
)

// LogRecordType 日志记录状态枚举
type LogRecordType = byte

const (
	// LogRecordNormal 生效中
	LogRecordNormal LogRecordType = iota
	// LogRecordDeleted 已删除
	LogRecordDeleted
)

// 日志记录固定头部长度 其中 key 和 value 设置为可变长类型
// crc(4) + type(1) + keySize(max[5]) + valueSize(max[5])
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// LogRecord 数据文件存放的日志记录
// 因为以追加形式写入 故称为日志记录
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// 日志记录头部
type logRecordHeader struct {
	crc        uint32        // crc 校验值
	recordType LogRecordType // LogRecord 类型
	keySize    uint32        // key 长度
	valueSize  uint32        // value 长度
}

// LogRecordPos 数据内存索引 描述日志记录在磁盘的位置
type LogRecordPos struct {
	Fid    uint32 // 文件id 定位数据所在文件
	Offset int64  // 数据偏移量 定位数据在文件中的位置
}

// EncodeLogRecord 对 LogRecord 实例编码
//
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	| crc 校验值  |  type 类型   |    key size |   value size |      key    |      value   |
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	    4字节          1字节        变长（最大5）   变长（最大5）     变长           变长
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	// 按最大长度初始化 header 部分的字节数组
	header := make([]byte, maxLogRecordHeaderSize)
	// 写入 type
	header[4] = logRecord.Type
	// 写入 key size + value size 使用变长类型节省空间
	var index = 5
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))
	// key + value 并计算日志记录的总长度创建对应的字节数组
	var size = index + len(logRecord.Key) + len(logRecord.Value)
	encBytes := make([]byte, size)
	// 拷贝 header 部分
	copy(encBytes[:index], header[:index])
	// 拷贝 key 和 value
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)
	// 写入 crc 校验值 按小端序编码
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(size)
}

// decodeLogRecordHeader 对 Header 头部解码
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	// 长度校验
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}
	var index = 5
	// 获取实际 key size
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n
	// 获取实际 value size
	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}

// getLogRecordCRC 计算日志记录的 crc 校验值
func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}

	// 结合头部和 kv 数据计算
	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)

	return crc
}
