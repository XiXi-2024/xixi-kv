package data

import "encoding/binary"

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
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}

// decodeLogRecordHeader 解码 获取日志记录的 Header 头部信息 返回实际长度
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	return nil, 0
}

// getLogRecordCRC 生成 CRC 值
func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	return 0
}
