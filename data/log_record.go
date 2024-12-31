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
	// LogRecordTxnFinished 事务完成标识
	LogRecordTxnFinished
)

// 日志记录头部最大长度
// crc(4) + type(1) + keySize(max[5]) + valueSize(max[5]) = 15
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// LogRecord 日志记录数据内容
// 以追加形式写入, 故称为日志记录
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

// LogRecordPos 数据内存索引, 描述日志记录在磁盘的位置
type LogRecordPos struct {
	Fid    uint32 // 文件id, 定位日志记录所在文件
	Offset int64  // 偏移量, 定位日志记录在文件中的位置
	Size   uint32 // 日志记录占用字节数大小
}

// TransactionRecords 事务相关的暂存数据
type TransactionRecords struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

// EncodeLogRecord 对 LogRecord 实例编码
// 返回编码后包含完日志记录的字节数组和数组长度
//
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	| crc 校验值  |  type 类型   |    key size |   value size |      key    |      value   |
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	    4字节          1字节        变长（最大5）   变长（最大5）     变长           变长
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	// 按最大长度初始化头部的字节数组
	header := make([]byte, maxLogRecordHeaderSize)
	// 写入 type
	header[4] = logRecord.Type
	var index = 5
	// 写入 key size + value size, 使用变长类型节省空间
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))
	// 计算日志记录总长度, 创建对应长度的字节数组
	var size = index + len(logRecord.Key) + len(logRecord.Value)
	encBytes := make([]byte, size)
	// 拷贝 header、key、value, 得到完整日志记录的字节数组
	copy(encBytes[:index], header[:index])
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)
	// 写入 crc 校验值, 按小端序编码
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(size)
}

// EncodeLogRecordPos 对索引位置信息实例编码
// 返回编码后的字节数组
func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	// 按可能的最大长度创建字节数组
	buf := make([]byte, 2*binary.MaxVarintLen32+binary.MaxVarintLen64)
	var index = 0
	// 使用变长类型, 节省空间
	index += binary.PutVarint(buf[index:], int64(pos.Fid))
	index += binary.PutVarint(buf[index:], pos.Offset)
	index += binary.PutVarint(buf[index:], int64(pos.Size))
	// 返回实际长度的字节数组
	return buf[:index]
}

// DecodeLogRecordPos 解码 LogRecordPos 实例
func DecodeLogRecordPos(buf []byte) *LogRecordPos {
	var index = 0
	fileId, n := binary.Varint(buf[index:])
	index += n
	offset, n := binary.Varint(buf[index:])
	index += n
	size, n := binary.Varint(buf[index:])
	return &LogRecordPos{Fid: uint32(fileId), Offset: offset, Size: uint32(size)}
}

// decodeLogRecordHeader 解码包含 Header 头部的字节数组
// 返回 logRecordHeader 实例和 Header 真实字节数
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	// 长度校验
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]), // 按小端序解码
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
// todo 接口不应依赖它不需要的东西, 可重构
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
