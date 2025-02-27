package datafile

import (
	"encoding/binary"
	"github.com/valyala/bytebufferpool"
	"hash/crc32"
)

type LogRecordType = byte

const (
	// LogRecordNormal 生效中
	LogRecordNormal LogRecordType = iota
	// LogRecordDeleted 已删除
	LogRecordDeleted
	// LogRecordBatchFinished 所属批次的批处理操作已完成
	LogRecordBatchFinished
)

type ChunkType = byte

const (
	Full ChunkType = iota
	First
	Middle
	Last
)

const (
	// chunk 头部长度
	// Checksum(4) + Length(2) + ChunkType(1) = 7 Byte
	chunkHeaderSize = 7

	blockSize = 32 * 1024
)

const (
	// MaxLogRecordHeaderSize LogRecord头部最大长度
	// type(1) + key size(5) + value size(5) + batchID(10) = 21
	MaxLogRecordHeaderSize = binary.MaxVarintLen32*2 + binary.MaxVarintLen64 + 1

	// MaxLogRecordPosSize 日志索引最大长度
	// SegmentId(5) + BlockNumber(5) + ChunkOffset(10) + ChunkSize(5) = 25
	MaxLogRecordPosSize = binary.MaxVarintLen32*3 + binary.MaxVarintLen64
)

// LogRecord 日志记录数据内容
// 以追加形式写入, 故称为日志记录
type LogRecord struct {
	Type    LogRecordType // 记录类型
	Key     []byte        // key
	Value   []byte        // value
	BatchID uint64        // 批处理唯一 ID
}

// DataPos 数据记录的起始索引
// 当一条数据记录过大时, 会被分割为多个 chunk 存储
// todo 优化点：索引字段私有化
type DataPos struct {
	Fid     FileID // 数据文件 ID, 定位 LogRecord 所在文件
	BlockID uint32 // Block ID, 定位 LogRecord 所在 Block
	Offset  uint32 // LogRecord 在 Block 的相对偏移量
	Size    uint32 // LogRecord 长度
}

// TransactionRecords 事务相关的暂存数据
type TransactionRecords struct {
	Record *LogRecord
	Pos    *DataPos
}

// EncodeLogRecord 对 LogRecord 实例编码
func EncodeLogRecord(logRecord *LogRecord, header []byte, buf *bytebufferpool.ByteBuffer) {
	// type
	header[0] = logRecord.Type

	idx := 1
	// key size
	idx += binary.PutVarint(header[idx:], int64(len(logRecord.Key)))

	// value size
	idx += binary.PutVarint(header[idx:], int64(len(logRecord.Value)))

	// batch id
	idx += binary.PutUvarint(header[idx:], logRecord.BatchID)

	buf.B = append(buf.B, header[:idx]...)
	buf.B = append(buf.B, logRecord.Key...)
	buf.B = append(buf.B, logRecord.Value...)
}

func DecodeLogRecord(data []byte) *LogRecord {
	// type
	recordType := data[0]

	// key size
	idx := 1
	keySize, n := binary.Varint(data[idx:])
	idx += n

	// value size
	valueSize, n := binary.Varint(data[idx:])
	idx += n

	// batchID
	batchID, n := binary.Uvarint(data[idx:])
	idx += n

	// key
	var key []byte
	if keySize > 0 {
		key = make([]byte, keySize)
		copy(key, data[idx:idx+int(keySize)])
		idx += int(keySize)
	}

	// value
	var value []byte
	if valueSize > 0 {
		value = make([]byte, valueSize)
		copy(value, data[idx:idx+int(valueSize)])
	}

	return &LogRecord{
		Type:    recordType,
		Key:     key,
		Value:   value,
		BatchID: batchID,
	}
}

func DecodeLogRecordValue(data []byte) []byte {
	idx := 1
	keySize, n := binary.Varint(data[idx:])
	idx += n
	valueSize, n := binary.Varint(data[idx:])
	idx += n
	_, n = binary.Uvarint(data[idx:])
	idx += n

	if valueSize == 0 {
		return nil
	}
	return data[idx+int(keySize):]
}

func EncodeHintRecord(key []byte, pos *DataPos, hintPos []byte, buf *bytebufferpool.ByteBuffer) {
	var idx = 0
	// Fid
	idx += binary.PutUvarint(hintPos[idx:], uint64(pos.Fid))
	// BlockID
	idx += binary.PutUvarint(hintPos[idx:], uint64(pos.BlockID))
	// Offset
	idx += binary.PutUvarint(hintPos[idx:], uint64(pos.Offset))
	// Size
	idx += binary.PutUvarint(hintPos[idx:], uint64(pos.Size))

	// key
	buf.B = append(buf.B, hintPos[:idx]...)
	buf.B = append(buf.B, key...)
}

func DecodeHintRecord(buf []byte) ([]byte, *DataPos) {
	var idx = 0
	// SegmentId
	segmentId, n := binary.Uvarint(buf[idx:])
	idx += n
	// BlockID
	blockID, n := binary.Uvarint(buf[idx:])
	idx += n
	// Offset
	offset, n := binary.Uvarint(buf[idx:])
	idx += n
	// Size
	size, n := binary.Uvarint(buf[idx:])
	idx += n
	// Key
	key := buf[idx:]

	return key, &DataPos{
		Fid:     FileID(segmentId),
		BlockID: uint32(blockID),
		Offset:  uint32(offset),
		Size:    uint32(size),
	}
}

func DecodeChunk(block []byte) ([]byte, ChunkType, error) {
	// length
	length := binary.LittleEndian.Uint16(block[4:6])
	start, end := chunkHeaderSize, chunkHeaderSize+uint32(length)
	checksum := crc32.ChecksumIEEE(block[4:end])
	savedSum := binary.LittleEndian.Uint32(block[:4])
	if savedSum != checksum {
		return nil, 0, ErrInvalidCRC
	}
	return block[start:end], block[6], nil
}
