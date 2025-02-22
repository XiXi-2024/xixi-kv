package datafile

import (
	"encoding/binary"
	"github.com/valyala/bytebufferpool"
	"hash/crc32"
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

type LogRecordStatus = byte

const (
	// LogRecordNormal 生效中
	LogRecordNormal LogRecordStatus = iota
	// LogRecordDeleted 已删除
	LogRecordDeleted
	// LogRecordTxnFinished 事务已完成
	LogRecordTxnFinished
)

const (
	// MaxChunkHeaderSize LogRecord头部最大长度
	// type(1) + key size(5) + value size(5)
	MaxChunkHeaderSize = binary.MaxVarintLen32*3 + binary.MaxVarintLen64

	// SegmentId(5) + BlockNumber(5) + ChunkOffset(10) + ChunkSize(5) = 25
	maxLogRecordPosSize = binary.MaxVarintLen32*3 + binary.MaxVarintLen64
)

// LogRecord 日志记录数据内容
// 以追加形式写入, 故称为日志记录
type LogRecord struct {
	Type  LogRecordStatus
	Key   []byte
	Value []byte
}

// DataPos 数据记录的起始索引
// 当一条数据记录过大时, 会被分割为多个 chunk 存储
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
// 返回编码后包含完日志记录的字节数组和数组长度
func EncodeLogRecord(logRecord *LogRecord, header []byte, buf *bytebufferpool.ByteBuffer) []byte {
	header[0] = logRecord.Type
	idx := 1
	// key size
	idx += binary.PutVarint(header[idx:], int64(len(logRecord.Key)))
	// value size
	idx += binary.PutVarint(header[idx:], int64(len(logRecord.Value)))
	_, _ = buf.Write(header[:idx])
	_, _ = buf.Write(logRecord.Key)
	_, _ = buf.Write(logRecord.Value)
	return buf.Bytes()
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

	// key
	key := make([]byte, keySize)
	copy(key, data[idx:idx+int(keySize)])
	idx += int(keySize)

	// value
	value := make([]byte, valueSize)
	copy(value, data[idx:idx+int(valueSize)])

	return &LogRecord{
		Type:  recordType,
		Key:   key,
		Value: value,
	}
}

func EncodeHintRecord(key []byte, pos *DataPos) []byte {
	// todo 优化点：提供复用缓冲区
	buf := make([]byte, maxLogRecordPosSize)
	var idx = 0
	// Fid
	idx += binary.PutUvarint(buf[idx:], uint64(pos.Fid))
	// BlockID
	idx += binary.PutUvarint(buf[idx:], uint64(pos.BlockID))
	// Offset
	idx += binary.PutUvarint(buf[idx:], uint64(pos.Offset))
	// Size
	idx += binary.PutUvarint(buf[idx:], uint64(pos.Size))

	// key
	result := make([]byte, idx+len(key))
	copy(result, buf[:idx])
	copy(result[idx:], key)
	return result
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

func EncodeMergeFinRecord(id FileID) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, id)
	return buf
}
