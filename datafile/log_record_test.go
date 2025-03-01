package datafile

import (
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/bytebufferpool"
	"hash/crc32"
	"testing"
)

// TestEncodeLogRecord 测试日志记录的编码
func TestEncodeLogRecord(t *testing.T) {
	// 1. 测试普通记录的编码
	record := &LogRecord{
		Type:    LogRecordNormal,
		Key:     []byte("test-key"),
		Value:   []byte("test-value"),
		BatchID: 123,
	}
	header := make([]byte, MaxLogRecordHeaderSize)
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	EncodeLogRecord(record, header, buf)
	// 解码并验证
	value := DecodeLogRecordValue(buf.Bytes())
	assert.Equal(t, record.Value, value)

	// 2. 测试删除记录的编码
	delRecord := &LogRecord{
		Type:    LogRecordDeleted,
		Key:     []byte("deleted-key"),
		Value:   nil,
		BatchID: 456,
	}

	buf.Reset()
	EncodeLogRecord(delRecord, header, buf)
	value = DecodeLogRecordValue(buf.Bytes())
	assert.Equal(t, delRecord.Value, value)

	// 3. 测试批处理完成记录的编码
	batchRecord := &LogRecord{
		Type:    LogRecordBatchFinished,
		Key:     nil,
		Value:   nil,
		BatchID: 789,
	}
	buf.Reset()
	EncodeLogRecord(batchRecord, header, buf)
	value = DecodeLogRecordValue(buf.Bytes())
	assert.Equal(t, delRecord.Value, value)
}

// TestEncodeDecodeHintRecord 测试索引记录的编解码
func TestEncodeDecodeHintRecord(t *testing.T) {
	// 1. 测试基本编解码
	key := []byte("test-key")
	pos := &DataPos{
		Fid:     1,
		BlockID: 2,
		Offset:  100,
		Size:    200,
	}

	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	hintPos := make([]byte, MaxLogRecordPosSize)
	EncodeHintRecord(key, pos, hintPos, buf)
	decodeKey, decodePos := DecodeHintRecord(buf.Bytes())

	assert.Equal(t, key, decodeKey)
	assert.Equal(t, pos.Fid, decodePos.Fid)
	assert.Equal(t, pos.BlockID, decodePos.BlockID)
	assert.Equal(t, pos.Offset, decodePos.Offset)
	assert.Equal(t, pos.Size, decodePos.Size)

	// 2. 测试空key的情况
	buf.Reset()
	key = []byte{}
	EncodeHintRecord(key, pos, hintPos, buf)
	decodeKey, decodePos = DecodeHintRecord(buf.Bytes())

	assert.Equal(t, key, decodeKey)
	assert.Equal(t, pos.Fid, decodePos.Fid)

	// 3. 测试大数值的情况
	buf.Reset()
	pos = &DataPos{
		Fid:     1<<32 - 1,
		BlockID: 1<<32 - 1,
		Offset:  1<<32 - 1,
		Size:    1<<32 - 1,
	}
	EncodeHintRecord(key, pos, hintPos, buf)
	_, decodePos = DecodeHintRecord(buf.Bytes())

	assert.Equal(t, pos.Fid, decodePos.Fid)
	assert.Equal(t, pos.BlockID, decodePos.BlockID)
	assert.Equal(t, pos.Offset, decodePos.Offset)
	assert.Equal(t, pos.Size, decodePos.Size)
}

// TestDecodeChunk 测试数据块的解码
func TestDecodeChunk(t *testing.T) {
	// 1. 测试正常数据块的解码
	data := []byte("test-data")
	block := make([]byte, chunkHeaderSize+len(data))
	// 写入长度
	binary.LittleEndian.PutUint16(block[4:6], uint16(len(data)))
	// 写入类型
	block[6] = Full
	// 写入数据
	copy(block[chunkHeaderSize:], data)
	// 计算并写入校验和
	checksum := crc32.ChecksumIEEE(block[4 : chunkHeaderSize+len(data)])
	binary.LittleEndian.PutUint32(block[:4], checksum)

	decoded, chunkType, err := DecodeChunk(block)
	assert.Nil(t, err)
	assert.Equal(t, data, decoded)
	assert.Equal(t, Full, chunkType)

	// 2. 测试校验和错误的情况
	block[0] = block[0] + 1 // 修改校验和
	_, _, err = DecodeChunk(block)
	assert.Equal(t, ErrInvalidCRC, err)

	// 3. 测试不同块类型
	blockTypes := []ChunkType{First, Middle, Last}
	for _, typ := range blockTypes {
		block[6] = typ
		checksum = crc32.ChecksumIEEE(block[4 : chunkHeaderSize+len(data)])
		binary.LittleEndian.PutUint32(block[:4], checksum)

		_, chunkType, err = DecodeChunk(block)
		assert.Nil(t, err)
		assert.Equal(t, typ, chunkType)
	}
}
