package datafile

import (
	"github.com/XiXi-2024/xixi-kv/fio"
	"github.com/stretchr/testify/assert"
	"io"
	"os"
	"testing"
)

// 基本读取和写入
func TestDataFile_WriteAndRead(t *testing.T) {
	// 创建临时测试目录
	dir, _ := os.MkdirTemp("", "bitcask-go-datafile")
	defer os.RemoveAll(dir)

	// 打开数据文件
	df, err := OpenFile(dir, 0, DataFileSuffix, fio.StandardFIO)
	assert.Nil(t, err)

	// 测试小数据写入和读取
	data1 := []byte("hello world")
	pos1, err := df.writeSingle(data1)
	assert.Nil(t, err)
	assert.NotNil(t, pos1)
	data2, err := df.read(pos1.BlockID, pos1.Offset)
	assert.Nil(t, err)
	assert.Equal(t, data1, data2)
	t.Log(string(data1), string(data2))

	// 测试大数据写入和读取（跨块）
	data1 = make([]byte, blockSize)
	for i := range data1 {
		data1[i] = byte(i % 256)
	}
	pos1, err = df.writeSingle(data1)
	assert.Nil(t, err)
	assert.NotNil(t, pos1)
	data2, err = df.read(pos1.BlockID, pos1.Offset)
	assert.Nil(t, err)
	assert.Equal(t, data1, data2)

	// 4. 测试边界条件：块边界写入
	// 先写入一些数据，使lastBlockSize接近blockSize
	data1 = make([]byte, blockSize-df.lastBlockSize-chunkHeaderSize+1)
	pos1, err = df.writeSingle(data1)
	assert.Nil(t, err)
	assert.NotNil(t, pos1)
	// 此时再写入数据会触发块填充
	data2 = []byte("test")
	pos2, err := df.writeSingle(data2)
	assert.Nil(t, err)
	// 验证数据写入到了新的块
	assert.Equal(t, df.lastBlockID-1, pos1.BlockID)
	data3, err := df.read(pos2.BlockID, pos2.Offset)
	assert.Nil(t, err)
	assert.Equal(t, data2, data3)

	// 测试错误处理：无效的读取位置
	_, err = df.read(1000, 0) // 读取不存在的块
	assert.Equal(t, io.EOF, err)
}

// DataReader
func TestDataReader(t *testing.T) {
	// 创建临时测试目录
	dir, _ := os.MkdirTemp("", "bitcask-go-data_reader")
	defer os.RemoveAll(dir)

	// 打开数据文件
	df, err := OpenFile(dir, 0, DataFileSuffix, fio.StandardFIO)
	assert.Nil(t, err)

	// 1. 测试空文件读取
	reader := df.NewReader()
	_, _, err = reader.NextLogRecord()
	assert.Equal(t, io.EOF, err)

	// 2. 测试基本读取功能
	// 写入一条小数据
	logRecord1 := &LogRecord{
		Key:   []byte("key1"),
		Value: []byte("value1"),
	}
	header := make([]byte, MaxLogRecordHeaderSize)
	pos1, err := df.WriteLogRecord(logRecord1, header)
	assert.Nil(t, err)
	// 读取
	readRecord1, readPos1, err := reader.NextLogRecord()
	assert.Nil(t, err)
	assert.Equal(t, logRecord1.Key, readRecord1.Key)
	assert.Equal(t, logRecord1.Value, readRecord1.Value)
	assert.Equal(t, pos1.BlockID, readPos1.BlockID)
	assert.Equal(t, pos1.Offset, readPos1.Offset)

	// 3. 测试跨块读取
	// 写入一条大数据，确保跨块
	largeValue := make([]byte, blockSize)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}
	logRecord2 := &LogRecord{
		Key:   []byte("key2"),
		Value: largeValue,
	}
	pos2, err := df.WriteLogRecord(logRecord2, header)
	assert.Nil(t, err)
	// 读取大数据
	readRecord2, readPos2, err := reader.NextLogRecord()
	assert.Nil(t, err)
	assert.Equal(t, logRecord2.Key, readRecord2.Key)
	assert.Equal(t, logRecord2.Value, readRecord2.Value)
	assert.Equal(t, pos2.BlockID, readPos2.BlockID)
	assert.Equal(t, pos2.Offset, readPos2.Offset)

	// 4. 测试读取到文件末尾
	_, _, err = reader.NextLogRecord()
	assert.Equal(t, io.EOF, err)

	// 5. 测试已关闭文件的读取
	df.Close()
	_, _, err = reader.NextLogRecord()
	assert.Equal(t, ErrClosed, err)
}

// TestDataReader_HintRecord 测试DataReader对HintRecord的读取
func TestDataReader_HintRecord(t *testing.T) {
	// 创建临时测试目录
	dir, _ := os.MkdirTemp("", "bitcask-go-datareader-hint")
	defer os.RemoveAll(dir)

	// 打开数据文件
	df, err := OpenFile(dir, 0, HintFileSuffix, fio.StandardFIO)
	assert.Nil(t, err)

	// 1. 写入hint记录
	key := []byte("test-key")
	pos := &DataPos{
		Fid:     1,
		BlockID: 2,
		Offset:  100,
		Size:    200,
	}
	hintPos := make([]byte, MaxLogRecordPosSize)
	err = df.WriteHintRecord(key, hintPos, pos)
	assert.Nil(t, err)

	// 2. 读取hint记录
	reader := df.NewReader()
	readKey, readPos, err := reader.NextHintRecord()
	assert.Nil(t, err)
	assert.Equal(t, key, readKey)
	assert.Equal(t, pos.Fid, readPos.Fid)
	assert.Equal(t, pos.BlockID, readPos.BlockID)
	assert.Equal(t, pos.Offset, readPos.Offset)
	assert.Equal(t, pos.Size, readPos.Size)

	// 3. 测试读取到文件末尾
	_, _, err = reader.NextHintRecord()
	assert.Equal(t, io.EOF, err)

	// 4. 测试已关闭文件的读取
	df.Close()
	_, _, err = reader.NextHintRecord()
	assert.Equal(t, ErrClosed, err)
}
