package data

import (
	"github.com/XiXi-2024/xixi-bitcask-kv/fio"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

// 打开文件
func TestOpenDataFile(t *testing.T) {
	dir := os.TempDir()
	t.Log(dir)
	// 打开文件
	dataFile1, err := OpenDataFile(dir, 0, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	// 重复打开相同文件
	dataFile2, err := OpenDataFile(dir, 111, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile2)
	dataFile3, err := OpenDataFile(dir, 111, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile3)
}

// 文件写入
func TestDataFile_Write(t *testing.T) {
	dir := os.TempDir()
	t.Log(dir)
	dataFile, err := OpenDataFile(dir, 0, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	// 向文件写入
	err = dataFile.Write([]byte("aaa"))
	assert.Nil(t, err)
	err = dataFile.Write([]byte("bbb"))
	assert.Nil(t, err)
	err = dataFile.Write([]byte("ccc"))
	assert.Nil(t, err)
}

// 关闭文件
func TestDataFile_Close(t *testing.T) {
	dir := os.TempDir()
	t.Log(dir)
	dataFile, err := OpenDataFile(dir, 123, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("aaa"))
	assert.Nil(t, err)

	err = dataFile.Close()
	assert.Nil(t, err)
}

// 文件持久化
func TestDataFile_Sync(t *testing.T) {
	dir := os.TempDir()
	t.Log(dir)
	dataFile, err := OpenDataFile(dir, 456, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("aaa"))
	assert.Nil(t, err)

	err = dataFile.Sync()
	assert.Nil(t, err)
}

// 从数据文件中读取日志记录
func TestDataFile_ReadLogRecord(t *testing.T) {
	dir := os.TempDir()
	t.Log(dir)
	dataFile, err := OpenDataFile(dir, 6666, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	// 读取单条记录
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask kv go"),
	}
	res1, size1 := EncodeLogRecord(rec1)
	err = dataFile.Write(res1)
	assert.Nil(t, err)
	readRec1, readSize1, err := dataFile.ReadLogRecord(0)
	assert.Nil(t, err)
	assert.Equal(t, rec1, readRec1)
	assert.Equal(t, size1, readSize1)
	t.Log(string(readRec1.Key), string(readRec1.Value))

	// 读取多条记录
	rec2 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("a new value"),
	}
	res2, size2 := EncodeLogRecord(rec2)
	err = dataFile.Write(res2)
	assert.Nil(t, err)
	readRec2, readSize2, err := dataFile.ReadLogRecord(size1)
	assert.Nil(t, err)
	assert.Equal(t, rec2, readRec2)
	assert.Equal(t, size2, readSize2)
	t.Log(string(readRec2.Key), string(readRec2.Value))

	// 读取被删除记录
	rec3 := &LogRecord{
		Key:   []byte("1"),
		Value: []byte(""),
		Type:  LogRecordDeleted,
	}
	res3, size3 := EncodeLogRecord(rec3)
	err = dataFile.Write(res3)
	assert.Nil(t, err)
	readRec3, readSize3, err := dataFile.ReadLogRecord(size1 + size2)
	assert.Nil(t, err)
	assert.Equal(t, rec3, readRec3)
	assert.Equal(t, size3, readSize3)
	t.Log(string(readRec3.Key))
}
