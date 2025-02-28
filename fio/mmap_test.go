package fio

import (
	"github.com/stretchr/testify/assert"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestMMap_Read(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcask-go-mmap")
	assert.Nil(t, err)
	dirName := filepath.Join(t.TempDir(), "mmap_read.txt")
	defer func() {
		_ = os.RemoveAll(dir)
	}()

	// 文件为空
	mmapIO, err := NewMMap(dirName)
	assert.Nil(t, err)
	b1 := make([]byte, 10)
	n1, err := mmapIO.Read(b1, 0)
	assert.Equal(t, 0, n1)
	assert.Equal(t, io.EOF, err)
	err = mmapIO.Close()
	assert.Nil(t, err)

	// 文件中存在数据
	fio, err := NewFileIO(dirName)
	assert.Nil(t, err)
	_, err = fio.Write([]byte("aa"))
	assert.Nil(t, err)
	_, err = fio.Write([]byte("bb"))
	assert.Nil(t, err)
	_, err = fio.Write([]byte("cc"))
	assert.Nil(t, err)
	mmapIO, err = NewMMap(dirName)
	assert.Nil(t, err)
	size, err := mmapIO.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(6), size)
	b2 := make([]byte, 2)
	n2, err := mmapIO.Read(b2, 0)
	assert.Nil(t, err)
	assert.Equal(t, 2, n2)
	assert.Equal(t, []byte("aa"), b2)
	err = mmapIO.Close()
	assert.Nil(t, err)

	// 读取大量数据
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 128)
	}
	for i := 0; i < 1024; i++ {
		_, err = fio.Write(data)
		assert.Nil(t, err)
	}
	mmapIO, err = NewMMap(dirName)
	assert.Nil(t, err)
	b3 := make([]byte, 1024)
	off := 0
	for {
		n, err := mmapIO.Read(b3, int64(off))
		if err == io.EOF {
			break
		}
		off += n
	}
}

func TestMMap_Write(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcask-go-mmap")
	assert.Nil(t, err)
	dir = filepath.Join(t.TempDir(), "mmap_write.txt")
	mmapIO, err := NewMMap(dir)
	assert.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(dir)
	}()

	// 写入空数据
	n1, err := mmapIO.Write([]byte(""))
	assert.Nil(t, err)
	assert.Equal(t, 0, n1)

	// 写入正常数据
	n2, err := mmapIO.Write([]byte("hello"))
	assert.Nil(t, err)
	assert.Equal(t, 5, n2)

	// 验证写入的数据
	b := make([]byte, 5)
	_, err = mmapIO.Read(b, 0)
	assert.Nil(t, err)
	assert.Equal(t, []byte("hello"), b)

	// 追加写入数据
	n3, err := mmapIO.Write([]byte("world"))
	assert.Nil(t, err)
	assert.Equal(t, 5, n3)

	// 验证追加的数据
	b2 := make([]byte, 5)
	_, err = mmapIO.Read(b2, 5)
	assert.Nil(t, err)
	assert.Equal(t, []byte("world"), b2)
}

func TestMMap_Sync(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcask-go-mmap")
	assert.Nil(t, err)
	dirName := filepath.Join(dir, "mmap_sync.txt")
	mmapIO, err := NewMMap(dirName)
	assert.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(dirName)
	}()

	// 空文件同步
	err = mmapIO.Sync()
	assert.Nil(t, err)

	// 写入数据后同步, 能正常读取数据
	b := []byte("test data")
	_, err = mmapIO.Write(b)
	assert.Nil(t, err)
	err = mmapIO.Sync()
	assert.Nil(t, err)
	c := make([]byte, 9)
	_, err = mmapIO.Read(c, 0)
	assert.Nil(t, err)
	assert.Equal(t, b, c)

	// 写入大量数据后文件大小正确
	for i := 0; i < 1024; i++ {
		_, err = mmapIO.Write(b)
	}
	err = mmapIO.Sync()
	assert.Nil(t, err)

	// 重复同步
	_, err = mmapIO.Write([]byte(" sync"))
	assert.Nil(t, err)
	err = mmapIO.Sync()
	assert.Nil(t, err)
	err = mmapIO.Sync()
	assert.Nil(t, err)
}

func TestMMap_Close(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcask-go-mmap")
	assert.Nil(t, err)
	dir = filepath.Join(t.TempDir(), "mmap_close.txt")
	mmapIO, err := NewMMap(dir)
	assert.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(dir)
	}()

	// 写入数据
	_, err = mmapIO.Write([]byte("test datafile"))
	assert.Nil(t, err)

	// 重复关闭
	err = mmapIO.Close()
	assert.Nil(t, err)
	err = mmapIO.Close()
	assert.NotNil(t, err)
}

func TestMMap_Size(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcask-go-mmap")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)
	dir = filepath.Join(t.TempDir(), "mmap_size.txt")
	mmapIO, err := NewMMap(dir)
	assert.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(dir)
	}()

	// 初始大小应该为0
	size, err := mmapIO.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(0), size)

	// 写入数据后检查大小
	_, err = mmapIO.Write([]byte("test datafile"))
	assert.Nil(t, err)

	size, err = mmapIO.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(13), size)
}
