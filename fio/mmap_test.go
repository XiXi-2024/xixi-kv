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
	dir = filepath.Join(t.TempDir(), "mmap_read.txt")
	mmapIO, err := NewMMap(dir)
	assert.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(dir)
	}()

	// 文件为空
	b1 := make([]byte, 10)
	n1, err := mmapIO.Read(b1, 0)
	assert.Equal(t, 0, n1)
	assert.Equal(t, io.EOF, err)

	// 文件中存在数据
	fio, err := NewFileIO(dir)
	assert.Nil(t, err)
	n, err := fio.Write([]byte("aa"))
	assert.Nil(t, err)
	mmapIO.offset += int64(n)
	n, err = fio.Write([]byte("bb"))
	assert.Nil(t, err)
	mmapIO.offset += int64(n)
	n, err = fio.Write([]byte("cc"))
	assert.Nil(t, err)
	mmapIO.offset += int64(n)

	size, err := mmapIO.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(6), size)

	b2 := make([]byte, 2)
	n2, err := mmapIO.Read(b2, 0)
	assert.Nil(t, err)
	assert.Equal(t, 2, n2)
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
	_, err = mmapIO.Write([]byte("test data"))
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
	_, err = mmapIO.Write([]byte("test data"))
	assert.Nil(t, err)

	size, err = mmapIO.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(9), size)
}
