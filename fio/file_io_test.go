package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

// 创建
func TestNewFileIOManager(t *testing.T) {
	path := filepath.Join(os.TempDir(), "a.datafile")
	//t.Log(path)
	fio, err := NewFileIO(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
}

// 读取
func TestFileIO_Read(t *testing.T) {
	path := filepath.Join(os.TempDir(), "a.datafile")
	//t.Log(path)
	fio, err := NewFileIO(path)
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("key-a"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte("key-b"))
	assert.Nil(t, err)

	b1 := make([]byte, 5)
	n, err := fio.Read(b1, 0)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-a"), b1)

	b2 := make([]byte, 5)
	n, err = fio.Read(b2, 5)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-b"), b2)

	err = fio.Close()
	assert.Nil(t, err)
}

// 写入
func TestFileIO_Write(t *testing.T) {
	path := filepath.Join(os.TempDir(), "a.datafile")
	//t.Log(path)
	fio, err := NewFileIO(path)
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("xixi"))
	assert.Equal(t, 4, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("nihao"))
	assert.Equal(t, 5, n)
	assert.Nil(t, err)

	err = fio.Close()
	assert.Nil(t, err)
}

// 关闭
func TestFileIO_Close(t *testing.T) {
	path := filepath.Join(os.TempDir(), "a.datafile")
	//t.Log(path)
	fio, err := NewFileIO(path)
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
}

// 持久化
func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join(os.TempDir(), "a.datafile")
	//t.Log(path)
	fio, err := NewFileIO(path)
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)

	err = fio.Close()
	assert.Nil(t, err)
}

// 清除生成的临时文件, 避免影响后续测试结果
func destroyFile(path string) {
	if err := os.RemoveAll(path); err != nil {
		panic(err)
	}
}
