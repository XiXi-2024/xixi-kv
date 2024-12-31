package fio

import (
	"github.com/stretchr/testify/assert"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// 读取
func TestMMap_Read(t *testing.T) {
	path := filepath.Join(os.TempDir(), "mmap-a.data")
	//t.Log(path)
	//defer destroyFile(path) // win 环境下手动删除
	mmapIO, err := NewMMapIOManager(path)
	assert.Nil(t, err)

	// 文件为空
	b1 := make([]byte, 10)
	n1, err := mmapIO.Read(b1, 0)
	assert.Equal(t, 0, n1)
	assert.Equal(t, io.EOF, err)

	// 文件中存在数据
	fio, err := NewFileIOManager(path)
	assert.Nil(t, err)
	_, err = fio.Write([]byte("aa"))
	assert.Nil(t, err)
	_, err = fio.Write([]byte("bb"))
	assert.Nil(t, err)
	_, err = fio.Write([]byte("cc"))
	assert.Nil(t, err)

	mmapIO2, err := NewMMapIOManager(path)
	assert.Nil(t, err)
	size, err := mmapIO2.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(6), size)

	b2 := make([]byte, 2)
	n2, err := mmapIO2.Read(b2, 0)
	assert.Nil(t, err)
	assert.Equal(t, 2, n2)
}
