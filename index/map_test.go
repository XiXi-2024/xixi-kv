package index

import (
	"github.com/XiXi-2024/xixi-bitcask-kv/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

// 添加
func TestMap_Put(t *testing.T) {
	mp := NewMap()

	// 添加 key 为 nil 的元素
	res1 := mp.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)

	// 添加正常元素
	res2 := mp.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res2)

	// 添加 key 重复元素
	res3 := mp.Put([]byte("a"), &data.LogRecordPos{Fid: 11, Offset: 12})
	assert.Equal(t, res3.Fid, uint32(1))
	assert.Equal(t, res3.Offset, int64(2))
}

// 查询
func TestMap_Get(t *testing.T) {
	mp := NewMap()

	res1 := mp.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)

	// 查询 key 为 nil 的元素
	pos1 := mp.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	// 查询重复添加的元素
	res2 := mp.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res2)
	res3 := mp.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.Equal(t, res3.Fid, uint32(1))
	assert.Equal(t, res3.Offset, int64(2))
	pos2 := mp.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(3), pos2.Offset)
}

func TestMap_Delete(t *testing.T) {
	mp := NewMap()
	res1 := mp.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)
	// 删除 key 为 nil 的元素
	res2, ok1 := mp.Delete(nil)
	assert.True(t, ok1)
	assert.Equal(t, res2.Fid, uint32(1))
	assert.Equal(t, res2.Offset, int64(100))
	pos := mp.Get(nil)
	assert.Nil(t, pos)

	res3 := mp.Put([]byte("aaa"), &data.LogRecordPos{Fid: 22, Offset: 33})
	assert.Nil(t, res3)
	// 删除正常元素
	res4, ok2 := mp.Delete([]byte("aaa"))
	assert.True(t, ok2)
	assert.Equal(t, res4.Fid, uint32(22))
	assert.Equal(t, res4.Offset, int64(33))
	pos = mp.Get([]byte("aaa"))
	assert.Nil(t, pos)
}

func TestMap_Iterator(t *testing.T) {
	mp := NewMap()
	// 索引为空
	iter1 := mp.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	// 索引非空
	mp.Put([]byte("code"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter2 := mp.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	mp.Put([]byte("acee"), &data.LogRecordPos{Fid: 1, Offset: 10})
	mp.Put([]byte("eede"), &data.LogRecordPos{Fid: 1, Offset: 10})
	mp.Put([]byte("bbcd"), &data.LogRecordPos{Fid: 1, Offset: 10})
	// 升序遍历
	iter3 := mp.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}

	// 逆序遍历
	iter4 := mp.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		assert.NotNil(t, iter4.Key())
	}

	// Seek
	iter5 := mp.Iterator(false)
	for iter5.Seek([]byte("cc")); iter5.Valid(); iter5.Next() {
		assert.NotNil(t, iter5.Key())
	}
}
