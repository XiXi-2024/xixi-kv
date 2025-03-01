package index

import (
	"github.com/XiXi-2024/xixi-kv/datafile"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBTree_put(t *testing.T) {
	bt := newBTree()

	// 添加 key 为 nil 的元素
	res1 := bt.put(nil, &datafile.DataPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)

	// 添加正常元素
	res2 := bt.put([]byte("a"), &datafile.DataPos{Fid: 1, Offset: 2})
	assert.Nil(t, res2)

	// 添加 key 重复元素
	res3 := bt.put([]byte("a"), &datafile.DataPos{Fid: 11, Offset: 12})
	assert.Equal(t, res3.Fid, uint32(1))
	assert.Equal(t, res3.Offset, uint32(2))
}

func TestBTree_get(t *testing.T) {
	bt := newBTree()

	res1 := bt.put(nil, &datafile.DataPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)

	// 查询 key 为 nil 的元素
	pos1 := bt.get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, uint32(100), pos1.Offset)

	res2 := bt.put([]byte("a"), &datafile.DataPos{Fid: 1, Offset: 2})
	assert.Nil(t, res2)
	res3 := bt.put([]byte("a"), &datafile.DataPos{Fid: 1, Offset: 3})
	assert.Equal(t, res3.Fid, uint32(1))
	assert.Equal(t, res3.Offset, uint32(2))

	// 查询重复添加的元素
	pos2 := bt.get([]byte("a"))
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, uint32(3), pos2.Offset)
}

func TestBTree_delete(t *testing.T) {
	bt := newBTree()
	res1 := bt.put(nil, &datafile.DataPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)
	// 删除 key 为 nil 的元素
	res2 := bt.delete(nil)
	assert.Equal(t, res2.Fid, uint32(1))
	assert.Equal(t, res2.Offset, uint32(100))

	res3 := bt.put([]byte("aaa"), &datafile.DataPos{Fid: 22, Offset: 33})
	assert.Nil(t, res3)
	// 删除正常元素
	res4 := bt.delete([]byte("aaa"))
	assert.Equal(t, res4.Fid, uint32(22))
	assert.Equal(t, res4.Offset, uint32(33))
}

func TestBTree_iterator(t *testing.T) {
	bt1 := newBTree()
	// btreeIndex 为空
	iter1 := bt1.iterator(false)
	assert.Equal(t, false, iter1.valid())

	// btreeIndex 非空
	bt1.put([]byte("code"), &datafile.DataPos{Fid: 1, Offset: 10})
	iter2 := bt1.iterator(false)
	assert.Equal(t, true, iter2.valid())
	assert.NotNil(t, iter2.key())
	assert.NotNil(t, iter2.value())
	iter2.next()
	assert.Equal(t, false, iter2.valid())

	bt1.put([]byte("acee"), &datafile.DataPos{Fid: 1, Offset: 10})
	bt1.put([]byte("eede"), &datafile.DataPos{Fid: 1, Offset: 10})
	bt1.put([]byte("bbcd"), &datafile.DataPos{Fid: 1, Offset: 10})

	// 升序遍历
	iter3 := bt1.iterator(false)
	for iter3.rewind(); iter3.valid(); iter3.next() {
		assert.NotNil(t, iter3.key())
	}

	// 逆序遍历
	iter4 := bt1.iterator(true)
	for iter4.rewind(); iter4.valid(); iter4.next() {
		assert.NotNil(t, iter4.key())
	}

	// Seek
	iter5 := bt1.iterator(false)
	for iter5.seek([]byte("cc")); iter5.valid(); iter5.next() {
		assert.NotNil(t, iter5.key())
	}
}
