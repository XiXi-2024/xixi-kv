package index

import (
	"github.com/XiXi-2024/xixi-kv/datafile"
	"github.com/XiXi-2024/xixi-kv/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

// 测试基本的Put操作
func TestShardedIndex_Put(t *testing.T) {
	// 创建分片索引实例
	index := NewShardedIndex(BTree, 16)
	assert.NotNil(t, index)

	// 测试基本的Put操作
	key := []byte("test_key")
	pos := createDataPos(1, 100, 200)

	// 首次Put应返回nil
	oldPos := index.Put(key, pos)
	assert.Nil(t, oldPos)

	// 再次Put相同的key应返回之前的pos
	newPos := createDataPos(2, 300, 400)
	oldPos = index.Put(key, newPos)
	assert.Equal(t, pos, oldPos)

	// 验证最新值已更新
	result := index.Get(key)
	assert.Equal(t, newPos, result)
}

// 测试基本的Get操作
func TestShardedIndex_Get(t *testing.T) {
	index := NewShardedIndex(BTree, 16)

	// 测试获取不存在的key
	result := index.Get([]byte("not_exist"))
	assert.Nil(t, result)

	// 测试获取存在的key
	key := []byte("test_key")
	pos := createDataPos(1, 100, 200)
	index.Put(key, pos)
	result = index.Get(key)
	assert.Equal(t, pos, result)
}

// 测试基本的Delete操作
func TestShardedIndex_Delete(t *testing.T) {
	index := NewShardedIndex(BTree, 16)

	// 测试删除不存在的key
	pos := index.Delete([]byte("not_exist"))
	assert.Nil(t, pos)

	// 测试删除存在的key
	key := []byte("test_key")
	pos1 := createDataPos(1, 100, 200)
	index.Put(key, pos1)
	pos2 := index.Delete(key)
	assert.Equal(t, pos1, pos2)

	// 验证删除后无法获取
	result := index.Get(key)
	assert.Nil(t, result)
}

// 测试Size操作
func TestShardedIndex_Size(t *testing.T) {
	index := NewShardedIndex(BTree, 16)

	// 初始大小应为0
	assert.Equal(t, 0, index.Size())

	// 添加一些数据
	for i := 0; i < 10; i++ {
		pos := createDataPos(0, 0, 0)
		index.Put(utils.GetTestKey(i), pos)
	}

	// 验证大小为10
	assert.Equal(t, 10, index.Size())

	// 删除一些数据
	for i := 0; i < 5; i++ {
		index.Delete(utils.GetTestKey(i))
	}

	// 验证大小为5
	assert.Equal(t, 5, index.Size())
}

// 测试Close操作
func TestShardedIndex_Close(t *testing.T) {
	index := NewShardedIndex(BTree, 16)

	// 添加一些数据
	for i := 0; i < 10; i++ {
		pos := createDataPos(0, 0, 0)
		index.Put(utils.GetTestKey(i), pos)
	}

	// 关闭索引
	err := index.Close()
	assert.Nil(t, err)

	// 验证关闭后的操作都返回nil
	key := utils.GetTestKey(0)
	assert.Nil(t, index.Get(key))
	assert.Nil(t, index.Put(key, createDataPos(1, 0, 0)))
	pos := index.Delete(key)
	assert.Nil(t, pos)
	assert.Equal(t, 0, index.Size())
}

// 测试迭代器功能
func TestShardedIndex_Iterator(t *testing.T) {
	// 创建分片索引实例
	index := NewShardedIndex(BTree, 16)

	// 准备测试数据
	testData := map[string]*datafile.DataPos{
		"key1": createDataPos(1, 100, 200),
		"key2": createDataPos(2, 200, 300),
		"key3": createDataPos(3, 300, 400),
		"key4": createDataPos(4, 400, 500),
		"key5": createDataPos(5, 500, 600),
	}

	// 插入测试数据
	for key, pos := range testData {
		index.Put([]byte(key), pos)
	}

	// 测试正向遍历
	t.Run("Forward iterator", func(t *testing.T) {
		it := index.Iterator(false)
		defer it.Close()

		// 验证迭代顺序
		expectedKeys := []string{"key1", "key2", "key3", "key4", "key5"}
		for _, expectedKey := range expectedKeys {
			assert.True(t, it.Valid())
			assert.Equal(t, []byte(expectedKey), it.Key())
			assert.Equal(t, testData[expectedKey], it.Value())
			it.Next()
		}
		assert.False(t, it.Valid())
	})

	// 测试反向遍历
	t.Run("Reverse iterator", func(t *testing.T) {
		it := index.Iterator(true)
		defer it.Close()

		// 验证迭代顺序
		expectedKeys := []string{"key5", "key4", "key3", "key2", "key1"}
		for _, expectedKey := range expectedKeys {
			assert.True(t, it.Valid())
			assert.Equal(t, []byte(expectedKey), it.Key())
			assert.Equal(t, testData[expectedKey], it.Value())
			it.Next()
		}
		assert.False(t, it.Valid())
	})

	// 测试Seek功能
	t.Run("Seek", func(t *testing.T) {
		// 正向Seek
		it := index.Iterator(false)
		defer it.Close()

		// Seek到存在的key
		it.Seek([]byte("key3"))
		assert.True(t, it.Valid())
		assert.Equal(t, []byte("key3"), it.Key())

		// Seek到不存在的key
		it.Seek([]byte("key3.5"))
		assert.True(t, it.Valid())
		assert.Equal(t, []byte("key4"), it.Key())

		// 反向Seek
		it = index.Iterator(true)
		it.Seek([]byte("key3"))
		assert.True(t, it.Valid())
		assert.Equal(t, []byte("key3"), it.Key())

		// Seek超出范围
		it.Seek([]byte("key6"))
		assert.Equal(t, []byte("key3"), it.Key())
	})

	// 测试Rewind功能
	t.Run("Rewind", func(t *testing.T) {
		it := index.Iterator(false)
		defer it.Close()
		assert.True(t, it.Valid())

		// 遍历到中间位置
		it.Next()
		it.Next()
		assert.Equal(t, []byte("key3"), it.Key())

		// 重置迭代器
		it.Rewind()
		assert.True(t, it.Valid())
		t.Log(it.Key())
		assert.Equal(t, []byte("key1"), it.Key())
	})

	// 测试空迭代器
	t.Run("Empty iterator", func(t *testing.T) {
		emptyIndex := NewShardedIndex(BTree, 16)
		it := emptyIndex.Iterator(false)
		defer it.Close()

		assert.False(t, it.Valid())
		assert.Nil(t, it.Key())
		assert.Nil(t, it.Value())

		// 测试空迭代器的操作
		it.Next()
		it.Rewind()
		it.Seek([]byte("any"))
		assert.False(t, it.Valid())
	})

	// 测试单键迭代器
	t.Run("Single Key iterator", func(t *testing.T) {
		singleIndex := NewShardedIndex(BTree, 16)
		singleIndex.Put([]byte("single"), createDataPos(1, 100, 200))

		it := singleIndex.Iterator(false)
		defer it.Close()

		assert.True(t, it.Valid())
		assert.Equal(t, []byte("single"), it.Key())
		it.Next()
		assert.False(t, it.Valid())
	})
}

// 测试辅助函数，创建测试用的数据位置信息
func createDataPos(id uint32, offset uint32, size uint32) *datafile.DataPos {
	return &datafile.DataPos{
		Fid:    id,
		Offset: offset,
		Size:   size,
	}
}
