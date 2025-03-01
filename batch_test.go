package xixi_kv

import (
	"github.com/XiXi-2024/xixi-kv/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

// 测试完成后清理测试数据和资源
func destroyBatchDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			_ = db.Close()
		}
		for _, of := range db.olderFiles {
			if of != nil {
				_ = of.Close()
			}
		}
		err := os.RemoveAll(db.options.DirPath)
		if err != nil {
			panic(err)
		}
	}
}

// 初始化测试用的DB实例和Batch实例
func initBatchDB() (*DB, *Batch, error) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-batch")
	opts.DirPath = dir
	db, err := Open(opts)
	if err != nil {
		return nil, nil, err
	}
	batch := db.NewBatch(DefaultBatchOptions)
	return db, batch, nil
}

func TestBatch_Put_Get(t *testing.T) {
	db, batch, err := initBatchDB()
	defer destroyBatchDB(db)
	assert.Nil(t, err)

	// 1. 正常 Put 验证数据一致性
	key1, value1 := utils.GetTestKey(1), utils.RandomValue(24)
	err = batch.Put(key1, value1)
	assert.Nil(t, err)
	// 提交前通过 batch.Get 验证
	val, err := batch.Get(key1)
	assert.Nil(t, err)
	assert.Equal(t, value1, val)
	// 提交后通过 db.Get 验证
	err = batch.Commit()
	assert.Nil(t, err)
	val, err = db.Get(key1)
	assert.Nil(t, err)
	assert.Equal(t, value1, val)

	// 重新创建batch进行后续测试
	batch = db.NewBatch(DefaultBatchOptions)

	// 2. key为空
	err = batch.Put(nil, value1)
	assert.Equal(t, ErrKeyIsEmpty, err)

	// 3. value为空
	key3 := utils.GetTestKey(3)
	err = batch.Put(key3, nil)
	assert.Nil(t, err)
	val, err = batch.Get(key3)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(val))

	// 4. 重复Put同一个key，验证值被覆盖
	key4, value4 := utils.GetTestKey(4), utils.RandomValue(24)
	err = batch.Put(key4, value4)
	assert.Nil(t, err)
	value4New := utils.RandomValue(24)
	err = batch.Put(key4, value4New)
	assert.Nil(t, err)
	val, err = batch.Get(key4)
	assert.Nil(t, err)
	assert.Equal(t, value4New, val)

	// 5. 大数据量写入测试
	values := make([][]byte, 1000)
	for i := 0; i < 1000; i++ {
		key := utils.GetTestKey(i)
		values[i] = utils.RandomValue(128)
		err = batch.Put(key, values[i])
		assert.Nil(t, err)
	}
	for i := 0; i < 1000; i++ {
		key := utils.GetTestKey(i)
		value, err := batch.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, values[i], value)
	}

	batch.Commit()
}

func TestBatch_Delete(t *testing.T) {
	db, batch, err := initBatchDB()
	defer destroyBatchDB(db)
	assert.Nil(t, err)

	// 1. 删除不存在的key
	err = batch.Delete([]byte("not-exist"))
	assert.Nil(t, err)

	// 2. key为空
	err = batch.Delete(nil)
	assert.Equal(t, ErrKeyIsEmpty, err)

	// 3. 删除已提交的数据
	key3, value3 := utils.GetTestKey(3), utils.RandomValue(24)
	_ = batch.Put(key3, value3)
	batch.Commit()
	err = db.Delete(key3)
	assert.Nil(t, err)
	value, err := db.Get(key3)
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, value)

	// 4. 删除批处理中的数据
	batch = db.NewBatch(DefaultBatchOptions)
	key4, value4 := utils.GetTestKey(4), utils.RandomValue(24)
	_ = batch.Put(key4, value4)
	err = batch.Delete(key4)
	assert.Nil(t, err)
	value, err = batch.Get(key4)
	assert.Nil(t, value)
	assert.Equal(t, ErrKeyNotFound, err)
	batch.Commit()
}
