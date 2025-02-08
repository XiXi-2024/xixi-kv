package xixi_bitcask_kv

import (
	"github.com/XiXi-2024/xixi-bitcask-kv/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
	"testing"
)

// 没有任何数据的情况下进行 merge
func TestDB_Merge(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask-go-merge-1")
	db, err := newTestMergeDB(dir)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Merge()
	assert.Nil(t, err)
}

// 全部都是有效的数据
func TestDB_Merge2(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask-go-merge-2")
	db, err := newTestMergeDB(dir)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer destroyDB(db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	// 重启校验
	err = db.Close()
	assert.Nil(t, err)

	db, err = newTestMergeDB(dir)
	assert.Nil(t, err)
	keys := db.ListKeys()
	assert.Equal(t, 50000, len(keys))

	for i := 0; i < 50000; i++ {
		val, err := db.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.NotNil(t, val)
	}
}

// 存在失效的、重复 Put 的数据
func TestDB_Merge3(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask-go-merge-3")
	db, err := newTestMergeDB(dir)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}
	for i := 0; i < 10000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}
	for i := 40000; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), []byte("new value in merge"))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	err = db.Close()
	assert.Nil(t, err)

	db2, err := newTestMergeDB(dir)
	defer func() {
		_ = db2.Close()
	}()
	assert.Nil(t, err)
	keys := db2.ListKeys()
	assert.Equal(t, 40000, len(keys))

	for i := 0; i < 10000; i++ {
		_, err := db2.Get(utils.GetTestKey(i))
		assert.Equal(t, ErrKeyNotFound, err)
	}
	for i := 40000; i < 50000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.Equal(t, []byte("new value in merge"), val)
	}
}

// 全部为无效数据
func TestDB_Merge4(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask-go-merge-4")
	db, err := newTestMergeDB(dir)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}
	for i := 0; i < 50000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	// 重启校验
	err = db.Close()
	assert.Nil(t, err)

	db2, err := newTestMergeDB(dir)
	defer func() {
		_ = db2.Close()
	}()
	assert.Nil(t, err)
	keys := db2.ListKeys()
	assert.Equal(t, 0, len(keys))
}

// Merge 的过程中有新的数据写入或删除
func TestDB_Merge5(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask-go-merge-2")
	db, err := newTestMergeDB(dir)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50000; i++ {
			err := db.Delete(utils.GetTestKey(i))
			assert.Nil(t, err)
		}
		for i := 60000; i < 70000; i++ {
			err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
			assert.Nil(t, err)
		}
	}()
	err = db.Merge()
	assert.Nil(t, err)
	wg.Wait()

	//重启校验
	err = db.Close()
	assert.Nil(t, err)

	db2, err := newTestMergeDB(dir)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	keys := db2.ListKeys()
	assert.Equal(t, 10000, len(keys))

	for i := 60000; i < 70000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.NotNil(t, val)
	}
	_ = db2.Close()
}

func newTestMergeDB(path string) (*DB, error) {
	opts := DefaultOptions
	opts.DataFileSize = 32 * 1024 * 1024
	opts.DataFileMergeRatio = 0
	opts.DirPath = path
	return Open(opts)
}
