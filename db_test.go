package xixi_kv

import (
	"github.com/XiXi-2024/xixi-kv/datafile"
	"github.com/XiXi-2024/xixi-kv/utils"
	"github.com/gofrs/flock"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestOpen(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
}

func TestDB_Put(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-put")
	opts.DirPath = dir
	opts.DataFileSize = 128 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1. Put
	key1, value1 := utils.GetTestKey(1), utils.RandomValue(24)
	err = db.Put(key1, value1)
	assert.Nil(t, err)
	val1, err := db.Get(key1)
	assert.Nil(t, err)
	assert.NotNil(t, val1)
	assert.Equal(t, val1, value1)

	// 2. Put重复key
	key2, value2 := utils.GetTestKey(1), utils.RandomValue(24)
	assert.Equal(t, key1, key2)
	assert.NotEqual(t, value1, value2)
	err = db.Put(key2, value2)
	assert.Nil(t, err)
	val2, err := db.Get(key2)
	assert.Nil(t, err)
	assert.NotNil(t, val2)
	assert.Equal(t, val2, value2)

	// 3. key 为空
	value3 := utils.RandomValue(24)
	err = db.Put(nil, value3)
	assert.Equal(t, ErrKeyIsEmpty, err)

	// 4. value 为空
	key4 := utils.GetTestKey(22)
	err = db.Put(key4, nil)
	assert.Nil(t, err)
	val3, err := db.Get(key4)
	assert.Equal(t, len(val3), 0)
	assert.Nil(t, err)

	// 5. 写入数据超过单个数据文件的最大容量
	n := 1000
	values := make([]string, 0, n)
	for i := 0; i < n; i++ {
		value := utils.RandomValue(128)
		err := db.Put(utils.GetTestKey(i), value)
		assert.Nil(t, err)
		values = append(values, string(value))
	}
	assert.Equal(t, 2, len(db.olderFiles)+1)
	for i := 0; i < n; i++ {
		value, err := db.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.NotNil(t, value)
		assert.Equal(t, values[i], string(value))
	}

	// 6. 重启数据库后进行 Put
	err = db.Close()
	assert.Nil(t, err)
	db, err = Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	key6, value6 := utils.GetTestKey(1001), utils.RandomValue(24)
	err = db.Put(key6, value6)
	assert.Nil(t, err)
	val6, err := db.Get(key6)
	assert.Nil(t, err)
	assert.NotNil(t, val6)
	assert.Equal(t, val6, value6)
}

func TestDB_Get(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-get")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1. key 不存在
	val1, err := db.Get([]byte("some key unknown"))
	assert.Nil(t, val1)
	assert.Equal(t, ErrKeyNotFound, err)

	// 2. key 被删除
	key2, value2 := utils.GetTestKey(33), utils.RandomValue(24)
	err = db.Put(key2, value2)
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(33))
	assert.Nil(t, err)
	val2, err := db.Get(utils.GetTestKey(33))
	assert.Equal(t, 0, len(val2))
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestDB_Delete(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-delete")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1. 删除 key 不存在
	err = db.Delete([]byte("unknown key"))
	assert.Nil(t, err)

	// 2. key 为空
	err = db.Delete(nil)
	assert.Equal(t, ErrKeyIsEmpty, err)

	// 3. 删除后再次 put
	key3, value3 := utils.GetTestKey(22), utils.RandomValue(128)
	err = db.Put(key3, value3)
	assert.Nil(t, err)
	err = db.Delete(key3)
	assert.Nil(t, err)
	err = db.Put(key3, value3)
	assert.Nil(t, err)
	val3, err := db.Get(key3)
	assert.NotNil(t, val3)
	assert.Nil(t, err)
	assert.Equal(t, val3, value3)

	// 5.重启
	key5, value5 := utils.GetTestKey(55), utils.RandomValue(128)
	err = db.Put(key5, value5)
	assert.Nil(t, err)
	val5, err := db.Get(key5)
	assert.Nil(t, err)
	assert.NotNil(t, val5)
	assert.Equal(t, val5, value5)
	err = db.Delete(key5)
	assert.Nil(t, err)

	err = db.Close()
	assert.Nil(t, err)
	db, err = Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 原先存在
	val5, err = db.Get(key3)
	assert.Nil(t, err)
	assert.NotNil(t, val5)
	assert.Equal(t, val5, value3)

	// 原先已删除
	val5, err = db.Get(key5)
	assert.Equal(t, 0, len(val5))
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestDB_ListKeys(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-list-keys")
	opts.DirPath = dir
	opts.DataFileSize = 128 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1. 数据库为空
	keys1 := db.ListKeys()
	assert.Equal(t, 0, len(keys1))

	// 2. 仅一条数据
	key2, value2 := utils.GetTestKey(0), utils.RandomValue(20)
	err = db.Put(key2, value2)
	assert.Nil(t, err)
	keys2 := db.ListKeys()
	assert.Equal(t, 1, len(keys2))
	assert.Equal(t, keys2[0], key2)

	// 3. 多条数据
	n := 10
	for i := 0; i < n; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(20))
		assert.Nil(t, err)
	}
	keys3 := db.ListKeys()
	assert.Equal(t, len(keys3), n)
	for _, k := range keys3 {
		assert.NotNil(t, k)
	}

	// 4. 删除数据
	err = db.Delete(utils.GetTestKey(0))
	assert.Nil(t, err)
	keys := db.ListKeys()
	assert.Equal(t, n-1, len(keys))
	for _, k := range keys {
		assert.NotEqual(t, utils.GetTestKey(0), k, "删除的 key 仍然存在")
	}

	// 5. 重启
	err = db.Close()
	assert.Nil(t, err)

	db, err = Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	keys = db.ListKeys()
	assert.Equal(t, n-1, len(keys))
	for _, k := range keys {
		assert.NotEqual(t, utils.GetTestKey(0), k, "重启后删除的 key 仍然存在")
	}
}

func TestDB_Fold(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-fold")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	n := 100
	for i := 0; i < n; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(20))
		assert.Nil(t, err)
	}

	cnt := 0
	err = db.Fold(func(key []byte, value []byte) bool {
		assert.NotNil(t, key)
		assert.NotNil(t, value)
		cnt++
		return true
	})
	assert.Nil(t, err)
	assert.Equal(t, n, cnt)
}

func TestDB_Close(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-close")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	err = db.Put(utils.GetTestKey(0), utils.RandomValue(20))
	assert.Nil(t, err)

	// 文件锁释放
	fileFlockName := filepath.Join(dir, datafile.FileLockSuffix)
	_, err = os.Stat(fileFlockName)
	assert.Nil(t, err)
	fileLock := flock.New(fileFlockName)
	ok, err := fileLock.TryLock()
	assert.Nil(t, err)
	assert.False(t, ok)

	err = db.Close()
	assert.Nil(t, err)

	ok, err = fileLock.TryLock()
	assert.Nil(t, err)
	assert.True(t, ok)
}

func TestDB_Stat(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-stat")
	opts.DirPath = dir
	opts.DataFileSize = 256 * 1024
	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer destroyDB(db)

	for i := 100; i < 1000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	for i := 100; i < 200; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}
	for i := 300; i < 500; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	stat := db.Stat()
	assert.NotNil(t, stat)
	assert.Equal(t, 800, stat.KeyNum)
	assert.Equal(t, 1, stat.DataFileNum)
}

// todo bug: mmap实现
func TestDB_Backup(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-backup")
	opts.DirPath = dir
	opts.DataFileSize = 512 * 1024
	db1, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db1)
	defer destroyDB(db1)

	value := utils.RandomValue(128)
	for i := 1; i < 1000; i++ {
		err := db1.Put(utils.GetTestKey(i), value)
		assert.Nil(t, err)
	}

	backupDir, _ := os.MkdirTemp("", "bitcask-go-backup-test")
	err = db1.Backup(backupDir)
	assert.Nil(t, err)
	opts.DirPath = backupDir
	db2, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	for i := 1; i < 1000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.Equal(t, value, val)
	}
}

// 测试完成之后销毁 DB 数据目录
func destroyDB(db *DB) {
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
