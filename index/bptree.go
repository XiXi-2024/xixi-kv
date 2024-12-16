package index

import (
	"github.com/XiXi-2024/xixi-bitcask-kv/data"
	"go.etcd.io/bbolt"
	"path/filepath"
)

// 索引文件名称
const bptreeIndexFileName = "bptree-index"

// Bucket名称
var indexBucketName = []byte("bitcask-index")

// BPlusTree 可持久化 B+ 树索引实现
// 底层库支持并发访问 无需加锁
// https://github.com/etcd-io/bbolt
type BPlusTree struct {
	tree *bbolt.DB // 实际为单独的存储引擎
}

func NewBPlusTree(dirPath string, syncWrites bool) *BPlusTree {
	opts := bbolt.DefaultOptions
	// 可自定义配置项
	opts.NoSync = !syncWrites // 是否不进行立即持久化
	// 打开索引文件 后续将索引持久化到磁盘中
	bptree, err := bbolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0644, opts)
	if err != nil {
		panic("failed to open bptree")
	}

	// Update 方法内自动开启事务
	if err := bptree.Update(func(tx *bbolt.Tx) error {
		// 创建对应的 bucket 实例 用于后续的数据操作
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("failed to create bucket in bptree")
	}

	return &BPlusTree{tree: bptree}
}

func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) bool {
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		// 取出 Bucket 实例进行操作
		bucket := tx.Bucket(indexBucketName)
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	}); err != nil {
		panic("failed to put value in bptree")
	}
	return true
}

func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	// View 方法自动开启只读事务, 方法内仅允许数据读取操作
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			pos = data.DecodeLogRecordPos(value)
		}
		return nil
	}); err != nil {
		panic("failed to get value in bptree")
	}
	return pos
}

func (bpt *BPlusTree) Delete(key []byte) bool {
	var ok bool
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		if value := bucket.Get(key); len(value) != 0 {
			ok = true
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to delete value in bptree")
	}
	return ok
}

func (bpt *BPlusTree) Size() int {
	var size int
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to get size in bptree")
	}
	return size
}

func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}

func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBptreeIterator(bpt.tree, reverse)
}

// B+树迭代器
type bptreeIterator struct {
	tx        *bbolt.Tx     // 事务客户端
	cursor    *bbolt.Cursor // 内置迭代器游标
	reverse   bool          // 是否降序遍历
	currKey   []byte        // 当前位置元素 key
	currValue []byte        // 当前位置元素 value
}

func newBptreeIterator(tree *bbolt.DB, reverse bool) *bptreeIterator {
	// 手动开启事务 迭代结束时提交
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin a transaction")
	}
	bpi := &bptreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(), // 获取迭代器游标
		reverse: reverse,
	}
	bpi.Rewind() // 重置游标
	return bpi
}

func (bpi *bptreeIterator) Rewind() {
	// 根据配置项将游标指向开头或末尾
	if bpi.reverse {
		bpi.currKey, bpi.currValue = bpi.cursor.Last()
	} else {
		bpi.currKey, bpi.currValue = bpi.cursor.First()
	}
}

func (bpi *bptreeIterator) Seek(key []byte) {
	// 调用内置方法
	bpi.currKey, bpi.currValue = bpi.cursor.Seek(key)
}

func (bpi *bptreeIterator) Next() {
	// 根据配置项移动游标
	if bpi.reverse {
		bpi.currKey, bpi.currValue = bpi.cursor.Prev()
	} else {
		bpi.currKey, bpi.currValue = bpi.cursor.Next()
	}
}

func (bpi *bptreeIterator) Valid() bool {
	return len(bpi.currKey) != 0
}

func (bpi *bptreeIterator) Key() []byte {
	return bpi.currKey
}

func (bpi *bptreeIterator) Value() *data.LogRecordPos {
	// 解码返回
	return data.DecodeLogRecordPos(bpi.currValue)
}

func (bpi *bptreeIterator) Close() {
	// 事务提交
	_ = bpi.tx.Rollback()
}
