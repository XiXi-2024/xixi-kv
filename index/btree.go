package index

import (
	"github.com/XiXi-2024/xixi-bitcask-kv/data"
	"github.com/google/btree"
	"sync"
)

// BTree 索引实现 对 google 的 btree 库的封装
// https://github.com/google/btree
type BTree struct {
	tree *btree.BTree
	// 底层实现非线程安全 对写操作需要加锁
	lock *sync.RWMutex
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(33),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return false
	}
	return true
}
