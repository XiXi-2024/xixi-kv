package index

import (
	"bytes"
	"github.com/XiXi-2024/xixi-bitcask-kv/data"
	"github.com/google/btree"
	"sort"
	"sync"
)

// BTree 索引实现
// https://github.com/google/btree
type BTree struct {
	tree *btree.BTree
	// 底层实现非线程安全需要自行保证
	lock *sync.RWMutex
}

func NewBTree() *BTree {
	// 返回默认实例
	return &BTree{
		tree: btree.New(33),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	oldItem := bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil
	}
	return oldItem.(*Item).pos
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil, false
	}
	return oldItem.(*Item).pos, true
}

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

func (bt *BTree) Close() error {
	return nil
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBTreeIterator(bt.tree, reverse)
}

// BTree 索引迭代器
type btreeIterator struct {
	curIndex int     // 当前遍历的下标位置
	reverse  bool    // 是否降序遍历
	values   []*Item // 类型复用, 存放 key + 位置索引信息
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	// todo 后期解决
	// 内置迭代方法无法满足个性化的迭代需求, 取出元素存入数组中再进行迭代
	// 可能导致占用内存急剧膨胀
	var idx int
	values := make([]*Item, tree.Len())

	// 定义迭代器函数
	saveValues := func(it btree.Item) bool {
		// 处理元素, 按顺序追加到数组中
		values[idx] = it.(*Item)
		idx++
		// 返回 false 终止遍历
		return true
	}

	// 遍历树中元素, 传入函数对遍历的每个元素进行处理
	if reverse {
		// 升序遍历
		tree.Descend(saveValues)
	} else {
		// 降序遍历
		tree.Ascend(saveValues)
	}

	return &btreeIterator{
		curIndex: 0,
		reverse:  reverse,
		values:   values,
	}
}

func (bti *btreeIterator) Rewind() {
	bti.curIndex = 0
}

func (bti *btreeIterator) Seek(key []byte) {
	// 初始化时底层数组已有序 直接二分查找即可
	if bti.reverse {
		bti.curIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else {
		bti.curIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}
}

func (bti *btreeIterator) Next() {
	bti.curIndex++
}

func (bti *btreeIterator) Valid() bool {
	return bti.curIndex < len(bti.values)
}

func (bti *btreeIterator) Key() []byte {
	return bti.values[bti.curIndex].key
}

func (bti *btreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.curIndex].pos
}

func (bti *btreeIterator) Close() {
	bti.values = nil
}
