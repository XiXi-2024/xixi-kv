package index

import (
	"bytes"
	"github.com/XiXi-2024/xixi-kv/datafile"
	"github.com/google/btree"
	"sort"
	"sync"
)

// BTreeIndex B 树索引实现
// https://github.com/google/btree
type BTreeIndex struct {
	tree *btree.BTree
	// 底层实现非线程安全, 需要自行保证
	lock *sync.RWMutex
}

// NewBTree 创建新索引实例
func NewBTree() *BTreeIndex {
	// 返回默认实例
	return &BTreeIndex{
		tree: btree.New(33),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTreeIndex) Put(key []byte, pos *datafile.DataPos) *datafile.DataPos {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	oldItem := bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil
	}
	return oldItem.(*Item).pos
}

func (bt *BTreeIndex) Get(key []byte) *datafile.DataPos {
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

func (bt *BTreeIndex) Delete(key []byte) (*datafile.DataPos, bool) {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil, false
	}
	return oldItem.(*Item).pos, true
}

func (bt *BTreeIndex) Size() int {
	return bt.tree.Len()
}

func (bt *BTreeIndex) Close() error {
	return nil
}

func (bt *BTreeIndex) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBTreeIterator(bt.tree, reverse)
}

// BTreeIndex 索引迭代器
type btreeIterator struct {
	reverse  bool // 是否降序遍历
	curIndex int  // 当前遍历位置
	values   []*Item
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	// 暂时将所有项放入数组中进行操作, 可能导致占用内存急剧膨胀
	var idx int
	values := make([]*Item, tree.Len())

	// 定义遍历函数
	saveValues := func(it btree.Item) bool {
		// 处理元素, 按顺序追加到数组中
		values[idx] = it.(*Item)
		idx++
		// 返回 false 终止遍历
		return true
	}

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

func (bti *btreeIterator) Value() *datafile.DataPos {
	return bti.values[bti.curIndex].pos
}

func (bti *btreeIterator) Close() {
	bti.values = nil
}
