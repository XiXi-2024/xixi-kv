package index

import (
	"bytes"
	"github.com/XiXi-2024/xixi-kv/datafile"
	"github.com/google/btree"
	"sort"
)

// B 树索引实现
// https://github.com/google/btree
type btreeIndex struct {
	tree *btree.BTree
}

type item struct {
	key []byte
	pos *datafile.DataPos
}

func (ai *item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*item).key) == -1
}

func newBTree() *btreeIndex {
	// 返回默认实例
	return &btreeIndex{
		tree: btree.New(33),
	}
}

func (bt *btreeIndex) put(key []byte, pos *datafile.DataPos) *datafile.DataPos {
	if bt.tree == nil {
		return nil
	}
	it := &item{key: key, pos: pos}
	oldItem := bt.tree.ReplaceOrInsert(it)
	if oldItem == nil {
		return nil
	}
	return oldItem.(*item).pos
}

func (bt *btreeIndex) get(key []byte) *datafile.DataPos {
	if bt.tree == nil {
		return nil
	}
	it := &item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*item).pos
}

func (bt *btreeIndex) delete(key []byte) *datafile.DataPos {
	if bt.tree == nil {
		return nil
	}
	it := &item{key: key}
	oldItem := bt.tree.Delete(it)
	if oldItem == nil {
		return nil
	}
	return oldItem.(*item).pos
}

func (bt *btreeIndex) size() int {
	if bt.tree == nil {
		return 0
	}
	return bt.tree.Len()
}

func (bt *btreeIndex) close() error {
	bt.tree = nil
	return nil
}

func (bt *btreeIndex) iterator(reverse bool) iterator {
	return newBTreeIterator(bt.tree, reverse)
}

// btreeIndex 索引迭代器
type btreeIterator struct {
	reverse  bool // 是否降序遍历
	curIndex int  // 当前遍历位置
	values   []*item
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	if tree == nil {
		return &btreeIterator{
			values: make([]*item, 0),
		}
	}

	// 暂时将所有项放入数组中进行操作, 可能导致占用内存急剧膨胀
	var idx int
	values := make([]*item, tree.Len())

	// 定义遍历函数
	saveValues := func(it btree.Item) bool {
		// 处理元素, 按顺序追加到数组中
		values[idx] = it.(*item)
		idx++
		// 返回 false 终止遍历
		return true
	}

	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}

	return &btreeIterator{
		curIndex: 0,
		values:   values,
		reverse:  reverse,
	}
}

func (bti *btreeIterator) rewind() {
	bti.curIndex = 0
}

func (bti *btreeIterator) seek(key []byte) {
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

func (bti *btreeIterator) next() {
	bti.curIndex++
}

func (bti *btreeIterator) valid() bool {
	return bti.curIndex < len(bti.values)
}

func (bti *btreeIterator) key() []byte {
	return bti.values[bti.curIndex].key
}

func (bti *btreeIterator) value() *datafile.DataPos {
	return bti.values[bti.curIndex].pos
}

func (bti *btreeIterator) close() {
	bti.values = nil
}
