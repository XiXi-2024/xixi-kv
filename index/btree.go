package index

import (
	"bytes"
	"github.com/XiXi-2024/xixi-kv/datafile"
	"github.com/google/btree"
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
	if bt.tree == nil {
		return nil
	}
	return newBTreeIterator(bt.tree, reverse)
}

// btreeIndex 索引迭代器
type btreeIterator struct {
	tree       *btree.BTree
	reverse    bool
	current    *item
	isIterable bool
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var current *item
	var valid bool
	if tree.Len() > 0 {
		if reverse {
			current = tree.Max().(*item)
		} else {
			current = tree.Min().(*item)
		}
		valid = true
	}
	return &btreeIterator{
		tree:       tree.Clone(),
		reverse:    reverse,
		current:    current,
		isIterable: valid,
	}
}

func (it *btreeIterator) rewind() {
	if it.tree == nil || it.tree.Len() == 0 {
		return
	}
	if it.reverse {
		it.current = it.tree.Max().(*item)
	} else {
		it.current = it.tree.Min().(*item)
	}
	it.isIterable = true
}

func (it *btreeIterator) seek(key []byte) {
	if it.tree == nil || !it.isIterable {
		return
	}
	seekItem := &item{key: key}
	it.isIterable = false
	if it.reverse {
		it.tree.DescendLessOrEqual(seekItem, func(i btree.Item) bool {
			it.current = i.(*item)
			it.isIterable = true
			return false
		})
	} else {
		it.tree.AscendGreaterOrEqual(seekItem, func(i btree.Item) bool {
			it.current = i.(*item)
			it.isIterable = true
			return false
		})
	}
}

func (it *btreeIterator) next() {
	if it.tree == nil || !it.isIterable {
		return
	}
	it.isIterable = false
	if it.reverse {
		it.tree.DescendLessOrEqual(it.current, func(i btree.Item) bool {
			if !i.(*item).Less(it.current) {
				return true
			}
			it.current = i.(*item)
			it.isIterable = true
			return false
		})
	} else {
		it.tree.AscendGreaterOrEqual(it.current, func(i btree.Item) bool {
			if !it.current.Less(i.(*item)) {
				return true
			}
			it.current = i.(*item)
			it.isIterable = true
			return false
		})
	}
	if !it.isIterable {
		it.current = nil
	}
}

func (it *btreeIterator) valid() bool {
	return it.isIterable
}

func (it *btreeIterator) key() []byte {
	if !it.isIterable {
		return nil
	}
	return it.current.key
}

func (it *btreeIterator) value() *datafile.DataPos {
	if !it.isIterable {
		return nil
	}
	return it.current.pos
}

func (it *btreeIterator) close() {
	it.tree.Clear(true)
	it.tree = nil
	it.current = nil
	it.isIterable = false
}
