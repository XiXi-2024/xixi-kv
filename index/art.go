package index

import (
	"bytes"
	"github.com/XiXi-2024/xixi-bitcask-kv/data"
	goart "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
)

// AdaptiveRadixTree 自适应基数树索引实现
// https://github.com/plar/go-adaptive-radix-tree
type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

// NewART 创建新索引实例
func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	art.lock.Lock()
	oldValue, _ := art.tree.Insert(key, pos)
	art.lock.Unlock()
	if oldValue == nil {
		return nil
	}
	return oldValue.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	value, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	art.lock.Lock()
	oldValue, deleted := art.tree.Delete(key)
	art.lock.Unlock()
	if oldValue == nil {
		return nil, false
	}
	return oldValue.(*data.LogRecordPos), deleted
}

func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	size := art.tree.Size()
	art.lock.RUnlock()
	return size
}

func (art *AdaptiveRadixTree) Close() error {
	// todo 优化点, 释放内存
	return nil
}

func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return newARTIterator(art.tree, reverse)
}

// Art 索引迭代器
type artIterator struct {
	reverse bool // 是否降序遍历 todo 扩展点：转换为配置项成员
	// todo 优化点：采取效率更高的迭代方式
	curIndex int     // 当前遍历的下标位置
	values   []*Item // 类型复用, 存放 key + 位置索引信息
}

func newARTIterator(tree goart.Tree, reverse bool) *artIterator {
	var idx int
	if reverse {
		idx = tree.Size() - 1
	}
	values := make([]*Item, tree.Size())
	// 定义遍历函数, 处理遍历的每个元素
	// 暂时将所有项放入数组中进行操作
	saveValues := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		// 根据配置项进行升序或降序遍历
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}

	// 遍历元素 使用函数参数逐个处理
	tree.ForEach(saveValues)

	return &artIterator{
		curIndex: 0,
		reverse:  reverse,
		values:   values,
	}
}

func (ai *artIterator) Rewind() {
	ai.curIndex = 0
}

func (ai *artIterator) Seek(key []byte) {
	if ai.reverse {
		ai.curIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) <= 0
		})
	} else {
		ai.curIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) >= 0
		})
	}
}

func (ai *artIterator) Next() {
	ai.curIndex += 1
}

func (ai *artIterator) Valid() bool {
	return ai.curIndex < len(ai.values)
}

func (ai *artIterator) Key() []byte {
	return ai.values[ai.curIndex].key
}

func (ai *artIterator) Value() *data.LogRecordPos {
	return ai.values[ai.curIndex].pos
}

func (ai *artIterator) Close() {
	ai.values = nil
}
