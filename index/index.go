package index

import (
	"bytes"
	"github.com/XiXi-2024/xixi-bitcask-kv/data"
	"github.com/google/btree"
)

// Indexer 抽象索引操作接口 允许多种索引实现
type Indexer interface {
	// Put 新增元素
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get 根据 key 获取元素
	Get(key []byte) *data.LogRecordPos

	// Delete 根据 key 删除元素
	Delete(key []byte) bool

	// Size 返回元素个数
	Size() int

	// Iterator 返回迭代器
	Iterator(reverse bool) Iterator

	// Close 关闭索引
	Close() error
}

type IndexType = int8

const (
	// Btree B树索引
	Btree IndexType = iota + 1
	// ART 自适应基数树索引
	ART
	// BPTree B+树索引
	BPTree
)

// NewIndexer 根据类型创建对应的索引实现
func NewIndexer(typ IndexType, dirPath string, sync bool) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		return NewART()
	case BPTree:
		return NewBPlusTree(dirPath, sync)
	default:
		panic("unsupported index type")
	}
}

// Item BTree节点实现
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

// Less 实现自定义比较器
func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

// Iterator 通用索引迭代器接口
type Iterator interface {
	// Rewind 迭代器重置回到起点
	Rewind()

	// Seek 返回首个大于(小于)等于指定 key 的目标 key
	Seek(key []byte)

	// Next 遍历下一个元素
	Next()

	// Valid 判断是否遍历完成
	Valid() bool

	// Key 返回当前位置的 key
	Key() []byte

	// Value 返回当前位置的 value
	Value() *data.LogRecordPos

	// Close 关闭迭代器 释放相关资源
	Close()
}
