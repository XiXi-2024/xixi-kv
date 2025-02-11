package index

import (
	"bytes"
	"github.com/XiXi-2024/xixi-bitcask-kv/data"
	"github.com/google/btree"
)

// IndexType 索引实现类型枚举
type IndexType = int8

const (
	// BTree Btree B树索引
	BTree IndexType = iota + 1
	// ART 自适应基数树索引
	ART
	// BPTree B+树索引
	BPTree
	// SkipList 跳表索引
	SkipList
	// HashMap 哈希索引
	HashMap
)

// Indexer 抽象索引操作接口
// todo 扩展点：新增map索引
type Indexer interface {
	// Put 新增元素
	// 允许 key 为 nil
	// 重复添加会覆盖并返回旧值
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos

	// Get 获取元素
	Get(key []byte) *data.LogRecordPos

	// Delete 删除元素
	Delete(key []byte) (*data.LogRecordPos, bool)

	// Size 获取元素个数
	Size() int

	// Iterator 获取索引迭代器
	Iterator(reverse bool) Iterator

	// Close 关闭索引
	Close() error
}

// NewIndexer 根据类型创建对应的索引实现
// todo 可设置为 DB 方法？
func NewIndexer(typ IndexType, dirPath string, sync bool) Indexer {
	switch typ {
	case BTree:
		return NewBTree()
	case ART:
		return NewART()
	case BPTree:
		return NewBPlusTree(dirPath, sync)
	case SkipList:
		return NewSkipList()
	default:
		panic("unsupported index type")
	}
}

// Item 通用结点
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

// Less 结点比较器
func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

// Iterator 通用索引迭代器接口
type Iterator interface {
	// Rewind 迭代器重置
	Rewind()

	// Seek 游标移动到指定 key 的元素
	Seek(key []byte)

	// Next 游标移动到下一元素
	Next()

	// Valid 判断是否迭代完成
	Valid() bool

	// Key 获取当前游标指向元素的 key
	Key() []byte

	// Value 获取当前游标指向元素的 value
	Value() *data.LogRecordPos

	// Close 关闭迭代器
	Close()
}
