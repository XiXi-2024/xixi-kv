package index

import (
	"bytes"
	"github.com/XiXi-2024/xixi-bitcask-kv/data"
	"github.com/google/btree"
)

// Indexer 抽象索引操作接口 允许多种索引实现
type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) bool
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
