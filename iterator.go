package xixi_kv

import (
	"bytes"
	"github.com/XiXi-2024/xixi-kv/index"
)

// Iterator 数据库层迭代器, 面向用户
type Iterator struct {
	indexIter *index.IndexIterator // 索引迭代器, 遍历 key
	db        *DB                  // DB 实例, 用于获取 value
	options   IteratorOptions      // 用户配置项
}

func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(opts.Reverse)
	return &Iterator{
		db:        db,
		indexIter: indexIter,
		options:   opts,
	}
}

// Rewind 迭代器重置回到起点
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}

// Seek 返回首个大于(小于)等于指定 key 的目标 key
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}

// Next 遍历下一个满足条件的元素
func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}

// Valid 判断是否遍历完成
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

// Key 返回当前位置的 key
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

// Value 返回当前位置 key 对应的实际 value
func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.indexIter.Value()
	return it.db.getValueByPosition(logRecordPos)
}

// Close 关闭迭代器 释放相关资源
func (it *Iterator) Close() {
	it.indexIter.Close()
}

// 跳转到下一个满足条件的元素
func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.Prefix)
	// 未指定前缀 直接返回
	if prefixLen == 0 {
		return
	}

	// 仅遍历 key 前缀满足条件的元素
	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		if prefixLen <= len(key) && bytes.Compare(it.options.Prefix, key[:prefixLen]) == 0 {
			break
		}
	}
}
