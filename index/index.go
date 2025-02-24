package index

import (
	"github.com/XiXi-2024/xixi-kv/datafile"
)

// IndexType 索引实现类型枚举
type IndexType = int8

const (
	// BTree Btree B树索引
	BTree IndexType = iota + 1
	// SkipList 跳表索引
	SkipList
	// HashMap 哈希索引
	HashMap
)

// 抽象索引接口
type index interface {
	// 新增元素
	// 允许 key 为 nil, 重复添加会覆盖并返回旧值
	put(key []byte, pos *datafile.DataPos) *datafile.DataPos

	// 获取元素
	get(key []byte) *datafile.DataPos

	// 删除元素
	// 如果操作成功, 返回 true
	delete(key []byte) (*datafile.DataPos, bool)

	// 获取元素个数
	size() int

	// 获取底层索引迭代器
	// 如果索引已关闭, 返回空的迭代器
	// 不保证关闭迭代器的线程安全, 推荐单线程使用
	iterator(reverse bool) iterator

	// 关闭索引
	close() error
}

// 根据类型创建底层索引实现
func newIndexer(typ IndexType) index {
	switch typ {
	case BTree:
		return newBTree()
	case SkipList:
		return newSkipList()
	case HashMap:
		return newMap()
	default:
		panic("unsupported index type")
	}
}

// 通用索引迭代器接口
type iterator interface {
	// 迭代器重置
	rewind()

	// 游标移动到首个大于等于 key 或小于等于 key 的的元素
	seek(key []byte)

	// 游标移动到下一元素
	next()

	// 判断迭代器是否可用
	valid() bool

	// 获取当前游标指向元素的 key
	key() []byte

	// 获取当前游标指向元素的 value
	value() *datafile.DataPos

	// 关闭迭代器
	close()
}
