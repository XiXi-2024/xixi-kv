package xixi_bitcask_kv

import "os"

// Options 用户配置项
type Options struct {
	DirPath      string      // 数据文件目录
	DataFileSize int64       // 数据文件存储阈值
	SyncWrites   bool        // 每次写数据是否立即持久化
	BytesPerSync uint        // 触发持久化操作的字节写入阈值
	IndexType    IndexerType // 索引类型
}

// IteratorOptions 索引迭代器配置项
type IteratorOptions struct {
	// key 过滤前缀, 默认为空
	Prefix []byte
	// 是否降序遍历, 默认为false
	Reverse bool
}

// WriteBatchOptions 批量写入配置项
type WriteBatchOptions struct {
	// 单个批次最大数据量
	MaxBatchNum uint

	// 提交事务时是否立即持久化
	SyncWrites bool
}

type IndexerType = int8

const (
	// BTree 索引
	BTree IndexerType = iota + 1
	// ART 自适应基数树索引
	ART
	// BPlusTree B+树索引
	BPlusTree
)

// DefaultOptions 默认Options, 供示例程序使用
var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, // 256MB
	SyncWrites:   false,
	BytesPerSync: 0, // 默认值 0, 表示不开启功能
	IndexType:    BTree,
}

// DefaultIteratorOptions 默认迭代器Options, 供测试使用
var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

// DefaultWriteBatchOptions 默认事务 Options, 供测试使用
var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
