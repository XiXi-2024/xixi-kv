package xixi_bitcask_kv

import "os"

// Options 用户配置项
type Options struct {
	DirPath      string // 数据文件目录
	DataFileSize int64  // 单个数据文件最大容量, 单位字节
	// todo 扩展点：新增为 1. 立即持久化 2. 每隔 1 秒持久化 3. 未持久化数据达到阈值持久化
	SyncWrites   bool        // 每次写数据是否立即持久化标识
	BytesPerSync uint        // 触发持久化操作的字节写入阈值
	IndexType    IndexerType // 索引类型
	// todo 扩展点：新增为 1. 标准文件IO 2. MMap 3.缓冲IO ...
	MMapAtStartup      bool    // 是否启用 MMap 加速数据加载标识
	DataFileMergeRatio float32 // 执行 merge 的无效数据占比阈值
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
	// 单个批次最大日志记录数量
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
	DirPath:            os.TempDir(),      // 默认使用系统临时目录
	DataFileSize:       256 * 1024 * 1024, // 256MB
	SyncWrites:         false,             // 默认非立即持久化
	BytesPerSync:       0,                 // 默认值 0, 表示不开启功能
	IndexType:          BTree,             // 默认使用 B 树
	MMapAtStartup:      true,              // 默认启用
	DataFileMergeRatio: 0.5,               // 无效数据占一半时清理
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
