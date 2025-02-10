package xixi_bitcask_kv

import (
	"github.com/XiXi-2024/xixi-bitcask-kv/fio"
	"github.com/XiXi-2024/xixi-bitcask-kv/index"
	"os"
)

type SyncStrategy byte

const (
	No SyncStrategy = iota

	Always // 立即持久化

	Everysec // 每隔 1 秒持久化

	Threshold // 新写入数据量达到阈值持久化 // 由操作系统决定
)

// Options 用户配置项
type Options struct {
	DirPath      string // 数据目录
	DataFileSize int64  // 数据文件最大容量, 单位字节
	// todo 优化点：实现各个持久化策略
	SyncStrategy       SyncStrategy    // 持久化策略
	SyncWrites         bool            // 每次写数据是否立即持久化标识
	BytesPerSync       uint            // 新写入数据量阈值, 未启动策略时为 0
	IndexType          index.IndexType // 索引类型
	FileIOType         fio.FileIOType  // 文件 IO 类型
	DataFileMergeRatio float32         // 执行 merge 的无效数据占比阈值
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

// DefaultOptions 默认Options, 供示例程序使用
var DefaultOptions = Options{
	DirPath:            os.TempDir(),
	DataFileSize:       256 * 1024,
	SyncWrites:         false,
	BytesPerSync:       0,
	IndexType:          index.BTree,
	FileIOType:         fio.StandardFIO,
	DataFileMergeRatio: 0.5,
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
