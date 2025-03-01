package xixi_kv

import (
	"github.com/XiXi-2024/xixi-kv/fio"
	"github.com/XiXi-2024/xixi-kv/index"
	"os"
)

type SyncStrategy byte

const (
	No SyncStrategy = iota // 由操作系统决定

	Always // 立即持久化

	Threshold // 新写入数据量达到阈值持久化
)

// Options 用户配置项
type Options struct {
	DirPath               string          // 数据目录
	DataFileSize          int64           // 数据文件最大容量, 单位字节
	SyncStrategy          SyncStrategy    // 持久化策略
	BytesPerSync          uint            // 新写入数据量阈值
	IndexType             index.IndexType // 索引类型
	FileIOType            fio.FileIOType  // 文件 IO 类型
	EnableBackgroundMerge bool            // 是否启用后台定时 merge
	DataFileMergeRatio    float32         // 执行 merge 的无效数据占比阈值
	ShardNum              int             // 索引分片数量
}

// IteratorOptions 索引迭代器配置项
type IteratorOptions struct {
	// key 过滤前缀, 默认为空
	Prefix []byte
	// 是否降序遍历, 默认为false
	Reverse bool
}

// BatchOptions 批处理操作配置项
type BatchOptions struct {
	Sync bool // 刷新时是否理解持久化
}

// DefaultOptions 默认Options, 供示例程序使用
var DefaultOptions = Options{
	DirPath:               os.TempDir(),
	DataFileSize:          512 * 1024 * 1024,
	SyncStrategy:          No,
	BytesPerSync:          1024 * 1024,
	EnableBackgroundMerge: false,
	IndexType:             index.HashMap,
	ShardNum:              16,
	FileIOType:            fio.MemoryMap,
	DataFileMergeRatio:    0.5,
}

// DefaultIteratorOptions 默认迭代器Options, 供测试使用
var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

// DefaultBatchOptions 默认事务 Options, 供测试使用
var DefaultBatchOptions = BatchOptions{
	Sync: false,
}
