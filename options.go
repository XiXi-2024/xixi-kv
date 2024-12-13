package xixi_bitcask_kv

// Options 用户配置项
type Options struct {
	DirPath      string      // 数据文件目录
	DataFileSize int64       // 数据文件存储阈值
	SyncWrites   bool        // 每次写数据是否立即持久化
	IndexType    IndexerType // 索引类型
}

type IndexerType = int8

const (
	// BTree 索引
	BTree IndexerType = iota + 1

	// ART 自适应基数树索引
	ART
)
