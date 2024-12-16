package fio

const DataFilePerm = 0644

type FileIOType = byte

const (
	// StandardFIO 标准文件 IO
	StandardFIO FileIOType = iota
	// MemoryMap 内存文件映射 IO
	MemoryMap
)

// IOManager IO 管理抽象接口 允许多种 IO 类型
type IOManager interface {
	// Read 文件读取 从指定位置开始
	Read([]byte, int64) (int, error)
	// Write 文件写入
	Write([]byte) (int, error)
	// Sync 持久化数据
	Sync() error
	// Close 关闭文件
	Close() error
	// Size 获取文件大小
	Size() (int64, error)
}

// NewIOManager 初始化指定类型的 IOManager 实例
func NewIOManager(fileName string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardFIO:
		return NewFileIOManager(fileName)
	case MemoryMap:
		return NewMMapIOManager(fileName)
	default:
		panic("unsupported io type")
	}
}
