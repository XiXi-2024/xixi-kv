package fio

const DataFilePerm = 0644

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
func NewIOManager(fileName string) (IOManager, error) {
	// 当前仅支持标准 FileIO 实现
	return NewFileIOManager(fileName)
}
