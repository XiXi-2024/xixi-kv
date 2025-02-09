package fio

import "errors"

const DataFilePerm = 0644

var ErrTypeUnsupported = errors.New("unsupported io type")

// todo 扩展点：新增缓冲IO实现
type FileIOType = byte

const (
	// StandardFIO 标准文件IO
	StandardFIO FileIOType = iota
	// MemoryMap  内存映射文件IO
	MemoryMap
)

type ReadWriter interface {
	Read([]byte, int64) (int, error)

	Write([]byte) (int, error)

	Sync() error

	Close() error

	Size() (int64, error)
}

// NewReadWriter 根据配置创建具体的文件 IO 实现
func NewReadWriter(fileName string, ioType FileIOType) (ReadWriter, error) {
	switch ioType {
	case StandardFIO:
		return NewFileIO(fileName)
	case MemoryMap:
		return NewMMap(fileName)
	default:
		return nil, ErrTypeUnsupported
	}
}
