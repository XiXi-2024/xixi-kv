package fio

import (
	"fmt"
	"github.com/edsrzf/mmap-go"
	"io"
	"os"
	"unsafe"
)

// 32位系统上映射区域至多为 2G, 可能发生溢出

const (
	blockSize = 512 * 1024 * 1024
)

type MMap struct {
	file        *os.File
	activeMap   mmap.MMap // 当前活动映射区域
	endOff      int64     // 当前映射区域的右边界
	virtualSize int64     // 虚拟文件大小
}

func NewMMap(fileName string) (*MMap, error) {
	fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, DataFilePerm)
	if err != nil {
		return nil, err
	}

	stat, err := fd.Stat()
	if err != nil {
		return nil, err
	}

	m := &MMap{
		file:        fd,
		virtualSize: stat.Size(),
	}

	err = m.remap(m.virtualSize, blockSize)
	if err != nil {
		_ = fd.Close()
		return nil, err
	}

	return m, nil
}

func (m *MMap) Read(b []byte, offset int64) (int, error) {
	// 检查边界
	if offset >= m.virtualSize {
		return 0, io.EOF
	}

	if err := m.remap(offset, len(b)); err != nil {
		return 0, err
	}

	// 计算实际可读范围
	readEnd := offset + int64(len(b))
	if readEnd > m.virtualSize {
		readEnd = m.virtualSize
	}

	// 执行拷贝
	copy(b, m.activeMap[offset:readEnd])
	return int(readEnd - offset), nil
}

func (m *MMap) Write(b []byte) (int, error) {
	if err := m.remap(m.virtualSize, len(b)); err != nil {
		return 0, err
	}
	copy(m.activeMap[m.virtualSize:m.virtualSize+int64(len(b))], b)
	m.virtualSize += int64(len(b))
	return len(b), nil
}

func (m *MMap) Sync() error {
	return m.activeMap.Flush()
}

func (m *MMap) Close() error {
	if err := m.activeMap.Flush(); err != nil {
		return err
	}
	if err := m.activeMap.Unmap(); err != nil {
		return err
	}
	if err := m.ResetFileSize(); err != nil {
		return err
	}
	return m.file.Close()
}

func (m *MMap) Size() (int64, error) {
	return m.virtualSize, nil
}

func (m *MMap) ResetFileSize() error {
	return m.file.Truncate(m.virtualSize)
}

// 如果有必要, 扩展映射区域
func (m *MMap) remap(newBase int64, dataSize int) error {
	// 如果映射区域已包含所需数据, 直接返回
	if newBase+int64(dataSize) <= m.endOff {
		return nil
	}

	// 动态扩展 blockSize 的整数倍的长度
	m.endOff = ((newBase + int64(dataSize) + blockSize - 1) / blockSize) * blockSize

	// 如果新映射区域超过设置的文件大小, 则进行调整
	if info, _ := m.file.Stat(); info.Size() < m.endOff {
		if err := m.file.Truncate(m.endOff); err != nil {
			return fmt.Errorf("truncate failed: %v", err)
		}
	}

	// 解除旧映射
	if m.activeMap != nil {
		if err := m.activeMap.Unmap(); err != nil {
			return fmt.Errorf("unmap failed: %v", err)
		}
	}

	// 如果当前为 32 位系统, 判断新映射区域是否溢出
	if unsafe.Sizeof(0) == 4 && m.endOff > 1<<31-1 {
		return fmt.Errorf("32bit system max mapping size is 2GB")
	}
	// 创建新映射
	data, err := mmap.MapRegion(m.file, int(m.endOff), mmap.RDWR, 0, 0)
	if err != nil {
		return fmt.Errorf("mmap failed: %v", err)
	}

	m.activeMap = data

	return nil
}
