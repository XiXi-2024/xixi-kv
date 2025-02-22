package datafile

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/XiXi-2024/xixi-kv/fio"
	"github.com/valyala/bytebufferpool"
	"hash/crc32"
	"io"
	"path/filepath"
	"sync"
)

var (
	ErrClosed     = errors.New("the segment file is closed")
	ErrInvalidCRC = errors.New("invalid crc value, log record maybe corrupted")
)

type FileID = uint32

type FileSuffix = string

const (
	DataFileSuffix          FileSuffix = ".data"
	HintFileSuffix          FileSuffix = ".hint"
	MergeFinishedFileSuffix FileSuffix = ".merge-finished"
	FileLockSuffix          FileSuffix = ".lock"
)

// DataFile 数据文件
type DataFile struct {
	ID            FileID         // 文件 id
	ReadWriter    fio.ReadWriter // IO 实现
	lastBlockID   uint32         // 末尾 Block ID
	lastBlockSize uint32         // 末尾 Block 已使用字节数
	closed        bool           // 文件关闭标识
	headerBuf     []byte         // chunk 头部复用缓冲区
}

var blockPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, blockSize)
	},
}

func getBuffer() []byte {
	return blockPool.Get().([]byte)
}

func putBuffer(buf []byte) {
	blockPool.Put(buf)
}

func OpenFile(dirPath string, id FileID, suffix FileSuffix, ioType fio.FileIOType) (*DataFile, error) {
	fileName := GetFileName(dirPath, id, suffix)
	readWriter, err := fio.NewReadWriter(fileName, ioType)
	if err != nil {
		return nil, err
	}

	// 获取当前文件大小
	size, err := readWriter.Size()
	if err != nil {
		return nil, err
	}

	return &DataFile{
		ID:            id,
		ReadWriter:    readWriter,
		lastBlockID:   uint32(size / blockSize),
		lastBlockSize: uint32(size % blockSize),
		closed:        false,
		headerBuf:     make([]byte, chunkHeaderSize),
	}, nil
}

// GetFileName 获取指定类型的文件路径
func GetFileName(dirPath string, id FileID, suffix FileSuffix) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d"+suffix, id))
}

func (df *DataFile) WriteLogRecord(logRecord *LogRecord, header []byte) (*DataPos, error) {
	if df.closed {
		return nil, ErrClosed
	}
	buf := bytebufferpool.Get()
	buf.Reset()
	defer bytebufferpool.Put(buf)
	data := EncodeLogRecord(logRecord, header, buf)

	pos, err := df.write(data)

	return pos, err
}

func (df *DataFile) WriteHintRecord(key []byte, pos *DataPos) error {
	if df.closed {
		return ErrClosed
	}
	data := EncodeHintRecord(key, pos)
	_, err := df.write(data)
	return err
}

func (df *DataFile) WriteMergeFinRecord(id FileID) error {
	if df.closed {
		return ErrClosed
	}
	data := EncodeMergeFinRecord(id)
	_, err := df.write(data)
	return err
}

func (df *DataFile) write(data []byte) (*DataPos, error) {
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)

	// 如果末尾 Block 剩余字节小于等于 chunk 头部所需字节则进行填充
	// 等于 chunk 头部时 data 长度为 0, 无意义
	// 仅初始写入时存在该情况
	if df.lastBlockSize+chunkHeaderSize >= blockSize && df.lastBlockSize != blockSize {
		p := make([]byte, blockSize-df.lastBlockSize)
		buf.B = append(buf.B, p...)

		df.lastBlockID += 1
		df.lastBlockSize = 0
	}

	pos := &DataPos{
		Fid:     df.ID,
		BlockID: df.lastBlockID,
		Offset:  df.lastBlockSize,
	}

	var (
		totalSize   = uint32(len(data)) // 总数据量
		writtenSize uint32              // 已写入的字节数
		chunkCount  uint32              // 已写入的 Chunk 头部个数
		writeSize   uint32              // 将要写入的字节数
	)
	for writtenSize < totalSize {
		var chunkType ChunkType
		// 计算将要写入的字节数
		writeSize = totalSize - writtenSize
		if writtenSize == 0 {
			// 仅在初始时存在 currBlockSize > 0 的场景
			writeSize = min(writeSize, blockSize-df.lastBlockSize-chunkHeaderSize)
		} else {
			writeSize = min(writeSize, blockSize-chunkHeaderSize)
		}
		if writtenSize+writeSize == totalSize {
			// 数据写入完毕
			if writtenSize == 0 {
				// 完整写入
				chunkType = Full
			} else {
				// 最后写入
				chunkType = Last
			}
		} else {
			if writtenSize == 0 {
				// 首次写入
				chunkType = First
			} else {
				// 中间写入
				chunkType = Middle
			}
		}

		// chunk header
		// type
		df.headerBuf[6] = chunkType
		// length
		binary.LittleEndian.PutUint16(df.headerBuf[4:6], uint16(writeSize))
		sum := crc32.ChecksumIEEE(df.headerBuf[4:])
		sum = crc32.Update(sum, crc32.IEEETable, data[writtenSize:writtenSize+writeSize])
		binary.LittleEndian.PutUint32(df.headerBuf[:4], sum)

		_, _ = buf.Write(df.headerBuf)
		_, _ = buf.Write(data[writtenSize : writtenSize+writeSize])

		writtenSize += writeSize
		chunkCount++
	}

	_, err := df.ReadWriter.Write(buf.Bytes())
	if err != nil {
		return nil, err
	}

	pos.Size = chunkCount*chunkHeaderSize + totalSize

	// 最后再更新, 中途出现 err 可回滚
	df.lastBlockSize += pos.Size
	df.lastBlockID += df.lastBlockSize / blockSize
	df.lastBlockSize %= blockSize

	return pos, nil
}

func (df *DataFile) ReadLogRecord(blockID uint32, offset uint32) (*LogRecord, error) {
	if df.closed {
		return nil, ErrClosed
	}
	data, err := df.read(blockID, offset)
	if err != nil {
		return nil, err
	}
	return DecodeLogRecord(data), nil
}

func (df *DataFile) ReadMergeFinRecord() (FileID, error) {
	if df.closed {
		return 0, ErrClosed
	}
	data, err := df.read(0, 0)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(data), nil
}

func (df *DataFile) read(blockID uint32, offset uint32) ([]byte, error) {
	if blockID > df.lastBlockID {
		return nil, io.EOF
	}
	// 获取用于读取 block 的缓冲区
	block := getBuffer()
	defer putBuffer(block)

	var (
		fileSize = df.Size()
		res      []byte
	)
	for {
		// 当前 block 绝对偏移量
		off := int64(blockID) * blockSize
		// 当前 block 实际大小
		size := uint32(min(fileSize-off, blockSize))

		// 更多是初始参数校验
		if offset >= size {
			return nil, io.EOF
		}

		if _, err := df.ReadWriter.Read(block[0:size], off); err != nil {
			return nil, err
		}

		// 对当前 chunk 解码
		data, chunkType, err := DecodeChunk(block[offset:])
		if err != nil {
			return nil, err
		}
		res = append(res, data...)
		// last chunk
		if chunkType == Full || chunkType == Last {
			break
		}
		blockID += 1
		offset = 0
	}

	return res, nil
}

type DataReader struct {
	dataFile *DataFile
	blockID  uint32
	offset   uint32
	blockBuf []byte
}

func (df *DataFile) NewReader() *DataReader {
	return &DataReader{
		dataFile: df,
		blockID:  0,
		offset:   0,
		blockBuf: make([]byte, blockSize),
	}
}

func (reader *DataReader) NextLogRecord() (*LogRecord, *DataPos, error) {
	if reader.dataFile.closed {
		return nil, nil, ErrClosed
	}

	data, pos, err := reader.next()
	if err != nil {
		return nil, nil, err
	}
	return DecodeLogRecord(data), pos, nil
}

func (reader *DataReader) NextHintRecord() ([]byte, *DataPos, error) {
	if reader.dataFile.closed {
		return nil, nil, ErrClosed
	}

	data, _, err := reader.next()
	if err != nil {
		return nil, nil, err
	}

	hintRecord, pos := DecodeHintRecord(data)
	return hintRecord, pos, nil
}

func (reader *DataReader) next() ([]byte, *DataPos, error) {
	var (
		fileSize = reader.dataFile.Size()
		cnt      uint32
		res      []byte
	)

	pos := &DataPos{
		Fid:     reader.dataFile.ID,
		BlockID: reader.blockID,
		Offset:  reader.offset,
	}

	for {
		// 当前 block 绝对偏移量
		off := int64(reader.blockID) * blockSize
		// 当前 block 实际大小
		size := uint32(min(fileSize-off, blockSize))

		if reader.offset >= size {
			return nil, nil, io.EOF
		}

		// 从共享缓冲区中读取
		_, err := reader.dataFile.ReadWriter.Read(reader.blockBuf[0:size], off)
		if err != nil {
			return nil, nil, err
		}

		// 对当前 chunk 解码
		data, chunkType, err := DecodeChunk(reader.blockBuf[reader.offset:])
		if err != nil {
			return nil, nil, err
		}
		res = append(res, data...)
		cnt++

		// last chunk
		if chunkType == Full || chunkType == Last {
			reader.offset += uint32(chunkHeaderSize + len(data))
			if reader.offset+chunkHeaderSize >= blockSize {
				reader.blockID += 1
				reader.offset = 0
			}
			break
		}
		reader.offset = 0
		reader.blockID += 1
	}

	pos.Size = cnt*chunkHeaderSize + uint32(len(res))

	return res, pos, nil
}

func (df *DataFile) Size() int64 {
	return int64(df.lastBlockID)*int64(blockSize) + int64(df.lastBlockSize)
}

func (df *DataFile) Sync() error {
	if !df.closed {
		return nil
	}
	return df.ReadWriter.Sync()
}

func (df *DataFile) Close() error {
	if df.closed {
		return nil
	}
	df.closed = true
	return df.ReadWriter.Close()
}
