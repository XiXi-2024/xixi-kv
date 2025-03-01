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
	ID             FileID                       // 文件 id
	ReadWriter     fio.ReadWriter               // IO 实现
	lastBlockID    uint32                       // 末尾 Block ID
	lastBlockSize  uint32                       // 末尾 Block 已使用字节数
	closed         bool                         // 文件关闭标识
	headerBuf      []byte                       // chunk 头部复用缓冲区
	bufferedWrites []*bytebufferpool.ByteBuffer // 批处理缓存数据
}

var blockPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, blockSize)
	},
}

func getBuf() []byte {
	return blockPool.Get().([]byte)
}

func putBuf(buf []byte) {
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
	EncodeLogRecord(logRecord, header, buf)

	pos, err := df.writeSingle(buf)

	return pos, err
}

func (df *DataFile) WriteHintRecord(key []byte, hintPos []byte, pos *DataPos) error {
	if df.closed {
		return ErrClosed
	}
	data := bytebufferpool.Get()
	EncodeHintRecord(key, pos, hintPos, data)
	_, err := df.writeSingle(data)

	return err
}

func (df *DataFile) WriteMergeFinRecord(id FileID) error {
	if df.closed {
		return ErrClosed
	}
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, id)
	_, err := df.ReadWriter.Write(data)
	return err
}

func (df *DataFile) WriteStagedLogRecord(logRecord *LogRecord, header []byte) {
	if df.closed {
		return
	}
	// 不应归还缓冲区, 避免数据被覆盖
	buf := bytebufferpool.Get()
	EncodeLogRecord(logRecord, header, buf)
	df.bufferedWrites = append(df.bufferedWrites, buf)
	return
}

func (df *DataFile) FlushStaged() ([]*DataPos, error) {
	dataPos, err := df.writeAll(df.bufferedWrites)
	if err != nil {
		return nil, err
	}
	// 清空暂存数据
	df.bufferedWrites = df.bufferedWrites[:0]
	return dataPos, nil
}

func (df *DataFile) writeSingle(data *bytebufferpool.ByteBuffer) (*DataPos, error) {
	buf := bytebufferpool.Get()

	pos, nextID, nextSize := df.writeToBuf(data.Bytes(), df.lastBlockID, df.lastBlockSize, buf)
	_, err := df.ReadWriter.Write(buf.Bytes())
	if err != nil {
		return nil, err
	}
	df.lastBlockID = nextID
	df.lastBlockSize = nextSize

	bytebufferpool.Put(buf)
	bytebufferpool.Put(data)

	return pos, nil
}

func (df *DataFile) writeAll(records []*bytebufferpool.ByteBuffer) ([]*DataPos, error) {
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	var (
		nextID    = df.lastBlockID
		nextSize  = df.lastBlockSize
		recordPos = make([]*DataPos, 0, len(records))
	)
	for _, data := range records {
		pos, id, size := df.writeToBuf(data.Bytes(), nextID, nextSize, buf)
		nextID, nextSize = id, size
		// 数据处理完成, 归还缓冲区
		recordPos = append(recordPos, pos)
		bytebufferpool.Put(data)
	}
	_, err := df.ReadWriter.Write(buf.Bytes())
	if err != nil {
		return nil, err
	}

	df.lastBlockID = nextID
	df.lastBlockSize = nextSize

	return recordPos, nil
}

func (df *DataFile) writeToBuf(data []byte, blockID uint32, blockLen uint32, buf *bytebufferpool.ByteBuffer) (*DataPos, uint32, uint32) {
	var (
		nextID   = blockID
		nextSize = blockLen
	)

	// 如果末尾 Block 剩余字节小于等于 chunk 头部所需字节则进行填充
	// 等于 chunk 头部时 data 长度为 0, 无意义
	// 仅初始写入时存在该情况
	if nextSize+chunkHeaderSize >= blockSize && nextSize != blockSize {
		p := make([]byte, blockSize-nextSize)
		buf.B = append(buf.B, p...)

		nextID += 1
		nextSize = 0
	}

	pos := &DataPos{
		Fid:     df.ID,
		BlockID: nextID,
		Offset:  nextSize,
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
			writeSize = min(writeSize, blockSize-nextSize-chunkHeaderSize)
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

	pos.Size = chunkCount*chunkHeaderSize + totalSize

	nextSize += pos.Size
	nextID += nextSize / blockSize
	nextSize %= blockSize

	return pos, nextID, nextSize
}

func (df *DataFile) ReadRecordValue(logRecordPos *DataPos) ([]byte, error) {
	if df.closed {
		return nil, ErrClosed
	}
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	err := df.readToBuf(logRecordPos.BlockID, logRecordPos.Offset, buf)
	if err != nil {
		return nil, err
	}
	value := DecodeLogRecordValue(buf.B)
	return value, nil
}

func (df *DataFile) ReadMergeFinRecord() FileID {
	if df.closed {
		return 0
	}
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	err := df.readToBuf(0, 0, buf)
	if err != nil {
		return 0
	}
	value := binary.LittleEndian.Uint32(buf.Bytes())
	return value
}

func (df *DataFile) readToBuf(blockID uint32, offset uint32, buf *bytebufferpool.ByteBuffer) error {
	if blockID > df.lastBlockID {
		return io.EOF
	}
	// 获取用于读取 block 的缓冲区
	block := getBuf()
	defer putBuf(block)
	fileSize := df.Size()
	for {
		// 当前 block 绝对偏移量
		off := int64(blockID) * blockSize
		// 当前 block 实际大小
		size := uint32(min(fileSize-off, blockSize))

		// 更多是初始参数校验
		if offset >= size {
			return io.EOF
		}

		if _, err := df.ReadWriter.Read(block[0:size], off); err != nil {
			return err
		}

		// 对当前 chunk 解码
		data, chunkType, err := DecodeChunk(block[offset:])
		if err != nil {
			return err
		}
		buf.B = append(buf.B, data...)
		// last chunk
		if chunkType == Full || chunkType == Last {
			break
		}
		blockID += 1
		offset = 0
	}

	return nil
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
	if df.closed {
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

// GetLogRecordDiskSize 粗略计算 logRecord 在磁盘中占用的字节数
func GetLogRecordDiskSize(keySize, valueSize int) int {
	// 单条记录编码后的大致长度
	size := MaxLogRecordHeaderSize + keySize + valueSize + binary.MaxVarintLen64 + 1
	// chunk 头部的大致总长度
	size += chunkHeaderSize + (size/blockSize+1)*chunkHeaderSize
	return size
}
