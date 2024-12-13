package xixi_bitcask_kv

import (
	"errors"
	"github.com/XiXi-2024/xixi-bitcask-kv/data"
	"github.com/XiXi-2024/xixi-bitcask-kv/index"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB bitcask存储引擎客户端
type DB struct {
	options    Options // 用户配置项
	mu         *sync.RWMutex
	fileIds    []int                     // 数据文件id集合 仅用于索引加载
	activeFile *data.DataFile            // 当前活跃文件 允许读写
	olderFiles map[uint32]*data.DataFile // 旧数据文件 只读
	index      index.Indexer             // 内存索引
}

// Open 客户端初始化
func Open(options Options) (*DB, error) {
	// 配置项校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// 判断数据目录是否存在 不存在则创建
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 初始化 DB 实例
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType),
	}

	// 加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// 加载索引
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	return db, nil
}

// Put 写入 Key/Value 数据 Key不允许为空
func (db *DB) Put(key []byte, value []byte) error {
	// 判断 key 是否合法
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造 LogRecord 结构体
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 将日志记录追加到数据文件中
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// 更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}

	return nil
}

// Get 根据 Key 读取数据
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	// 判断key是否合法
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// 从内存中获取 Key 对应的索引数据
	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	// 根据文件 id 找到对应的数据文件
	var dataFile *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// 根据偏移读取对应的数据
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	// 判断是否已被删除
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

// 将日志记录追加到当前活跃文件
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 判断当前是否存在活跃文件 不存在则初始化
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 编码
	encRecord, size := data.EncodeLogRecord(logRecord)

	// 活跃文件剩余空间不足 新建数据文件作为新的活跃文件
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		// 持久化原活跃文件
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 将原活跃文件转换为旧数据文件
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		// 设置新的活跃文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 根据已有偏移量将日志记录追加写入当前活跃文件
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	// 根据用户配置决定是否立即持久化
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	// 构造内存索引信息返回
	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff}
	return pos, nil
}

// 设置新的活跃文件
// 存在并发问题
func (db *DB) setActiveDataFile() error {
	// 获取新的文件id 实际为自增
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}
	// 获取新的数据文件
	// 通过用户配置文件的配置项获取文件地址
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}

	db.activeFile = dataFile
	return nil
}

// 从磁盘中加载数据文件
func (db *DB) loadDataFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	// 遍历目录 筛取以 .data 结尾的文件
	for _, entry := range dirEntries {
		if !strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			continue
		}
		// 解析文件名 转换为文件id
		splitNames := strings.Split(entry.Name(), ".")
		fileId, err := strconv.Atoi(splitNames[0])
		// 文件名不符合规范 解析失败
		if err != nil {
			return ErrDataDirectoryCorrupted
		}
		fileIds = append(fileIds, fileId)
	}

	// 对文件id排序 从小到大加载
	sort.Ints(fileIds)
	db.fileIds = fileIds

	// 遍历文件id 构建对应的数据文件实例
	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}
		// id 最大的文件视为最新文件 标识为活跃文件
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.olderFiles[uint32(fid)] = dataFile
		}
	}

	return nil
}

// 从数据文件中加载索引
func (db *DB) loadIndexFromDataFiles() error {
	// 未加载数据文件 数据库为空 直接返回
	if len(db.fileIds) == 0 {
		return nil
	}

	// 遍历所有数据文件实例 逐个更新到内存索引中
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		var offset int64 = 0
		// 遍历和加载当前数据文件的日志记录
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// 构建内存索引信息并保存
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset}
			// 判断日志记录状态
			if logRecord.Type == data.LogRecordDeleted {
				// 墓碑值 则删除对应的内存索引信息
				db.index.Delete(logRecord.Key)
			} else {
				db.index.Put(logRecord.Key, logRecordPos)
			}

			// 更新已读取位置偏移
			offset += size
		}

		// 当前为活跃文件 更新文件实例的WriteOff 供之后追加写入
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}

	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}

	return nil
}
