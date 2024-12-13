package xixi_bitcask_kv

import (
	"github.com/XiXi-2024/xixi-bitcask-kv/data"
	"github.com/XiXi-2024/xixi-bitcask-kv/index"
	"sync"
)

// DB bitcask存储引擎客户端
type DB struct {
	options    Options // 用户配置项
	mu         *sync.RWMutex
	activeFile *data.DataFile            // 当前活跃文件 允许读写
	olderFiles map[uint32]*data.DataFile // 旧数据文件 只读
	index      index.Indexer             // 内存索引
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
	logRecord, err := dataFile.ReadLogRecord(logRecordPos.Offset)
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
