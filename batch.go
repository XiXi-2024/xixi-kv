package xixi_kv

import (
	"bytes"
	"fmt"
	"github.com/XiXi-2024/xixi-kv/datafile"
	"github.com/bwmarrin/snowflake"
	"github.com/cespare/xxhash"
	"sync"
)

// todo 优化点：新增方法结束自动提交的事务形式
// todo 优化点：gpt查询优化点

const (
	maxFinRecord = 70
)

// Batch 批处理操作客户端
type Batch struct {
	db            *DB
	staged        []*datafile.LogRecord // 暂存数据, 数组存储保证添加顺序
	stageIndex    map[uint64][]int      // 加快暂存数据查询效率
	options       BatchOptions          // 批处理配置
	mu            sync.RWMutex          // 批处理互斥锁, 保证批处理本身并发安全
	committed     bool                  // 已提交标识
	batchID       snowflake.ID          // 批次唯一ID
	logRecordPool sync.Pool             // 记录结构体复用缓冲池
	dataSize      int64                 // 当前累计数据量
}

func (db *DB) NewBatch(options BatchOptions) *Batch {
	// 保证批处理期间禁用 DB 客户端使用, 保证 Batch 客户端保存全量最新数据
	db.mu.RLock()

	batch := &Batch{
		db:        db,
		options:   options,
		committed: false,
		logRecordPool: sync.Pool{
			New: func() interface{} {
				return &datafile.LogRecord{}
			},
		},
	}
	node, err := snowflake.NewNode(1)
	if err != nil {
		panic(fmt.Sprintf("snowflake.NewNode(1) failed: %v", err))
	}
	batch.batchID = node.Generate()
	return batch
}

func (b *Batch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.committed {
		return ErrBatchCommitted
	}
	logRecord := b.findPendingRecord(key)
	if logRecord == nil {
		logRecord = b.logRecordPool.Get().(*datafile.LogRecord)
		logRecord.Key = key
		logRecord.Value = value
		logRecord.Type = datafile.LogRecordNormal
		size := int64(datafile.GetMaxDataSize(len(key), len(value)))
		if b.dataSize+size > b.db.options.DataFileSize {
			if err := b.FlushStaged(); err != nil {
				return fmt.Errorf("flush staged failed: %v", err)
			}
			// 更新数据文件
			err := b.db.sync()
			if err != nil {
				return fmt.Errorf("sync failed: %v", err)
			}
		}
		b.dataSize += size
		b.addPendingRecord(key, logRecord)
		return nil
	} else {
		oldSize := int64(datafile.GetMaxDataSize(len(logRecord.Key), len(logRecord.Value)))
		newSize := int64(datafile.GetMaxDataSize(len(key), len(value)))
		if b.dataSize+newSize-oldSize+maxFinRecord > b.db.options.DataFileSize {
			if err := b.FlushStaged(); err != nil {
				return fmt.Errorf("flush staged failed: %v", err)
			}
			// 更新数据文件
			err := b.db.sync()
			if err != nil {
				return fmt.Errorf("sync failed: %v", err)
			}
			// 由于原 logRecord 已归还, 重新获取
			logRecord = b.logRecordPool.Get().(*datafile.LogRecord)
			logRecord.Type = datafile.LogRecordNormal
			logRecord.Key = key
			logRecord.Value = value
			b.addPendingRecord(key, logRecord)
			b.dataSize += newSize
		} else {
			logRecord.Key = key
			logRecord.Value = value
			b.dataSize += newSize - oldSize
		}
	}
	return nil
}

func (b *Batch) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	b.mu.RLock()
	if b.db.closed {
		return nil, ErrDBClosed
	}
	if b.committed {
		return nil, ErrBatchCommitted
	}
	logRecord := b.findPendingRecord(key)
	b.mu.RUnlock()

	// 优先查询内存中暂存的最新数据
	if logRecord != nil {
		if logRecord.Type == datafile.LogRecordDeleted {
			return nil, ErrKeyNotFound
		}
		return logRecord.Value, nil
	}

	// 内存中不存在再查询磁盘中的记录
	pos := b.db.index.Get(key)
	if pos == nil {
		return nil, ErrKeyNotFound
	}
	value, err := b.db.activeFile.ReadRecordValue(pos)
	if err != nil {
		return nil, err
	}

	return value, nil
}

func (b *Batch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	// 优先操作内存中最新的暂存数据
	logRecord := b.findPendingRecord(key)
	if logRecord != nil {
		logRecord.Type = datafile.LogRecordDeleted
		logRecord.Value = nil
	}

	pos := b.db.index.Get(key)
	if pos == nil {
		return nil
	}

	// 持久化到磁盘, 追加墓碑值
	logRecord = &datafile.LogRecord{
		Key:  key,
		Type: datafile.LogRecordDeleted,
	}
	// 如果当前数据文件不足以容纳暂存数据, 进行一次刷新
	size := int64(datafile.GetMaxDataSize(len(key), 0))
	if b.dataSize+size+maxFinRecord > b.db.options.DataFileSize {
		if err := b.FlushStaged(); err != nil {
			return fmt.Errorf("flush staged failed: %v", err)
		}
	}
	b.addPendingRecord(key, logRecord)
	b.dataSize += size
	return nil
}

func (b *Batch) Commit() error {
	// 提交后允许操作 DB 实例
	defer b.db.mu.RUnlock()

	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.staged) == 0 {
		return nil
	}
	if b.committed {
		return ErrBatchCommitted
	}

	err := b.FlushStaged()
	if err != nil {
		return err
	}

	// 追加批处理完成标识记录
	logRecord := b.logRecordPool.Get().(*datafile.LogRecord)
	logRecord.Key = b.batchID.Bytes()
	logRecord.Type = datafile.LogRecordBatchFinished
	_, err = b.db.activeFile.WriteLogRecord(logRecord, b.db.logRecordHeader)

	if err != nil {
		return err
	}

	b.staged = nil
	b.stageIndex = nil
	b.committed = true
	return nil
}

// 通过 map 快速查找 key 对应的暂存记录位置
func (b *Batch) findPendingRecord(key []byte) *datafile.LogRecord {
	if len(b.stageIndex) == 0 {
		return nil
	}

	hashKey := xxhash.Sum64(key)
	for _, id := range b.stageIndex[hashKey] {
		if bytes.Equal(b.staged[id].Key, key) {
			return b.staged[id]
		}
	}
	return nil
}

// 新增暂存数据并记录位置
func (b *Batch) addPendingRecord(key []byte, record *datafile.LogRecord) {
	b.staged = append(b.staged, record)
	// 延迟初始化
	if b.stageIndex == nil {
		b.stageIndex = make(map[uint64][]int)
	}
	hashKey := xxhash.Sum64(key)
	b.stageIndex[hashKey] = append(b.stageIndex[hashKey], len(b.staged)-1)
}

func (b *Batch) FlushStaged() error {
	// 顺序遍历暂存数据依次追加磁盘
	for _, record := range b.staged {
		record.BatchID = uint64(b.batchID)
		b.db.activeFile.WriteStagedLogRecord(record, b.db.logRecordHeader)
		// 写入完成后放回缓冲池
		b.logRecordPool.Put(record)
	}

	// IO 层面将所有数据字节一次性写入
	dataPos, err := b.db.activeFile.FlushStaged()
	if err != nil {
		return err
	}
	if len(dataPos) != len(b.staged) {
		panic("chunk positions length is not equal to pending writes length")
	}

	// 根据配置判断是否立即持久化
	if b.options.Sync {
		if err := b.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// 追加操作全部完成后, 更新索引
	for i, record := range b.staged {
		var pos *datafile.DataPos
		if record.Type == datafile.LogRecordDeleted {
			pos = b.db.index.Delete(record.Key)
		} else {
			pos = b.db.index.Put(record.Key, dataPos[i])
		}
		if pos != nil {
			b.db.reclaimSize += int64(pos.Size)
		}
	}

	// 清空暂存数据
	b.staged = b.staged[:0]
	b.stageIndex = map[uint64][]int{}
	b.dataSize = 0
	return nil
}
