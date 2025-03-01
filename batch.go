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

const (
	// 批处理完成标识记录最大字节长度
	maxFinRecord = 70
)

// Batch 批处理操作客户端
type Batch struct {
	db             *DB
	staged         []*datafile.LogRecord // 暂存数据, 数组存储保证添加顺序
	stageIndex     map[uint64][]int      // 加快暂存数据查询效率
	options        BatchOptions          // 批处理配置
	mu             sync.RWMutex          // 批处理互斥锁, 保证批处理本身并发安全
	committed      bool                  // 已提交标识
	batchID        snowflake.ID          // 批次唯一ID
	cachedDataSize int64                 // 当前已缓存数据量
}

func (db *DB) NewBatch(options BatchOptions) *Batch {
	// 保证批处理期间禁用 DB 客户端使用, 保证 Batch 客户端保存全量最新数据
	db.mu.Lock()

	batch := &Batch{
		db:        db,
		options:   options,
		committed: false,
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

	// 缓存记录
	if logRecord == nil {
		// 将待写入的 logRecord 追加到缓存中
		// 从记录结构体缓冲池中获取结构体对象, 实现复用
		logRecord = b.db.recordPool.Get().(*datafile.LogRecord)
		logRecord.Key = append(logRecord.Key, key...)
		logRecord.Value = append(logRecord.Value, value...)

		// 计算当前已缓存数据是否超过当前活跃文件的剩余容量, 是则自动刷新缓存并更新活跃文件
		size := int64(datafile.GetLogRecordDiskSize(len(key), len(value)))
		if b.cachedDataSize+size+maxFinRecord > b.db.options.DataFileSize {
			if err := b.flushStagedAndUpdateFile(); err != nil {
				return err
			}
		}
		b.cachedDataSize += size
		b.addPendingRecord(key, logRecord)
		return nil
	}

	// 记录已在缓存中存在, 直接操作缓存
	// 计算更新缓存后已缓存数据是否超过当前活跃文件的剩余容量, 是则自动刷新缓存并更新活跃文件
	oldSize := int64(datafile.GetLogRecordDiskSize(len(logRecord.Key), len(logRecord.Value)))
	newSize := int64(datafile.GetLogRecordDiskSize(len(key), len(value)))
	if b.cachedDataSize+newSize-oldSize+maxFinRecord > b.db.options.DataFileSize {
		if err := b.flushStagedAndUpdateFile(); err != nil {
			return err
		}
		// 刷新后原 logRecord 已归还缓冲池, 需要重新获取
		logRecord = b.db.recordPool.Get().(*datafile.LogRecord)
		logRecord.Type = datafile.LogRecordNormal
		logRecord.Key = append(logRecord.Key, key...)
		logRecord.Value = append(logRecord.Value, value...)
		b.addPendingRecord(key, logRecord)
		b.cachedDataSize += newSize
	} else {
		// 如果缓存命中则直接修改缓存
		logRecord.Key = key
		logRecord.Value = value
		b.cachedDataSize += newSize - oldSize
	}
	return nil
}

func (b *Batch) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.committed {
		return nil, ErrBatchCommitted
	}
	logRecord := b.findPendingRecord(key)

	// 缓存命中直接返回
	if logRecord != nil {
		if logRecord.Type == datafile.LogRecordDeleted {
			return nil, ErrKeyNotFound
		}
		return logRecord.Value, nil
	}

	// 记录未缓存则执行查询
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

	logRecord := b.findPendingRecord(key)

	// 缓存命中, 直接操作缓存
	if logRecord != nil {
		b.cachedDataSize += int64(len(logRecord.Value))
		logRecord.Type = datafile.LogRecordDeleted
		logRecord.Value = nil
		return nil
	}

	// 记录未缓存则执行删除
	pos := b.db.index.Get(key)
	if pos == nil {
		return nil
	}

	// 持久化到磁盘, 追加墓碑值
	logRecord = b.db.recordPool.Get().(*datafile.LogRecord)
	logRecord.Type = datafile.LogRecordDeleted
	logRecord.Key = append(logRecord.Key, key...)

	// 如果当前数据文件不足以容纳暂存数据, 进行一次刷新
	size := int64(datafile.GetLogRecordDiskSize(len(key), 0))
	if b.cachedDataSize+size+maxFinRecord > b.db.options.DataFileSize {
		if err := b.flushStagedAndUpdateFile(); err != nil {
			return err
		}
	}
	b.addPendingRecord(key, logRecord)
	b.cachedDataSize += size
	return nil
}

func (b *Batch) Commit() error {
	// 提交后允许操作 DB 实例
	defer b.db.mu.Unlock()

	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.staged) == 0 {
		return nil
	}
	if b.committed {
		return ErrBatchCommitted
	}

	err := b.flushStaged()
	if err != nil {
		return err
	}

	// 追加批处理完成标识记录
	logRecord := b.db.recordPool.Get().(*datafile.LogRecord)
	logRecord.Key = append(logRecord.Key, b.batchID.Bytes()...)
	logRecord.Type = datafile.LogRecordBatchFinished
	_, err = b.db.activeFile.WriteLogRecord(logRecord, b.db.logRecordHeader)
	b.db.putRecordToPool(logRecord)
	if err != nil {
		return err
	}

	b.staged = nil
	b.stageIndex = nil
	b.committed = true
	return nil
}

// 从缓存中查找 key 对应的 logRecord
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

// 新增缓存
func (b *Batch) addPendingRecord(key []byte, record *datafile.LogRecord) {
	b.staged = append(b.staged, record)
	// 延迟初始化
	if b.stageIndex == nil {
		b.stageIndex = make(map[uint64][]int)
	}
	hashKey := xxhash.Sum64(key)
	b.stageIndex[hashKey] = append(b.stageIndex[hashKey], len(b.staged)-1)
}

// 刷新缓存并更新活跃文件
func (b *Batch) flushStagedAndUpdateFile() error {
	if err := b.flushStaged(); err != nil {
		return fmt.Errorf("flush staged failed: %v", err)
	}
	err := b.db.sync()
	if err != nil {
		return fmt.Errorf("sync failed: %v", err)
	}
	return nil
}

// 刷新缓存
func (b *Batch) flushStaged() error {
	// 顺序遍历暂存数据依次追加磁盘
	for _, record := range b.staged {
		record.BatchID = uint64(b.batchID)
		b.db.activeFile.WriteStagedLogRecord(record, b.db.logRecordHeader)
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
			b.db.reclaimSize += int64(dataPos[i].Size)
		} else {
			pos = b.db.index.Put(record.Key, dataPos[i])
		}
		if pos != nil {
			b.db.reclaimSize += int64(pos.Size)
		}
		// 写入完成后将结构体归还缓冲池
		b.db.putRecordToPool(record)
	}

	// 清空缓存
	b.staged = b.staged[:0]
	b.stageIndex = map[uint64][]int{}
	b.cachedDataSize = 0
	return nil
}
