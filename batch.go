package xixi_kv

import (
	"encoding/binary"
	"github.com/XiXi-2024/xixi-kv/datafile"
	"sync"
	"sync/atomic"
)

// 非事务 key 前缀标识
const nonTransactionSeqNo uint64 = 0

// 事务完成标识 key
var txnFinKey = []byte("txn-fin")

// WriteBatch 事务客户端
// todo 优化点：新增方法结束自动提交的事务形式
// todo 优化点：新增 Get 方法
type WriteBatch struct {
	options       WriteBatchOptions
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*datafile.LogRecord // 暂存数据
}

// NewWriteBatch 创建新 WriteBatch 实例
func (db *DB) NewWriteBatch(opts WriteBatchOptions) *WriteBatch {
	return &WriteBatch{
		options:       opts,
		mu:            new(sync.Mutex),
		db:            db,
		pendingWrites: make(map[string]*datafile.LogRecord),
	}
}

func (wb *WriteBatch) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 仅暂存
	logRecord := &datafile.LogRecord{Key: key, Value: value}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Delete 根据 key 删除元素
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	logRecordPos := wb.db.index.Get(key)

	// 待删除元素未提交, 删除缓存即可
	if logRecordPos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	// 待删除元素已持久化, 追加墓碑值
	logRecord := &datafile.LogRecord{Key: key, Type: datafile.LogRecordDeleted}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Commit 事务提交, 将暂存数据持久化并更新索引
func (wb *WriteBatch) Commit() error {
	// 对 WB 实例加锁
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 缓存为空
	if len(wb.pendingWrites) == 0 {
		return nil
	}

	// 已缓存个数超过配置的最大数量
	// todo bug：是否应按最大数量提交
	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}

	// 对 DB 实例加锁, 串行化实现隔离性
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	// 获取当前最新的事务序列号
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	// 遍历当前事务客户端的写入缓存, 依次进行写入
	// 由于缓存包含最新数据, 故允许无序遍历
	positions := make(map[string]*datafile.DataPos)
	for _, record := range wb.pendingWrites {
		// 无需重复加锁, 使用不加锁的 appendLogRecord 方法
		logRecordPos, err := wb.db.appendLogRecord(&datafile.LogRecord{
			// 将 key 和 seqNo 进行合并, 节省空间
			Key:   logRecordKeyWithSeq(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})

		if err != nil {
			return err
		}

		// 暂存索引信息, 所有数据写入完成后统一更新索引
		positions[string(record.Key)] = logRecordPos
	}

	// 事务成功, 追加带事务完成标识的日志记录
	finishedRecord := &datafile.LogRecord{
		Key:  logRecordKeyWithSeq(txnFinKey, seqNo),
		Type: datafile.LogRecordTxnFinished,
	}
	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}

	// 根据配置项决定是否立即持久化
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// 数据持久化完成 更新内存索引
	for _, record := range wb.pendingWrites {
		pos := positions[string(record.Key)]
		var oldPos *datafile.DataPos
		if record.Type == datafile.LogRecordNormal {
			oldPos = wb.db.index.Put(record.Key, pos)
		}
		// 追加形式, 遇到删除状态的日志记录同样更新索引
		// todo bug：未统计 pos 本身的字节数
		if record.Type == datafile.LogRecordDeleted {
			oldPos, _ = wb.db.index.Delete(record.Key)
		}
		if oldPos != nil {
			wb.db.reclaimSize += int64(oldPos.Size)
		}
	}

	// 清空暂存数据
	wb.pendingWrites = make(map[string]*datafile.LogRecord)

	return nil
}

// 将 key 和事务ID seqNo 合并编码
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	// 获取 seqNo 实际占用字节数
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)

	// 按实际长度创建字节数组顺序存储
	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)

	return encKey
}
