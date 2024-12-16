package xixi_bitcask_kv

import (
	"encoding/binary"
	"errors"
	"github.com/XiXi-2024/xixi-bitcask-kv/data"
	"github.com/XiXi-2024/xixi-bitcask-kv/index"
	"io"
	"os"
	"path/filepath"
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
	seqNo      uint64                    // 事务序列号 全局递增
	isMerging  bool                      // merge 执行状态标识
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

	// 加载 merge 临时目录中的数据文件
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// 加载数据目录中的数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// 先尝试使用 hint 文件加载索引
	if err := db.loadIndexFromHintFile(); err != nil {
		return nil, err
	}

	// 加载索引
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	return db, nil
}

// Put 写入key/value数据
func (db *DB) Put(key []byte, value []byte) error {
	// 判断 key 是否合法
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造 LogRecord 结构体
	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 将日志记录追加到数据文件中
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// 更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}

	return nil
}

// Get 根据 key 读取数据
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	// 判断key是否合法
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// 从内存中获取 key 对应的索引数据
	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	// 获取 value 并返回
	return db.getValueByPosition(logRecordPos)
}

// Delete 根据 key 删除数据
func (db *DB) Delete(key []byte) error {
	// 验证 key 是否合法
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// key 不存在直接返回 避免重复向磁盘数据文件追加墓碑值
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	// 构造 LogRecord 设置删除状态 作为墓碑值追加到数据文件中
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted,
	}
	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// 同步删除索引中对应key
	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}

	return nil
}

// ListKeys 获取数据库中的所有 key
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	defer iterator.Close()
	keys := make([][]byte, db.index.Size())
	var idx int
	// 直接通过迭代器遍历获取所有 key
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}

// Fold 遍历数据库中所有 key/value 数据 由传入的函数逐个进行自定义操作 直到遍历完成或函数返回false终止遍历
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// 利用索引迭代器进行遍历
	iterator := db.index.Iterator(false)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}

		// 将遍历的每个 key/value 交给传入的函数进行处理
		if !fn(iterator.Key(), value) {
			// 函数返回false时终止遍历
			break
		}
	}
	return nil
}

// Close 关闭数据库
func (db *DB) Close() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	// 关闭当前活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	// 关闭旧的数据文件
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Sync 数据持久化
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	// 仅持久化当前活跃文件即可
	return db.activeFile.Sync()
}

// 将日志记录追加到当前活跃文件 加锁
func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.appendLogRecord(logRecord)
}

// 将日志记录追加到当前活跃文件 不加锁
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
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
		// todo
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

	// 判断是否发生 merge
	// 如果发生 merge, 从完成标识文件中获取未参与 merge 的最近数据文件id
	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinFileName := filepath.Join(db.options.DirPath, data.DataFileNameSuffix)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid
	}

	// 更新索引逻辑封装为局部函数 实现复用
	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		// 判断日志记录状态
		var ok bool
		if typ == data.LogRecordDeleted {
			// 墓碑值 则删除对应的内存索引信息
			ok = db.index.Delete(key)
		} else {
			ok = db.index.Put(key, pos)
		}

		if !ok {
			panic("failed to update index at startup")
		}
	}

	// 属于事务提交的记录的相关暂存数据
	transactionRecords := make(map[uint64][]*data.TransactionRecords)
	// 临时事务序列号 更新获取最大值
	var currentSeqNo uint64 = nonTransactionSeqNo

	// 遍历所有数据文件实例 逐个更新到内存索引中
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		// 当前数据文件对应的索引信息已通过 hint 文件加载 无需重复加载
		if hasMerge && fileId < nonMergeFileId {
			continue
		}
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

			// 解析 key, 提取真实 key 和 seq 事务前缀
			realKey, seqNo := parseLogRecordKey(logRecord.Key)

			// 判断当前日志记录是否属于事务提交
			if seqNo == nonTransactionSeqNo {
				// 日志记录属于非事务提交, 直接更新即可
				// 索引存放的 key 是真实 key
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				// 日志记录属于事务提交
				// 读取到带事务完成标识的记录时统一更新
				if logRecord.Type == data.LogRecordTxnFinished {
					// 更新对应事务的所有数据
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					logRecord.Key = realKey
					// 暂存用于后续更新索引的相关数据
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecords{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}

			// 序列号自增 获取最新的序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			// 更新已读取位置偏移
			offset += size
		}

		// 当前为活跃文件 更新文件实例的WriteOff 供之后追加写入
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}

	// 更新事务序列号 确保后续获取序列号唯一
	db.seqNo = currentSeqNo

	return nil
}

// 校验配置项是否合法
func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}

	return nil
}

// 根据索引信息获取 value
func (db *DB) getValueByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {
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

// 解析 key, 提取真实 key 和 seq 事务前缀
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
