package xixi_kv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/XiXi-2024/xixi-kv/datafile"
	"github.com/XiXi-2024/xixi-kv/fio"
	"github.com/XiXi-2024/xixi-kv/index"
	"github.com/XiXi-2024/xixi-kv/utils"
	"github.com/gofrs/flock"
	"github.com/valyala/bytebufferpool"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DB bitcask存储引擎客户端
type DB struct {
	options         Options // 用户配置项
	mu              *sync.RWMutex
	activeFile      *datafile.DataFile            // 当前活跃文件, 允许读写
	olderFiles      map[uint32]*datafile.DataFile // 旧数据文件, 只读
	index           index.Indexer                 // 内存索引
	seqNo           uint64                        // 事务id
	isMerging       bool                          // merge 执行状态标识
	logRecordHeader []byte                        // LogRecord 头部复用缓冲区
	fileLock        *flock.Flock                  // 文件锁
	bytesWrite      uint                          // 自上次持久化后累计写入数据量, 单位字节
	reclaimSize     int64                         // 无效数据量, 单位字节
	totalSize       int64                         // 数据文件总数据量, 单位字节
	closedChan      chan struct{}                 // 用于控制后台持久化协程关闭的通道
}

// Stat 实时统计信息
// todo 扩展点：后续进行维护和利用
type Stat struct {
	KeyNum          uint  // 当前 key 的数量
	DataFileNum     uint  // 当前数据文件数量
	ReclaimableSize int64 // 当前 merge 可回收的数据量, 单位字节
	DiskSize        int64 // 数据目录的磁盘占用空间大小
}

// Stat 获取当前时刻数据库统计信息
func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var dataFileCount = uint(len(db.olderFiles))
	if db.activeFile != nil {
		dataFileCount += 1
	}

	return &Stat{
		KeyNum:          uint(db.index.Size()),
		DataFileNum:     dataFileCount,
		ReclaimableSize: db.reclaimSize,
		DiskSize:        db.totalSize,
	}
}

// Open 客户端初始化
func Open(options Options) (*DB, error) {
	// 校验配置项
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// 数据目录不存在则创建
	if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
		return nil, err
	}

	// 尝试获取文件锁
	// 通过文件锁确保多进程下同一数据目录的 DB 实例唯一
	fileLock := flock.New(filepath.Join(options.DirPath, datafile.FileLockSuffix))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	syncWrites := options.SyncStrategy == Always
	// 初始化 DB 实例
	db := &DB{
		options:         options,
		mu:              new(sync.RWMutex),
		olderFiles:      make(map[uint32]*datafile.DataFile),
		index:           index.NewIndexer(options.IndexType, options.DirPath, syncWrites),
		logRecordHeader: make([]byte, datafile.MaxChunkHeaderSize),
		fileLock:        fileLock,
		closedChan:      make(chan struct{}),
	}

	// 尝试加载 merge 临时目录中的数据文件
	// 当 nonMergeFileId == 0 时可表示 merge 失败, 否则成功
	nonMergeFileId, err := db.loadMergeFiles()
	if err != nil {
		return nil, err
	}

	// 加载数据目录中的数据文件
	files, err := db.loadDataFiles()
	if err != nil {
		return nil, err
	}

	// 如果 merge 成功, 尝试使用 hint 文件快速加载索引
	if nonMergeFileId > 0 {
		maxFileId, err := db.loadIndexFromHintFile()
		if err != nil {
			return nil, err
		}
		nonMergeFileId = min(maxFileId, nonMergeFileId)
	}

	// 加载索引
	if err := db.loadIndexFromDataFiles(files, nonMergeFileId); err != nil {
		return nil, err
	}

	if db.options.EnableBackgroundMerge {
		go func() {
			var flushes uint = 0
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					if flushes == db.bytesWrite {
						continue
					}
					if err := db.Merge(); err != nil {
						// 记录错误日志
						fmt.Printf("failed to merge db: %v\n", err)
					}
					flushes = db.bytesWrite
				case <-db.closedChan:
					return
				}
			}
		}()
	}

	return db, nil
}

// Backup 数据库备份
func (db *DB) Backup(dir string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	// 将数据目录中的数据文件拷贝到指定目录中
	return utils.CopyDir(db.options.DirPath, dir, []string{datafile.FileLockSuffix})
}

// Put 新增元素
func (db *DB) Put(key []byte, value []byte) error {
	// 校验 key 是否为 nil
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造日志记录实例
	logRecord := &datafile.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  datafile.LogRecordNormal,
	}

	// 将日志记录追加到当前活跃文件
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// 更新索引, 并维护无效数据量
	if oldPos := db.index.Put(key, pos); oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}

	return nil
}

// Get 根据 key 读取数据
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	// 校验 key 是否为 nil
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
	// 校验 key 是否为 nil
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	// 构造 LogRecord 设置删除状态, 作为墓碑值追加到数据文件中
	logRecord := &datafile.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: datafile.LogRecordDeleted,
	}
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	// 墓碑值本身可视为无效数据
	db.reclaimSize += int64(pos.Size)

	// 更新索引信息
	oldPos, ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	if oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
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

// Fold 对数据库所有项执行自定义操作, 项改变不会同步数据库
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// 利用索引迭代器进行遍历
	iterator := db.index.Iterator(false)
	// 使用完成后必须关闭, 否则可能导致 B+ 树索引的读写事务互斥阻塞
	defer iterator.Close()
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}

		// 将遍历的每项交给传入函数处理
		if !fn(iterator.Key(), value) {
			// 函数返回 false 时终止遍历
			break
		}
	}
	return nil
}

// Close 关闭数据库
func (db *DB) Close() error {
	// 释放文件锁
	defer func() {
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("failed to unlock the directory, %v", err))
		}
	}()

	// 关闭后台 merge 协程
	if db.options.EnableBackgroundMerge && db.closedChan != nil {
		// 安全关闭
		select {
		case <-db.closedChan:
		default:
			close(db.closedChan)
		}
	}

	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	// B+树索引实例实际是 DB 实例需要同步关闭, 否则下次重复打开导致报错
	if err := db.index.Close(); err != nil {
		return err
	}

	// 关闭当前活跃文件, 自动持久化
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	// 关闭旧的数据文件
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	// 辅助 GC
	db.olderFiles = nil

	return nil
}

// Sync 数据持久化
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	// 仅持久化当前活跃文件
	return db.activeFile.Sync()
}

// 将日志记录追加到当前活跃文件, 加锁
func (db *DB) appendLogRecordWithLock(logRecord *datafile.LogRecord) (*datafile.DataPos, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.appendLogRecord(logRecord)
}

// 将日志记录追加到当前活跃文件
func (db *DB) appendLogRecord(logRecord *datafile.LogRecord) (*datafile.DataPos, error) {
	// 如果数据库为空, 先创建数据文件并设置为活跃文件
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 活跃文件剩余空间不足, 新建数据文件作为新的活跃文件
	if db.activeFile.Size() > db.options.DataFileSize {
		if err := db.sync(); err != nil {
			return nil, err
		}
	}

	pos, err := db.activeFile.WriteLogRecord(logRecord, db.logRecordHeader)
	if err != nil {
		return nil, err
	}
	// 维护总数据量
	db.totalSize += int64(pos.Size)
	// 维护累计写入数据量
	db.bytesWrite += uint(pos.Size)

	// 执行配置的持久化策略
	syncStrategy := db.options.SyncStrategy
	if syncStrategy == Always || (syncStrategy == Threshold && db.bytesWrite >= db.options.BytesPerSync) {
		// 执行持久化操作
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		// 清空累计值
		db.bytesWrite = 0
	}

	return pos, nil
}

// 持久化并设置新活跃文件
func (db *DB) sync() error {
	// 持久化原活跃文件
	if err := db.activeFile.Sync(); err != nil {
		db.bytesWrite = 0
		return err
	}

	// 将原活跃文件转换为旧数据文件
	db.olderFiles[db.activeFile.ID] = db.activeFile

	// 设置新的活跃文件
	if err := db.setActiveDataFile(); err != nil {
		return err
	}

	return nil
}

// 创建并设置新活跃文件
func (db *DB) setActiveDataFile() error {
	// 通过原活跃文件id自增获取新的文件id
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.ID + 1
	}
	// 创建并打开新的数据文件
	dataFile, err := datafile.OpenFile(db.options.DirPath, initialFileId, datafile.DataFileSuffix, db.options.FileIOType)
	if err != nil {
		return err
	}
	// 设置为新活跃文件
	db.activeFile = dataFile
	return nil
}

// 从磁盘中加载数据文件
func (db *DB) loadDataFiles() ([]uint32, error) {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return nil, err
	}
	fileIds := make([]uint32, 0, len(dirEntries))
	// 遍历目录, 筛取数据文件
	for _, entry := range dirEntries {
		if !strings.HasSuffix(entry.Name(), datafile.DataFileSuffix) {
			continue
		}
		// 解析文件名称
		splitNames := strings.Split(entry.Name(), ".")
		fileId, err := strconv.Atoi(splitNames[0])
		// 文件名称不合法
		if err != nil {
			return nil, ErrDataDirectoryCorrupted
		}
		fileIds = append(fileIds, uint32(fileId))
	}

	// 按文件 id 从小到大加载, 保证最终得到最新数据
	// 由于 ReadDir 方法底层已按文件名进行排序, 按顺序遍历得到的文件 id 已有序
	for i, fid := range fileIds {
		dataFile, err := datafile.OpenFile(db.options.DirPath, fid, datafile.DataFileSuffix, db.options.FileIOType)
		if err != nil {
			return nil, err
		}
		// id 最大的文件视为最新文件, 作为活跃文件
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.olderFiles[fid] = dataFile
		}
	}

	return fileIds, nil
}

// 从数据文件中加载索引
func (db *DB) loadIndexFromDataFiles(fileIds []uint32, nonMergeFileId uint32) error {
	// 数据库为空
	if len(fileIds) == 0 {
		return nil
	}

	// 更新索引逻辑封装为局部函数实现复用
	updateIndex := func(key []byte, typ datafile.LogRecordStatus, pos *datafile.DataPos) {
		// 维护总数据量
		db.totalSize += int64(pos.Size)

		var oldPos *datafile.DataPos
		// 发现墓碑值同样删除对应的索引信息
		if typ == datafile.LogRecordDeleted {
			oldPos, _ = db.index.Delete(key)
			db.reclaimSize += int64(pos.Size)
		} else {
			oldPos = db.index.Put(key, pos)
		}
		if oldPos != nil {
			db.reclaimSize += int64(oldPos.Size)
		}
	}

	// 属于事务提交的记录的相关暂存数据
	transactionRecords := make(map[uint64][]*datafile.TransactionRecords)
	// 临时事务序列号, 更新获取最大值
	var currentSeqNo uint64 = nonTransactionSeqNo

	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)

	// 从小到大遍历数据文件 id 顺序更新索引, 保证最终索引记录最新数据信息
	for _, fileId := range fileIds {
		// 已通过 hint 文件加载, 无需重复加载
		if fileId < nonMergeFileId {
			continue
		}
		// 获取文件 id 对应的 DataFile 实例
		var dataFile *datafile.DataFile
		if fileId == db.activeFile.ID {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}
		reader := dataFile.NewReader()
		for {
			logRecord, pos, err := reader.NextLogRecord()
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			// 解析 key, 提取真实 key 和 seq 事务前缀
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			// 判断当前日志记录是否属于事务提交
			if seqNo == nonTransactionSeqNo {
				// 日志记录属于非事务提交, 直接更新索引
				// 索引存放的 key 是真实 key
				updateIndex(realKey, logRecord.Type, pos)
			} else {
				// 日志记录属于事务提交
				// 读取到带事务完成标识的记录时再统一更新索引
				if logRecord.Type == datafile.LogRecordTxnFinished {
					// 更新相同事务 id 的所有数据对应的索引信息
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					logRecord.Key = realKey
					// 暂存用于后续更新索引的相关数据
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &datafile.TransactionRecords{
						Record: logRecord,
						Pos:    pos,
					})
				}
			}

			// 顺便更新序列号, 从而获取最大序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}
		}
	}

	// 更新事务 id, 确保后续自增获取的新事务 id 唯一
	db.seqNo = currentSeqNo

	return nil
}

// 配置项校验
// todo 优化点：完善校验, 采用责任链模式重构
func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database datafile file size must be greater than 0")
	}
	if options.DataFileMergeRatio < 0 || options.DataFileMergeRatio > 1 {
		return errors.New("invalid merge ratio, must between 0 and 1")
	}
	if options.FileIOType == fio.MemoryMap && options.DataFileSize > 512*1024*1024 {
		return errors.New("memory map datafile size should not exceed 512MB")
	}
	if options.BytesPerSync > 16*1024*1024 {
		return errors.New("BytesPerSync should not exceed 16MB")
	}
	if options.SyncStrategy == Threshold && options.BytesPerSync == 0 {
		return errors.New("SyncStrategy should not never be 0")
	}
	return nil
}

// 根据索引信息获取 value
func (db *DB) getValueByPosition(logRecordPos *datafile.DataPos) ([]byte, error) {
	// 根据文件 id 找到对应的数据文件
	var dataFile *datafile.DataFile
	if db.activeFile.ID == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// 根据偏移读取对应的数据
	logRecord, err := dataFile.ReadLogRecord(logRecordPos.BlockID, logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	// 判断是否已被删除
	// 当索引删除失败时, 可能导致索引非 nil 而日志记录标记已删除的情况
	if logRecord.Type == datafile.LogRecordDeleted {
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
