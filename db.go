package xixi_bitcask_kv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/XiXi-2024/xixi-bitcask-kv/data"
	"github.com/XiXi-2024/xixi-bitcask-kv/index"
	"github.com/XiXi-2024/xixi-bitcask-kv/utils"
	"github.com/gofrs/flock"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const (
	// 事务序列号文件存放数据 Key
	seqNoKey = "seq.no"
	// 文件锁名称
	fileLockName = "flock"
)

// DB bitcask存储引擎客户端
type DB struct {
	options    Options // 用户配置项
	mu         *sync.RWMutex
	fileIds    []int                     // 数据文件 id 集合, 仅用于索引加载
	activeFile *data.DataFile            // 当前活跃文件, 允许读写
	olderFiles map[uint32]*data.DataFile // 旧数据文件, 只读
	index      index.Indexer             // 内存索引
	seqNo      uint64                    // 事务id
	isMerging  bool                      // merge 执行状态标识
	// todo 优化点：省略
	seqNoFileExists bool         // 事务序列号文件存在标识
	isInitial       bool         // 首次初始化数据目录标识
	fileLock        *flock.Flock // 文件锁
	bytesWrite      uint         // 新写入数据量, 单位字节
	reclaimSize     int64        // 无效数据量, 单位字节
}

// Stat 实时统计信息
// todo 扩展点：后续进行维护和利用
type Stat struct {
	KeyNum          uint  // 当前 key 的数量
	DataFileNum     uint  // 当前数据文件数量
	ReclaimableSize int64 // 当前 merge 可回收的数据量, 单位字节
	DiskSize        int64 // 数据目录的磁盘占用空间大小
}

// Stat 获取当前时刻数据库统计信息实例
func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var dataFiles = uint(len(db.olderFiles))
	if db.activeFile != nil {
		dataFiles += 1
	}

	dirSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get dir size : %v", err))
	}
	return &Stat{
		KeyNum:          uint(db.index.Size()),
		DataFileNum:     dataFiles,
		ReclaimableSize: db.reclaimSize,
		DiskSize:        dirSize,
	}
}

// Open 客户端初始化
func Open(options Options) (*DB, error) {
	// 校验配置项
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// 是否为首次加载标识
	var isInitial bool

	// 数据目录不存在则创建
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}
	// 数据目录为空
	if len(entries) == 0 {
		isInitial = true
	}

	// 尝试获取文件锁
	// 通过文件锁确保多进程下同一数据目录的 DB 实例唯一
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	// 初始化 DB 实例
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType, options.DirPath, options.SyncWrites),
		isInitial:  isInitial,
		fileLock:   fileLock,
	}

	// 尝试加载 merge 临时目录中的数据文件
	// merge 失败时 nonMergeFileId 为 0, 可表示该情况
	nonMergeFileId, err := db.loadMergeFiles()
	if err != nil {
		return nil, err
	}

	// 加载数据目录中的数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// 索引实现选择可持久化 B+ 树, 无需加载索引到内存
	if options.IndexType == index.BPTree {
		// 从文件中加载事务 id
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}
		// 更新活跃文件偏移量
		if db.activeFile != nil {
			size, err := db.activeFile.ReadWriter.Size()
			if err != nil {
				return nil, err
			}
			// 直接设置为当前活跃文件的大小
			db.activeFile.WriteOff = size
		}
		return db, nil
	}

	// 尝试使用 hint 文件快速加载索引
	if err := db.loadIndexFromHintFile(); err != nil {
		return nil, err
	}

	// 加载索引
	if err := db.loadIndexFromDataFiles(nonMergeFileId); err != nil {
		return nil, err
	}

	return db, nil
}

// Backup 数据库备份
func (db *DB) Backup(dir string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	// 将数据目录中的数据文件拷贝到指定目录中
	return utils.CopyDir(db.options.DirPath, dir, []string{fileLockName})
}

// Put 新增元素
func (db *DB) Put(key []byte, value []byte) error {
	// 校验 key 是否为 nil
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造日志记录实例
	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
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
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted,
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

// Fold 对数据库所有项执行自定义操作
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

	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	// B+树索引实例实际是 DB 实例需要同步关闭, 否则下次重复打开导致报错
	if err := db.index.Close(); err != nil {
		return err
	}

	// 如果选择 B+ 树索引实现, 不存在索引加载流程, 无法借此获得事务id
	// 需要在关闭数据库时将当前最新事务 id 持久化
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record := &data.LogRecord{
		Key:   []byte(seqNoKey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)),
	}
	encRecord, _ := data.EncodeLogRecord(record)
	if err := seqNoFile.Write(encRecord); err != nil {
		return err
	}
	if err := seqNoFile.Close(); err != nil {
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
func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.appendLogRecord(logRecord)
}

// 将日志记录追加到当前活跃文件
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	// 如果数据库为空, 先创建数据文件并设置为活跃文件
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 编码
	encRecord, size := data.EncodeLogRecord(logRecord)

	// 活跃文件剩余空间不足, 新建数据文件作为新的活跃文件
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		if err := db.sync(); err != nil {
			return nil, err
		}
	}

	// 记录新日志记录的起始偏移量
	writeOff := db.activeFile.WriteOff
	// 追加写入新日志记录
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	// 累加当前写入字节
	db.bytesWrite += uint(size)

	// 执行配置的持久化策略
	var needSync = db.options.SyncWrites
	if !needSync && db.options.BytesPerSync > 0 && db.bytesWrite >= db.options.BytesPerSync {
		needSync = true
	}
	if needSync {
		// 执行持久化操作
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		// 持久化后清空累计值
		db.bytesWrite = 0
	}
	// 构造内存索引信息返回
	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff, Size: uint32(size)}
	return pos, nil
}

// 持久化并设置新活跃文件
func (db *DB) sync() error {
	// 持久化原活跃文件
	if err := db.activeFile.Sync(); err != nil {
		return err
	}

	// 将原活跃文件转换为旧数据文件
	db.olderFiles[db.activeFile.FileId] = db.activeFile

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
		initialFileId = db.activeFile.FileId + 1
	}
	// 创建并打开新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId, db.options.FileIOType)
	if err != nil {
		return err
	}
	// 设置为新活跃文件
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
	// 遍历目录, 筛取数据文件
	for _, entry := range dirEntries {
		if !strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			continue
		}
		// 解析文件名称
		splitNames := strings.Split(entry.Name(), ".")
		fileId, err := strconv.Atoi(splitNames[0])
		// 文件名称不合法
		if err != nil {
			return ErrDataDirectoryCorrupted
		}
		fileIds = append(fileIds, fileId)
	}

	db.fileIds = fileIds

	// 按文件 id 从小到大加载, 保证最终得到最新数据
	// 由于 ReadDir 方法底层已按文件名进行排序, 按顺序遍历得到的文件 id 已有序
	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid), db.options.FileIOType)
		if err != nil {
			return err
		}
		// id 最大的文件视为最新文件, 作为活跃文件
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.olderFiles[uint32(fid)] = dataFile
		}
	}

	return nil
}

// 从数据文件中加载索引
func (db *DB) loadIndexFromDataFiles(nonMergeFileId uint32) error {
	// 数据库为空
	if len(db.fileIds) == 0 {
		return nil
	}

	// 更新索引逻辑封装为局部函数实现复用
	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var oldPos *data.LogRecordPos
		// 发现墓碑值同样删除对应的索引信息
		if typ == data.LogRecordDeleted {
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
	transactionRecords := make(map[uint64][]*data.TransactionRecords)
	// 临时事务序列号, 更新获取最大值
	var currentSeqNo uint64 = nonTransactionSeqNo

	// 从小到大遍历数据文件 id 顺序更新索引, 保证最终索引记录最新数据信息
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		// 已通过 hint 文件加载, 无需重复加载
		if fileId < nonMergeFileId {
			continue
		}
		// 获取文件 id 对应的 DataFile 实例
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		// 通过 DataFile 实例顺序读取文件的日志记录
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// 构建内存索引信息并保存
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset, Size: uint32(size)}

			// 解析 key, 提取真实 key 和 seq 事务前缀
			realKey, seqNo := parseLogRecordKey(logRecord.Key)

			// todo 未知：hint文件加载时未更新事务 id, 可能存在问题？
			// 判断当前日志记录是否属于事务提交
			if seqNo == nonTransactionSeqNo {
				// 日志记录属于非事务提交, 直接更新索引
				// 索引存放的 key 是真实 key
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				// 日志记录属于事务提交
				// 读取到带事务完成标识的记录时再统一更新索引
				if logRecord.Type == data.LogRecordTxnFinished {
					// 更新相同事务 id 的所有数据对应的索引信息
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

			// 顺便更新序列号, 从而获取最大序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			// 更新已读取位置偏移
			offset += size
		}

		// 当前为活跃文件时需更新文件实例的 WriteOff, 供之后追加写入
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}

	// 更新事务 id, 确保后续自增获取的新事务 id 唯一
	db.seqNo = currentSeqNo

	return nil
}

// 配置项校验
// todo 优化点：完善校验
func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}
	if options.DataFileMergeRatio < 0 || options.DataFileMergeRatio > 1 {
		return errors.New("invalid merge ratio, must between 0 and 1")
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
	// 当索引删除失败时, 可能导致索引非 nil 而日志记录标记已删除的情况
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

// 加载事务id
func (db *DB) loadSeqNo() error {
	// 判断是否存在事务id文件
	fileName := filepath.Join(db.options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}

	// 打开文件
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}

	// 读取事务id
	record, _, err := seqNoFile.ReadLogRecord(0)
	if err != nil {
		return err
	}
	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}

	db.seqNo = seqNo
	db.seqNoFileExists = true
	return nil
}
