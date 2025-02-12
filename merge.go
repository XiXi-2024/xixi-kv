package xixi_bitcask_kv

import (
	"github.com/XiXi-2024/xixi-bitcask-kv/data"
	"github.com/XiXi-2024/xixi-bitcask-kv/utils"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

const (
	// merge临时目录名称后缀
	mergeDirName = "-merge"

	// merge完成标识文件中的未参与 merge 的最近文件key
	mergeFinishedKey = "merge.finished"
)

// Merge 立即执行 Merge 过程
// todo 扩展点：新增定时任务和清除策略配置项, 监控数据状态, 进行自动清理
// todo 优化点：使用性能更高的 merge 方法
func (db *DB) Merge() error {
	// 校验数据是否为空
	if db.activeFile == nil {
		return nil
	}

	// 校验是否满足 merge 条件
	if err := db.mergeCheck(); err != nil {
		return err
	}

	// 方法仅部分逻辑需加锁, 不应 defer
	db.mu.Lock()

	// 更新 merge 状态
	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	// 当前活跃文件同样加入参与 merge 的集合
	if err := db.sync(); err != nil {
		db.mu.Unlock()
		return err
	}

	// 记录未参与 merge 的最近文件 id
	nonMergeFileId := db.activeFile.FileId

	// 存放参与 merge 的数据文件
	var mergeFiles []*data.DataFile
	// 将所有旧数据文件加入参与 merge 的集合
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}

	// 由于采用操作临时目录方式, 故允许提前释放锁
	db.mu.Unlock()

	// 获取 merge 临时目录路径
	mergePath := db.getMergePath()
	// 如果存在上次 merge 的残留目录, 将其删除
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}

	// 新建 merge 临时目录
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	// 创建新临时 DB 实例操作临时目录, 从而避免并发冲突
	// todo 优化点：直接创建 DB 实例？
	mergeOptions := db.options
	mergeOptions.DirPath = mergePath
	mergeOptions.SyncStrategy = No // 加快 merge 速度
	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}

	// 在 merge 临时目录创建并打开 hint 索引文件
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}

	// 执行 merge
	// 依次读取每个数据文件, 解析得到日志记录并写入新 merge 目录
	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			// 解析得到真实key
			realKey, _ := parseLogRecordKey(logRecord.Key)
			logRecordPos := db.index.Get(realKey)
			// 与内存中的最新数据比较, 判断是否为有效数据
			if logRecordPos != nil &&
				logRecordPos.Fid == dataFile.FileId && logRecordPos.Offset == offset {
				// 对于有效数据, 无论是否携带事务标记都表示事务已成功, 直接清除
				logRecord.Key = logRecordKeyWithSeq(realKey, nonTransactionSeqNo)
				// 将数据重写到 merge 临时目录中
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}
				// merge的过程中顺便将构建索引所需信息写入 Hint 文件中, 用于后续重启时加速构建索引
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}
			}
			offset += size
		}
	}

	// 将重写的数据文件和 hint 文件持久化
	if err := hintFile.Close(); err != nil {
		return err
	}
	if err := mergeDB.Close(); err != nil {
		return err
	}

	// 在 merge 临时目录创建并打开 merge 完成标识文件
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	// 向文件写入未参与该次 merge 的最近数据文件id
	mergeFinRecord := &data.LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
	}
	encRecord, _ := data.EncodeLogRecord(mergeFinRecord)
	if err := mergeFinishedFile.Write(encRecord); err != nil {
		return err
	}
	if err := mergeFinishedFile.Close(); err != nil {
		return err
	}

	return nil
}

// merge 执行时机校验
func (db *DB) mergeCheck() error {
	// 校验是否正在进行 merge
	// 由于 merge 过程中会提前释放锁, 故存在同时尝试进行 merge 的情况
	if db.isMerging {
		return ErrMergeIsProgress
	}

	// 校验无效数据占比是否达到阈值
	if float32(db.reclaimSize)/float32(db.totalSize) < db.options.DataFileMergeRatio {
		return ErrMergeRatioUnreached
	}

	// 校验数据目录所在磁盘剩余空间是否能容纳 merge 后的数据量
	availableDiskSize, err := utils.AvailableDiskSize(db.options.DirPath)
	if err != nil {
		return err
	}
	if uint64(db.totalSize-db.reclaimSize) >= availableDiskSize {
		return ErrNoEnoughSpaceForMerge
	}

	return nil
}

// 获取 merge 临时目录路径, 与数据目录同级
func (db *DB) getMergePath() string {
	// 获取数据目录的父目录路径
	dir := filepath.Dir(filepath.Clean(db.options.DirPath))
	// 获取数据目录名称
	base := filepath.Base(db.options.DirPath)
	return filepath.Join(dir, base+mergeDirName)
}

// 尝试加载 merge 临时目录
func (db *DB) loadMergeFiles() (uint32, error) {
	mergePath := db.getMergePath()
	// 未进行过 merge
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return 0, nil
	}

	defer func() {
		// 加载完成后删除临时目录
		_ = os.RemoveAll(mergePath)
	}()

	// 读取目录中所有文件
	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return 0, err
	}

	// 是否存在 merge 完成文件标识
	var mergeFinished bool
	// 参与 merge 的数据文件、Hint文件的名称集合
	var mergeFileNames []string
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
		}
		// 过滤事务 id 文件, 其中包含的事务 id 非最新, 加载无意义
		if entry.Name() == data.SeqNoFileName {
			continue
		}
		// 过滤文件锁文件
		if entry.Name() == fileLockName {
			continue
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}

	// merge 未完成
	if !mergeFinished {
		return 0, nil
	}

	// 从标识文件中取出未参与 merge 的最近数据文件 id
	nonMergeFileId, err := db.getNonMergeFileId()
	if err != nil {
		return 0, err
	}

	// 数据目录中删除参与 merge 的旧数据文件
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		// 获取完整数据文件名称
		fileName := data.GetDataFileName(db.options.DirPath, fileId)
		// 如果存在则删除
		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return 0, err
			}
		}
	}

	// 将重写的数据文件和 hint 文件移动到数据目录中
	for _, fileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, fileName)
		destPath := filepath.Join(db.options.DirPath, fileName)
		if err := os.Rename(srcPath, destPath); err != nil {
			return 0, err
		}
	}

	return nonMergeFileId, nil
}

// 获取 merge 完成标识文件中保存的未参与 merge 的最近数据文件id
func (db *DB) getNonMergeFileId() (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(db.getMergePath())
	if err != nil {
		return 0, err
	}
	record, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	nonMergeFileId, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}
	return uint32(nonMergeFileId), nil
}

// 尝试通过 hint 文件加载索引
func (db *DB) loadIndexFromHintFile() (uint32, error) {
	// 打开 hint 文件
	// 调用该方法说明 merge 必然成功, hint 文件必然存在
	// 如果手动删去 hint 文件, 会自动创建新的空文件进行读取
	hintFile, err := data.OpenHintFile(db.options.DirPath)
	if err != nil {
		return 0, err
	}

	// 实际读取到的最大数据文件 id
	// 避免 hint 文件被删除导致无法加载的情况
	var maxFileId uint32 = 0

	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}

		// 快速加载索引
		pos := data.DecodeLogRecordPos(logRecord.Value)
		db.index.Put(logRecord.Key, pos)
		offset += size

		// 统计总数据量
		db.totalSize += int64(pos.Size)

		maxFileId = max(maxFileId, pos.Fid)
	}

	return maxFileId, nil
}
