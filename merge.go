package xixi_kv

import (
	"github.com/XiXi-2024/xixi-kv/datafile"
	"github.com/XiXi-2024/xixi-kv/fio"
	"github.com/XiXi-2024/xixi-kv/utils"
	"io"
	"os"
	"path/filepath"
)

// merge临时目录名称后缀
const mergeDirName = "-merge"

// Merge 立即执行 Merge 过程
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
	nonMergeFileId := db.activeFile.ID

	// 存放参与 merge 的数据文件
	var mergeFiles []*datafile.DataFile
	// 将所有旧数据文件加入参与 merge 的集合
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}

	// 由于采用操作临时目录方式, 故允许提前释放锁
	db.mu.Unlock()

	// 获取 merge 临时目录路径
	mergePath := db.mergePath()
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

	// 创建新临时 DB 实例操作临时目录, 避免并发冲突
	// 实际仅需要保证 appendLogRecord 方法的执行, 可简化初始化流程
	mergeOptions := db.options
	mergeOptions.DirPath = mergePath
	mergeOptions.SyncStrategy = No // 加快 merge 速度
	mergeDB := &DB{
		options:         mergeOptions,
		olderFiles:      make(map[uint32]*datafile.DataFile),
		logRecordHeader: make([]byte, datafile.MaxLogRecordHeaderSize),
	}
	if err := mergeDB.setActiveFile(); err != nil {
		return nil
	}

	// 在 merge 临时目录创建并打开 hint 索引文件
	hintFile, err := datafile.OpenFile(mergePath, 0, datafile.HintFileSuffix, db.options.FileIOType)
	if err != nil {
		return err
	}
	if db.hintPos == nil {
		db.hintPos = make([]byte, datafile.MaxLogRecordPosSize)
	}

	// 执行 merge
	// 依次读取每个数据文件, 解析得到日志记录并写入新 merge 目录
	for _, dataFile := range mergeFiles {
		reader := dataFile.NewReader()
		for {
			logRecord, logRecordPos, err := reader.NextLogRecord()
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			// 比较内存中索引的最新数据, 判断是否为有效数据
			pos := db.index.Get(logRecord.Key)
			if pos != nil && pos.Fid == dataFile.ID &&
				pos.Offset == logRecordPos.Offset && pos.BlockID == logRecordPos.BlockID {
				// 将数据重写到 merge 临时目录中
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}
				// merge的过程中顺便将构建索引所需信息写入 Hint 文件中, 用于后续重启时加速构建索引
				if err := hintFile.WriteHintRecord(logRecord.Key, db.hintPos, pos); err != nil {
					return err
				}
			}
		}
	}

	// 将重写的数据文件和 hint 文件持久化
	if err := hintFile.Close(); err != nil {
		return err
	}
	if mergeDB.activeFile != nil {
		if err := mergeDB.activeFile.Close(); err != nil {
			return err
		}
	}

	// 在 merge 临时目录创建并打开 merge 完成标识文件
	mergeFinishedFile, err := datafile.OpenFile(mergePath, 0,
		datafile.MergeFinishedFileSuffix, db.options.FileIOType)
	if err != nil {
		return err
	}
	// 向文件写入未参与该次 merge 的最近数据文件id
	if err := mergeFinishedFile.WriteMergeFinRecord(nonMergeFileId); err != nil {
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
	// 同时总数据量需达到 256MB, 避免小的无效数据过于影响比值
	if db.totalSize > 256*1024*1024 &&
		float32(db.reclaimSize)/float32(db.totalSize) < db.options.DataFileMergeRatio {
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
func (db *DB) mergePath() string {
	// 获取数据目录的父目录路径
	dir := filepath.Dir(filepath.Clean(db.options.DirPath))
	// 获取数据目录名称
	base := filepath.Base(db.options.DirPath)
	return filepath.Join(dir, base+mergeDirName)
}

// 尝试加载 merge 临时目录
func (db *DB) loadMergeFiles() (uint32, error) {
	mergePath := db.mergePath()
	// 如果 merge 目录不存在或其他错误则执行正常加载流程
	if _, err := os.Stat(mergePath); err != nil {
		return 0, nil
	}

	// 尝试从标识文件中取出未参与 merge 的最近数据文件 id
	mergeID := db.getNonMergeFileID(mergePath)
	// 标识文件不存在同样执行正常加载流程
	if mergeID == 0 {
		return 0, nil
	}

	defer func() {
		// 加载完成后删除 merge 目录
		_ = os.RemoveAll(mergePath)
	}()

	// 处理经过重写的数据文件, 处理中途失败需返回错误
	for fileID := uint32(0); fileID < mergeID; fileID++ {
		// 删除原数据文件
		destName := datafile.GetFileName(db.options.DirPath, fileID, datafile.DataFileSuffix)
		var exist bool
		if _, err := os.Stat(destName); err == nil {
			if err = os.Remove(destName); err != nil {
				return 0, err
			}
			exist = true
		}
		// 将重写的数据文件移动到数据目录中
		srcFile := datafile.GetFileName(mergePath, fileID, datafile.DataFileSuffix)
		if _, err := os.Stat(srcFile); err != nil {
			// 如果原数据文件不存在, 则允许重写文件不存在
			if !exist && os.IsNotExist(err) {
				continue
			}
			return 0, err
		}
		if err := os.Rename(srcFile, destName); err != nil {
			return 0, err
		}
	}

	// 移动对应的 hint 文件, 移动失败应当返回错误
	srcHintFile := datafile.GetFileName(mergePath, 0, datafile.HintFileSuffix)
	destHintFile := datafile.GetFileName(db.options.DirPath, 0, datafile.HintFileSuffix)
	if _, err := os.Stat(srcHintFile); err != nil {
		return 0, err
	}
	if err := os.Rename(srcHintFile, destHintFile); err != nil {
		return 0, err
	}

	return mergeID, nil
}

// 获取 merge 完成标识文件中保存的未参与 merge 的最近数据文件id
// 返回 0 表示读取失败
func (db *DB) getNonMergeFileID(dirPath string) datafile.FileID {
	mergeFinishedFile, err := datafile.OpenFile(dirPath, 0, datafile.MergeFinishedFileSuffix, fio.StandardFIO)
	if err != nil {
		return 0
	}
	return mergeFinishedFile.ReadMergeFinRecord()
}

// 尝试通过 hint 文件加载索引
func (db *DB) loadIndexFromHintFile() (uint32, error) {
	// 打开 hint 文件
	// 调用该方法说明 merge 必然成功, hint 文件必然存在
	// 如果手动删去 hint 文件, 会自动创建新的空文件进行读取
	hintFile, err := datafile.OpenFile(db.options.DirPath, 0, datafile.HintFileSuffix, db.options.FileIOType)
	if err != nil {
		return 0, err
	}

	// 实际读取到的最大数据文件 id
	// 避免 hint 文件被删除导致无法加载的情况
	var maxFileId datafile.FileID
	reader := hintFile.NewReader()
	for {
		key, pos, err := reader.NextHintRecord()
		if err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}
		db.index.Put(key, pos)
		db.totalSize += int64(pos.Size)

		maxFileId = max(maxFileId, pos.Fid)
	}

	return maxFileId, nil
}
