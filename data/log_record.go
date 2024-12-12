package data

// LogRecordPos 数据内存索引 描述数据在磁盘的位置
type LogRecordPos struct {
	Fid    uint32 // 文件id 标志数据所在文件
	Offset int64  // 数据偏移量 标志数据在文件中的位置
}
