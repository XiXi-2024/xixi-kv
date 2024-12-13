package data

// LogRecordType 日志记录状态枚举
type LogRecordType = byte

const (
	// LogRecordNormal 生效中
	LogRecordNormal LogRecordType = iota
	// LogRecordDeleted 已删除
	LogRecordDeleted
)

// LogRecord 数据文件存放的日志记录
// 因为以追加形式写入 故称为日志记录
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecordPos 数据内存索引 描述日志记录在磁盘的位置
type LogRecordPos struct {
	Fid    uint32 // 文件id 定位数据所在文件
	Offset int64  // 数据偏移量 定位数据在文件中的位置
}

// EncodeLogRecord 对 LogRecord 实例编码
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}
