package datatype

import (
	"encoding/binary"
	"errors"
	bitcask "github.com/XiXi-2024/xixi-bitcask-kv"
	"time"
)

var (
	ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)

// DataType 数据类型枚举
type DataType = byte

const (
	String DataType = iota
	Hash
	Set
	List
	ZSet
)

// DataTypeService 数据类型服务
type DataTypeService struct {
	db *bitcask.DB
}

// NewDataTypeService 创建数据类型服务实例
func NewDataTypeService(options bitcask.Options) (*DataTypeService, error) {
	db, err := bitcask.Open(options)
	if err != nil {
		return nil, err
	}

	return &DataTypeService{db: db}, nil
}

// ========================= String 数据类型 ========================
// todo 后续接口参数重构为 string 类型
func (dts *DataTypeService) Set(key, value []byte, ttl time.Duration) error {
	if value == nil {
		return nil
	}

	// 对 value 编码
	buf := make([]byte, binary.MaxVarintLen64+1)
	// 设置数据类型
	buf[0] = String
	// 根据生效时长计算过期时间
	var expire int64 = 0
	var index = 1
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buf[index:], expire)

	// 与真实 value 合并编码
	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)

	// 底层调用引擎接口写入数据
	return dts.db.Put(key, encValue)
}

func (dts *DataTypeService) Get(key []byte) ([]byte, error) {
	encValue, err := dts.db.Get(key)
	if err != nil {
		return nil, err
	}

	// 对 value 字节数组解码
	dataType := encValue[0]
	// 判断类型是否正确
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}

	var index = 1
	expire, n := binary.Varint(encValue[index:])
	index += n
	// 判断 key 是否过期
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}

	return encValue[index:], nil
}
