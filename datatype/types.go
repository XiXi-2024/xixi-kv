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

// 数据类型枚举
type dataType = byte

const (
	String dataType = iota
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
// todo 后续拆分到各个文件
// todo 扩展点：后续新增其它命令
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

// ========================= Hash 数据类型 ========================

func (dts *DataTypeService) HSet(key, field, value []byte) (bool, error) {
	// 获取元数据
	meta, err := dts.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	// 构造数据部分key实例并编码
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encKey := hk.encode()

	// 判断数据部分key是否存在
	var exist = true
	if _, err = dts.db.Get(encKey); err == bitcask.ErrKeyNotFound {
		exist = false
	}

	// 涉及更新数据和元数据两步操作, 需保证原子性
	wb := dts.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	// 不存在则更新元数据
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	// 不存在则新增, 已存在则覆盖
	_ = wb.Put(encKey, value)

	if err = wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

func (dts *DataTypeService) HGet(key, field []byte) ([]byte, error) {
	// 查询元数据信息
	meta, err := dts.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}
	// 无数据直接返回
	if meta.size == 0 {
		return nil, nil
	}
	// 构造数据部分key并查询数据返回
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	return dts.db.Get(hk.encode())
}

func (dts *DataTypeService) HDel(key, field []byte) (bool, error) {
	// 查询元数据信息
	meta, err := dts.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	// 无数据直接返回
	if meta.size == 0 {
		return false, nil
	}
	// 构造数据部分key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encKey := hk.encode()

	var exist = true
	if _, err = dts.db.Get(encKey); err == bitcask.ErrKeyNotFound {
		exist = false
	}

	// 当field存在时进行删除
	if exist {
		// 涉及更新数据和元数据两步操作, 需保证原子性
		wb := dts.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
		meta.size--
		_ = wb.Put(key, meta.encode())
		_ = wb.Delete(encKey)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}

	return exist, nil
}

// 根据key获取元数据
func (dts *DataTypeService) findMetadata(key []byte, dt dataType) (*metadata, error) {
	metaBuf, err := dts.db.Get(key)
	if err != nil && err != bitcask.ErrKeyNotFound {
		return nil, err
	}

	var meta *metadata
	// key 存在标识
	var exist = true
	if err == bitcask.ErrKeyNotFound {
		exist = false
	} else {
		// key 存在, 进行解码
		meta = decodeMetadata(metaBuf)
		// 判断数据类型是否正确
		if meta.dataType != dt {
			return nil, ErrWrongTypeOperation
		}
		// 判断是否过期
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false // 过期仍视为不存在
		}
	}

	// 不存在则创建
	if !exist {
		meta = &metadata{
			dataType: dt,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
	}
	if dt == List {
		meta.head = initialListMark
		meta.tail = initialListMark
	}

	return meta, nil
}

// ========================= Set 数据类型 ========================

// SAdd 新增 member 到集合
func (dts *DataTypeService) SAdd(key, member []byte) (bool, error) {
	meta, err := dts.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	// 构造数据部分 key 实例
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	var ok bool
	// 当 member 不存在时新增
	if _, err = dts.db.Get(sk.encode()); err == bitcask.ErrKeyNotFound {
		// 涉及更新数据和元数据两步操作, 需保证原子性
		wb := dts.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
		meta.size++
		_ = wb.Put(key, meta.encode())
		// value 为 nil
		_ = wb.Put(sk.encode(), nil)
		if err = wb.Commit(); err != nil {
			return false, err
		}
		ok = true
	}

	return ok, nil
}

// SIsMember 判断 member 是否在集合中存在
func (dts *DataTypeService) SIsMember(key, member []byte) (bool, error) {
	meta, err := dts.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	// 构造数据部分 key 实例
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	_, err = dts.db.Get(sk.encode())
	if err != nil && err != bitcask.ErrKeyNotFound {
		return false, err
	}
	if err == bitcask.ErrKeyNotFound {
		return false, nil
	}
	return true, nil
}

// SRem 从集合中删除 member
func (dts *DataTypeService) SRem(key, member []byte) (bool, error) {
	meta, err := dts.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	// 构造数据部分 key 实例
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	if _, err = dts.db.Get(sk.encode()); err == bitcask.ErrKeyNotFound {
		return false, nil
	}

	// 涉及更新数据和元数据两步操作, 需保证原子性
	wb := dts.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	meta.size--
	_ = wb.Put(key, meta.encode())
	_ = wb.Delete(sk.encode())
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}
