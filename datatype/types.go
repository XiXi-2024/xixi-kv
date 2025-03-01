package datatype

import (
	"encoding/binary"
	"errors"
	bitcask "github.com/XiXi-2024/xixi-kv"
	"github.com/XiXi-2024/xixi-kv/utils"
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

func (dts *DataTypeService) Close() error {
	return dts.db.Close()
}

// ========================= String 数据类型 ========================
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
	wb := dts.db.NewBatch(bitcask.DefaultBatchOptions)
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
		wb := dts.db.NewBatch(bitcask.DefaultBatchOptions)
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
		if dt == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
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
		wb := dts.db.NewBatch(bitcask.DefaultBatchOptions)
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
	wb := dts.db.NewBatch(bitcask.DefaultBatchOptions)
	meta.size--
	_ = wb.Put(key, meta.encode())
	_ = wb.Delete(sk.encode())
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// ======================= List 数据结构 =======================

// LPush 左侧入队
func (dts *DataTypeService) LPush(key, element []byte) (uint32, error) {
	return dts.pushInner(key, element, true)
}

// RPush 右侧入队
func (dts *DataTypeService) RPush(key, element []byte) (uint32, error) {
	return dts.pushInner(key, element, false)
}

// LPop 左侧出队
func (dts *DataTypeService) LPop(key []byte) ([]byte, error) {
	return dts.popInner(key, true)
}

// RPop 右侧出队
func (dts *DataTypeService) RPop(key []byte) ([]byte, error) {
	return dts.popInner(key, false)
}

// 入队操作
func (dts *DataTypeService) pushInner(key, element []byte, isLeft bool) (uint32, error) {
	// 查询元数据信息
	meta, err := dts.findMetadata(key, List)
	if err != nil {
		return 0, err
	}

	// 构造数据部分 key 实例
	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}
	// 确定入队元素的下标
	if isLeft {
		lk.index = meta.head - 1
	} else {
		lk.index = meta.tail
	}

	// 涉及更新数据和元数据两步操作, 需保证原子性
	wb := dts.db.NewBatch(bitcask.DefaultBatchOptions)
	meta.size++
	if isLeft {
		meta.head--
	} else {
		meta.tail++
	}
	_ = wb.Put(key, meta.encode())
	_ = wb.Put(lk.encode(), element)
	if err = wb.Commit(); err != nil {
		return 0, err
	}

	return meta.size, nil
}

// 出队操作
func (dts *DataTypeService) popInner(key []byte, isLeft bool) ([]byte, error) {
	// 查询元数据信息
	meta, err := dts.findMetadata(key, List)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	// 构造数据部分 key 实例
	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}
	// 确定出队元素的下标
	if isLeft {
		lk.index = meta.head
	} else {
		lk.index = meta.tail - 1
	}

	// 获取出队元素进行返回
	element, err := dts.db.Get(lk.encode())
	if err != nil {
		return nil, err
	}

	// 仅更新元数据即可
	meta.size--
	if isLeft {
		meta.head++
	} else {
		meta.tail--
	}
	if err = dts.db.Put(key, meta.encode()); err != nil {
		return nil, err
	}

	return element, nil
}

// ======================= ZSet 数据结构 =======================

// ZAdd 集合新增元素
func (dts *DataTypeService) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	// 查询元数据信息
	meta, err := dts.findMetadata(key, ZSet)
	if err != nil {
		return false, err
	}

	// 构造数据部分 key 实例
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		score:   score,
		member:  member,
	}

	var exist = true
	// 查询是否已存在
	value, err := dts.db.Get(zk.encodeWithMember())
	if err != nil && err != bitcask.ErrKeyNotFound {
		return false, err
	}
	if err == bitcask.ErrKeyNotFound {
		exist = false
	}
	if exist {
		if score == utils.FloatFromBytes(value) {
			return false, nil
		}
	}

	// 涉及更新数据和元数据两步操作, 需保证原子性
	wb := dts.db.NewBatch(bitcask.DefaultBatchOptions)
	if !exist {
		// 元素不存在进行新增, 更新元数据
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	if exist {
		// 元素已存在时需进行覆盖, 删除原元素
		oldKey := &zsetInternalKey{
			key:     key,
			version: meta.version,
			member:  member,
			score:   utils.FloatFromBytes(value),
		}
		_ = wb.Delete(oldKey.encodeWithScore())
	}
	// 新增元素
	_ = wb.Put(zk.encodeWithMember(), utils.Float64ToBytes(score))
	_ = wb.Put(zk.encodeWithScore(), nil)
	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil
}

// ZScore 查询指定元素的 score 值
func (dts *DataTypeService) ZScore(key []byte, member []byte) (float64, error) {
	// 查询元数据信息
	meta, err := dts.findMetadata(key, ZSet)
	if err != nil {
		return -1, err
	}
	if meta.size == 0 {
		return -1, nil
	}

	// 构造数据部分 key 实例
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	value, err := dts.db.Get(zk.encodeWithMember())
	if err != nil {
		return -1, err
	}

	return utils.FloatFromBytes(value), nil
}
