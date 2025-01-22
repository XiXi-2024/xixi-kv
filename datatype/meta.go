package datatype

import (
	"encoding/binary"
	"math"
)

const (
	// 基本元数据最大长度
	maxMetadataSize = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	// List 相关元数据最大长度
	extraListMetaSize = binary.MaxVarintLen64 * 2
	// List 相关值
	initialListMark = math.MaxUint64 / 2
)

// 元数据
type metadata struct {
	dataType byte   // 数据类型
	expire   int64  // 过期时间
	version  int64  // 版本号
	size     uint32 // 数据量
	head     uint64 // List 相关
	tail     uint64 // List 相关
}

// 元数据编码
func (md *metadata) encode() []byte {
	// 计算当前元数据最大长度, 创建对应长度的字节数组
	var size = maxMetadataSize
	if md.dataType == List {
		size += extraListMetaSize
	}
	buf := make([]byte, size)

	// 按字段顺序依次编码
	buf[0] = md.dataType
	var index = 1
	index += binary.PutVarint(buf[index:], md.expire)
	index += binary.PutVarint(buf[index:], md.version)
	index += binary.PutVarint(buf[index:], int64(md.size))

	// 如果为 List 类型, 对 List 相关字段编码
	if md.dataType == List {
		index += binary.PutUvarint(buf[index:], md.head)
		index += binary.PutUvarint(buf[index:], md.tail)
	}

	return buf[:index]
}

// 元数据解码
func decodeMetadata(buf []byte) *metadata {
	dataType := buf[0]

	var index = 1
	expire, n := binary.Varint(buf[index:])
	index += n
	version, n := binary.Varint(buf[index:])
	index += n
	size, n := binary.Varint(buf[index:])
	index += n

	var head uint64 = 0
	var tail uint64 = 0
	// 如果为 List 类型, 对 List 相关字段解码
	if dataType == List {
		head, n = binary.Uvarint(buf[index:])
		index += n
		tail, _ = binary.Uvarint(buf[index:])
	}
	return &metadata{
		dataType: dataType,
		expire:   expire,
		version:  version,
		size:     uint32(size),
		head:     head,
		tail:     tail,
	}
}

// Hash类型数据部分key
type hashInternalKey struct {
	key     []byte
	version int64
	field   []byte
}

func (hk *hashInternalKey) encode() []byte {
	buf := make([]byte, len(hk.key)+len(hk.field)+8)
	// key
	var index = 0
	copy(buf[index:index+len(hk.key)], hk.key)
	index += len(hk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(hk.version))
	index += 8

	// field
	copy(buf[index:], hk.field)

	return buf
}
