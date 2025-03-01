package index

import (
	"bytes"
	"container/heap"
	"github.com/XiXi-2024/xixi-kv/datafile"
	"github.com/cespare/xxhash"
	"sync"
)

const (
	MaxCap = 1 << 10
)

// ShardedIndex 分片索引, 保证并发安全
type ShardedIndex struct {
	index     []index        // 底层索引分片
	indexLock []sync.RWMutex // 分片级别锁
	cap       int            // 分片数量
}

func NewShardedIndex(typ IndexType, shardNum int) *ShardedIndex {
	shardNum = nextPowerOfTwo(shardNum)
	shards := make([]index, shardNum)
	locks := make([]sync.RWMutex, shardNum)

	for i := 0; i < shardNum; i++ {
		shards[i] = newIndexer(typ)
	}

	return &ShardedIndex{
		index:     shards,
		indexLock: locks,
		cap:       shardNum,
	}
}

func (s *ShardedIndex) Put(key []byte, pos *datafile.DataPos) *datafile.DataPos {
	shard, lock := s.locateShard(key)
	lock.Lock()
	defer lock.Unlock()
	return shard.put(key, pos)
}

func (s *ShardedIndex) Get(key []byte) *datafile.DataPos {
	shard, lock := s.locateShard(key)
	lock.RLock()
	defer lock.RUnlock()
	return shard.get(key)
}

func (s *ShardedIndex) Delete(key []byte) *datafile.DataPos {
	shard, lock := s.locateShard(key)
	lock.Lock()
	defer lock.Unlock()
	return shard.delete(key)
}

func (s *ShardedIndex) Size() int {
	// 当前仅用于Stat方法, 不存在性能瓶颈
	// 当需要更高性能时可通过 WaitGroup 引入多协程
	total := 0
	for i := range s.index {
		s.indexLock[i].RLock()
		total += s.index[i].size()
		s.indexLock[i].RUnlock()
	}
	return total
}

func (s *ShardedIndex) Iterator(reverse bool) *IndexIterator {
	iters := make([]iterator, 0, s.cap)
	for i := 0; i < s.cap; i++ {
		s.indexLock[i].RLock()
		it := s.index[i].iterator(reverse)
		if it.valid() {
			iters = append(iters, it)
		}
		s.indexLock[i].RUnlock()
	}
	return newIndexIterator(iters, reverse)
}

func (s *ShardedIndex) Close() error {
	var firstErr error
	for idx := range s.index {
		s.indexLock[idx].Lock()
		if err := s.index[idx].close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.indexLock[idx].Unlock()
	}
	return firstErr
}

// 选择分片
func (s *ShardedIndex) locateShard(key []byte) (index, *sync.RWMutex) {
	// 使用 xxhash 算法
	hash := xxhash.Sum64(key)
	idx := hash & uint64(s.cap-1)
	return s.index[idx], &s.indexLock[idx]
}

func nextPowerOfTwo(cap int) int {
	n := cap - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16

	// 111..的二级制数 + 1即得到恰好大于等于它的2的次幂数
	if n >= MaxCap {
		return MaxCap
	}
	return n + 1
}

type IndexIterator struct {
	heap     *iterHeap  // 优先队列
	oldItems []iterator // 存放迭代过程中失效的迭代器
}

type iterHeap struct {
	items   []iterator
	reverse bool
}

func (h *iterHeap) Len() int {
	return len(h.items)
}

func (h *iterHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h.items[i].key(), h.items[j].key())
	return cmp < 0 != h.reverse
}

func (h *iterHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *iterHeap) Push(x interface{}) {
	h.items = append(h.items, x.(iterator))
}

func (h *iterHeap) Pop() interface{} {
	item := h.items[len(h.items)-1]
	h.items = h.items[0 : len(h.items)-1]
	return item
}

func newIndexIterator(iters []iterator, reverse bool) *IndexIterator {
	h := &iterHeap{
		items:   iters,
		reverse: reverse,
	}

	// 初始化堆 O(n)
	heap.Init(h)

	return &IndexIterator{
		heap: h,
	}
}

func (it *IndexIterator) Rewind() {
	if it.heap == nil {
		return
	}
	for _, t := range it.heap.items {
		t.rewind()
	}
	// 重置失效的迭代器
	for _, t := range it.oldItems {
		t.rewind()
		it.heap.items = append(it.heap.items, t)
	}
	it.oldItems = nil

	heap.Init(it.heap)
}

func (it *IndexIterator) Seek(key []byte) {
	if it.heap == nil || !it.Valid() {
		return
	}

	oldItems := it.heap.items
	it.heap.items = nil
	for _, item := range oldItems {
		item.seek(key)
		if item.valid() {
			it.heap.items = append(it.heap.items, item)
		} else {
			it.oldItems = append(it.oldItems, item)
		}
	}

	// 重新构建堆
	heap.Init(it.heap)
}

func (it *IndexIterator) Next() {
	if it.heap == nil || !it.Valid() {
		return
	}

	// 取出当前最小/最大元素
	item := heap.Pop(it.heap).(iterator)

	item.next()
	if item.valid() {
		heap.Push(it.heap, item)
	} else {
		it.oldItems = append(it.oldItems, item)
	}
}

func (it *IndexIterator) Valid() bool {
	if it.heap == nil {
		return false
	}
	return it.heap.Len() > 0
}

func (it *IndexIterator) Key() []byte {
	if it.heap == nil || !it.Valid() {
		return nil
	}
	return it.heap.items[0].key()
}

func (it *IndexIterator) Value() *datafile.DataPos {
	if it.heap == nil || !it.Valid() {
		return nil
	}
	return it.heap.items[0].value()
}

func (it *IndexIterator) Close() {
	if it.heap == nil {
		return
	}
	for _, item := range it.heap.items {
		item.close()
	}
	it.heap.items = nil
	it.heap = nil
}
