package index

import (
	"bytes"
	"github.com/XiXi-2024/xixi-kv/datafile"
	"github.com/huandu/skiplist"
	"sort"
	"sync"
)

type SkipListIndex struct {
	list *skiplist.SkipList
	lock *sync.RWMutex
}

func NewSkipList() *SkipListIndex {
	return &SkipListIndex{
		list: skiplist.New(skiplist.Bytes),
		lock: &sync.RWMutex{},
	}
}

func (s *SkipListIndex) Put(key []byte, pos *datafile.DataPos) *datafile.DataPos {
	s.lock.Lock()
	defer s.lock.Unlock()

	oldItem := s.list.Get(key)
	var oldValue *datafile.DataPos
	if oldItem != nil {
		oldValue = oldItem.Value.(*datafile.DataPos)
	}
	s.list.Set(key, pos)
	return oldValue
}

func (s *SkipListIndex) Get(key []byte) *datafile.DataPos {
	s.lock.RLock()
	oldItem := s.list.Get(key)
	s.lock.RUnlock()
	if oldItem == nil {
		return nil
	}
	return oldItem.Value.(*datafile.DataPos)
}

func (s *SkipListIndex) Delete(key []byte) (*datafile.DataPos, bool) {
	s.lock.Lock()
	oldItem := s.list.Remove(key)
	var oldValue *datafile.DataPos
	if oldItem != nil {
		oldValue = oldItem.Value.(*datafile.DataPos)
	}
	s.lock.Unlock()
	if oldItem == nil {
		return oldValue, false
	}
	return oldValue, true
}

func (s *SkipListIndex) Size() int {
	return s.list.Len()
}

func (s *SkipListIndex) Iterator(reverse bool) Iterator {
	return newSkipListIterator(reverse, s.list)
}

func (s *SkipListIndex) Close() error {
	return nil
}

type SkipListIterator struct {
	reverse bool // 是否降序遍历 todo 扩展点：转换为配置项成员
	// todo 优化点：采取效率更高的迭代方式
	curIndex int     // 当前遍历的下标位置
	values   []*Item // 类型复用, 存放 key + 位置索引信息
}

func newSkipListIterator(reverse bool, sl *skiplist.SkipList) *SkipListIterator {
	values := make([]*Item, sl.Len())
	var idx int = 0
	if reverse {
		idx = sl.Len() - 1
	}
	for i := sl.Front(); i != nil; i = i.Next() {
		values[idx] = &Item{
			key: i.Key().([]byte),
			pos: i.Value.(*datafile.DataPos),
		}
		if reverse {
			idx--
		} else {
			idx++
		}
	}
	return &SkipListIterator{
		reverse:  reverse,
		curIndex: 0,
		values:   values,
	}
}

func (s *SkipListIterator) Rewind() {
	s.curIndex = 0
}

func (s *SkipListIterator) Seek(key []byte) {
	if s.reverse {
		s.curIndex = sort.Search(len(s.values), func(i int) bool {
			return bytes.Compare(s.values[i].key, key) <= 0
		})
	} else {
		s.curIndex = sort.Search(len(s.values), func(i int) bool {
			return bytes.Compare(s.values[i].key, key) >= 0
		})
	}
}

func (s *SkipListIterator) Next() {
	s.curIndex += 1
}

func (s *SkipListIterator) Valid() bool {
	return s.curIndex < len(s.values)
}

func (s *SkipListIterator) Key() []byte {
	return s.values[s.curIndex].key
}

func (s *SkipListIterator) Value() *datafile.DataPos {
	return s.values[s.curIndex].pos
}

func (s *SkipListIterator) Close() {
	s.values = nil
}
