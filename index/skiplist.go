package index

import (
	"bytes"
	"github.com/XiXi-2024/xixi-kv/datafile"
	"github.com/huandu/skiplist"
	"sort"
)

type skipListIndex struct {
	list *skiplist.SkipList
}

func newSkipList() *skipListIndex {
	return &skipListIndex{
		list: skiplist.New(skiplist.Bytes),
	}
}

func (s *skipListIndex) put(key []byte, pos *datafile.DataPos) *datafile.DataPos {
	if s.list == nil {
		return nil
	}
	oldItem := s.list.Get(key)
	var oldValue *datafile.DataPos
	if oldItem != nil {
		oldValue = oldItem.Value.(*datafile.DataPos)
	}
	s.list.Set(key, pos)
	return oldValue
}

func (s *skipListIndex) get(key []byte) *datafile.DataPos {
	if s.list == nil {
		return nil
	}
	oldItem := s.list.Get(key)
	if oldItem == nil {
		return nil
	}
	return oldItem.Value.(*datafile.DataPos)
}

func (s *skipListIndex) delete(key []byte) *datafile.DataPos {
	if s.list == nil {
		return nil
	}
	oldItem := s.list.Remove(key)
	var oldValue *datafile.DataPos
	if oldItem != nil {
		oldValue = oldItem.Value.(*datafile.DataPos)
	}
	return oldValue
}

func (s *skipListIndex) size() int {
	if s.list == nil {
		return 0
	}
	return s.list.Len()
}

func (s *skipListIndex) iterator(reverse bool) iterator {
	return newSkipListIterator(reverse, s.list)
}

func (s *skipListIndex) close() error {
	s.list = nil
	return nil
}

type skipListIterator struct {
	reverse  bool    // 是否降序遍历
	curIndex int     // 当前遍历的下标位置
	values   []*item // 类型复用, 存放 key + 位置索引信息
}

func newSkipListIterator(reverse bool, sl *skiplist.SkipList) *skipListIterator {
	if sl == nil {
		return &skipListIterator{
			values: make([]*item, 0),
		}
	}
	values := make([]*item, sl.Len())
	idx := 0
	if reverse {
		idx = sl.Len() - 1
	}
	for i := sl.Front(); i != nil; i = i.Next() {
		values[idx] = &item{
			key: i.Key().([]byte),
			pos: i.Value.(*datafile.DataPos),
		}
		if reverse {
			idx--
		} else {
			idx++
		}
	}
	return &skipListIterator{
		reverse:  reverse,
		curIndex: 0,
		values:   values,
	}
}

func (s *skipListIterator) rewind() {
	s.curIndex = 0
}

func (s *skipListIterator) seek(key []byte) {
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

func (s *skipListIterator) next() {
	s.curIndex += 1
}

func (s *skipListIterator) valid() bool {
	return s.curIndex < len(s.values)
}

func (s *skipListIterator) key() []byte {
	return s.values[s.curIndex].key
}

func (s *skipListIterator) value() *datafile.DataPos {
	return s.values[s.curIndex].pos
}

func (s *skipListIterator) close() {
	s.values = nil
}
