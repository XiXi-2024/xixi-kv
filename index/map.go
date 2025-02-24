package index

import (
	"bytes"
	"github.com/XiXi-2024/xixi-kv/datafile"
	"slices"
	"sort"
)

type hashMapIndex struct {
	mp map[string]*datafile.DataPos
}

func newMap() *hashMapIndex {
	return &hashMapIndex{
		mp: map[string]*datafile.DataPos{},
	}
}

func (m *hashMapIndex) put(key []byte, pos *datafile.DataPos) *datafile.DataPos {
	if m.mp == nil {
		return nil
	}
	oldPos := m.mp[string(key)]
	m.mp[string(key)] = pos
	return oldPos
}

func (m *hashMapIndex) get(key []byte) *datafile.DataPos {
	if m.mp == nil {
		return nil
	}
	oldPos := m.mp[string(key)]
	return oldPos
}

func (m *hashMapIndex) delete(key []byte) (*datafile.DataPos, bool) {
	if m.mp == nil {
		return nil, false
	}
	oldPos := m.mp[string(key)]
	delete(m.mp, string(key))
	return oldPos, true
}

func (m *hashMapIndex) size() int {
	if m.mp == nil {
		return 0
	}
	return len(m.mp)
}

func (m *hashMapIndex) iterator(reverse bool) iterator {
	return newMapIterator(m, reverse)
}

func (m *hashMapIndex) close() error {
	m.mp = nil
	return nil
}

type mapIterator struct {
	reverse  bool // 是否降序遍历
	curIndex int  // 当前遍历位置
	values   []*item
}

func newMapIterator(mp *hashMapIndex, reverse bool) *mapIterator {
	if mp == nil {
		return &mapIterator{
			values: make([]*item, 0),
		}
	}
	var idx int
	values := make([]*item, mp.size())

	for key, value := range mp.mp {
		values[idx] = &item{[]byte(key), value}
		idx++
	}

	t := -1
	if reverse {
		t = 1
	}
	slices.SortFunc(values, func(a *item, b *item) int {
		if a.Less(b) {
			return t
		}
		return -t
	})

	return &mapIterator{
		curIndex: 0,
		reverse:  reverse,
		values:   values,
	}
}

func (m *mapIterator) rewind() {
	m.curIndex = 0
}

func (m *mapIterator) seek(key []byte) {
	if m.reverse {
		m.curIndex = sort.Search(len(m.values), func(i int) bool {
			return bytes.Compare(m.values[i].key, key) <= 0
		})
	} else {
		m.curIndex = sort.Search(len(m.values), func(i int) bool {
			return bytes.Compare(m.values[i].key, key) >= 0
		})
	}
}

func (m *mapIterator) next() {
	m.curIndex++
}

func (m *mapIterator) valid() bool {
	return m.curIndex < len(m.values)
}

func (m *mapIterator) key() []byte {
	return m.values[m.curIndex].key
}

func (m *mapIterator) value() *datafile.DataPos {
	return m.values[m.curIndex].pos
}

func (m *mapIterator) close() {
	m.values = nil
}
