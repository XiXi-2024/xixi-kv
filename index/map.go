package index

import (
	"bytes"
	"github.com/XiXi-2024/xixi-kv/data"
	"slices"
	"sort"
	"sync"
)

type HashMapIndex struct {
	mp   map[string]*data.LogRecordPos
	lock *sync.RWMutex
}

func NewMap() *HashMapIndex {
	return &HashMapIndex{
		mp:   map[string]*data.LogRecordPos{},
		lock: &sync.RWMutex{},
	}
}

func (m *HashMapIndex) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	m.lock.Lock()
	defer m.lock.Unlock()

	oldPos := m.mp[string(key)]
	m.mp[string(key)] = pos
	return oldPos
}

func (m *HashMapIndex) Get(key []byte) *data.LogRecordPos {
	m.lock.RLock()
	defer m.lock.RUnlock()

	oldPos := m.mp[string(key)]
	return oldPos
}

func (m *HashMapIndex) Delete(key []byte) (*data.LogRecordPos, bool) {
	m.lock.Lock()
	defer m.lock.Unlock()

	oldPos := m.mp[string(key)]
	delete(m.mp, string(key))
	return oldPos, true
}

func (m *HashMapIndex) Size() int {
	return len(m.mp)
}

func (m *HashMapIndex) Iterator(reverse bool) Iterator {
	return newMapIterator(m, reverse)
}

func (m *HashMapIndex) Close() error {
	m.mp = nil
	return nil
}

type mapIterator struct {
	reverse  bool // 是否降序遍历
	curIndex int  // 当前遍历位置
	values   []*Item
}

func newMapIterator(mp *HashMapIndex, reverse bool) *mapIterator {
	var idx int
	values := make([]*Item, mp.Size())

	mp.lock.Lock()
	for key, value := range mp.mp {
		values[idx] = &Item{[]byte(key), value}
		idx++
	}
	mp.lock.Unlock()

	t := -1
	if reverse {
		t = 1
	}
	slices.SortFunc(values, func(a *Item, b *Item) int {
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

func (m *mapIterator) Rewind() {
	m.curIndex = 0
}

func (m *mapIterator) Seek(key []byte) {
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

func (m *mapIterator) Next() {
	m.curIndex++
}

func (m *mapIterator) Valid() bool {
	return m.curIndex < len(m.values)
}

func (m *mapIterator) Key() []byte {
	return m.values[m.curIndex].key
}

func (m *mapIterator) Value() *data.LogRecordPos {
	return m.values[m.curIndex].pos
}

func (m *mapIterator) Close() {
	m.values = nil
}
