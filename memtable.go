package mini_lsm

import (
	"bytes"
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"github.com/huandu/skiplist"
	"sync"
	"sync/atomic"
)

type MemTable struct {
	rwLock          sync.RWMutex
	skipMap         *skiplist.SkipList
	id              uint32
	approximateSize uint32
}

var CompareKeyFunc = skiplist.GreaterThanFunc(func(a, b interface{}) int {
	return bytes.Compare(a.(KeyType).Val, b.(KeyType).Val)
})

func CreateMemTable(id uint32) *MemTable {
	return &MemTable{
		skipMap:         skiplist.New(CompareKeyFunc),
		id:              id,
		approximateSize: 0,
	}
}

func (m *MemTable) Put(key KeyType, value []byte) error {
	estimatedSize := uint32(len(key.Val) + len(value))
	m.rwLock.Lock()
	m.skipMap.Set(key, utils.Copy(value))
	m.rwLock.Unlock()
	atomic.AddUint32(&m.approximateSize, estimatedSize)
	return nil
}

func (m *MemTable) Get(key KeyType) []byte {
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()
	v, ok := m.skipMap.GetValue(key)
	if ok {
		return utils.Copy(v.([]byte))
	}
	return nil
}

func (m *MemTable) Scan(lower, upper KeyBound) *MemTableIterator {
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()
	return CreateMemTableIterator(m.skipMap, lower, upper)
}

func (m *MemTable) ApproximateSize() uint32 {
	return atomic.LoadUint32(&m.approximateSize)
}

func (m *MemTable) ForTestingPutSlice(key KeyType, value []byte) error {
	return m.Put(key, value)
}

func (m *MemTable) ForTestingGetSlice(key KeyType) []byte {
	return m.Get(key)
}

type BoundType uint8

const (
	Unbounded BoundType = iota
	Included
	Excluded
)

type KeyBound struct {
	Val  KeyType
	Type BoundType
}

func Bounded(val KeyType, boundType BoundType) KeyBound {
	return KeyBound{Val: val, Type: boundType}
}

func Include(val KeyType) KeyBound {
	return Bounded(val, Included)
}

func Exclude(val KeyType) KeyBound {
	return Bounded(val, Excluded)
}

func Unbound() KeyBound {
	return Bounded(KeyType{
		Val: nil,
	}, Unbounded)
}

type MemTableIterator struct {
	ele   *skiplist.Element
	upper KeyBound
}

func CreateMemTableIterator(list *skiplist.SkipList, lower, upper KeyBound) *MemTableIterator {
	if lower.Type == Unbounded {
		return &MemTableIterator{ele: list.Front(), upper: upper}
	} else if lower.Type == Included {
		return &MemTableIterator{ele: list.Find(lower.Val), upper: upper}
	} else {
		ele := list.Find(lower.Val)
		if ele != nil {
			ele = ele.Next()
		}
		return &MemTableIterator{ele: ele, upper: upper}
	}
}

func (m *MemTableIterator) Value() []byte {
	v := m.ele.Value.([]byte)
	return utils.Copy(v)
}

func (m *MemTableIterator) Key() KeyType {
	k := m.ele.Key().(KeyType)
	return KeyType{Val: utils.Copy(k.Val)}
}

func (m *MemTableIterator) IsValid() bool {
	return m.ele != nil && len(m.ele.Key().(KeyType).Val) != 0
}

func (m *MemTableIterator) Next() error {
	m.ele = m.ele.Next()
	if m.ele != nil {
		if m.upper.Type == Unbounded {
			return nil
		}
		compare := bytes.Compare(m.ele.Key().(KeyType).Val, m.upper.Val.Val)
		if m.upper.Type == Excluded && compare == 0 {
			m.ele = nil
		} else if compare == 1 {
			m.ele = nil
		}
	}
	return nil
}

func (m *MemTableIterator) NumActiveIterators() int {
	return 1
}
