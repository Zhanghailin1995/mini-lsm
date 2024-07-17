package mini_lsm

import (
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
	return a.(KeyType).Compare(b.(KeyType))
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
	ele          *skiplist.Element
	upper        KeyBound
	currentKey   KeyType
	currentValue []byte
}

func newMemTableIterator(ele *skiplist.Element, upper KeyBound) *MemTableIterator {
	var k KeyType
	var v []byte
	if ele != nil {
		k = Key(utils.Copy(ele.Key().(KeyType).Val))
		v = utils.Copy(ele.Value.([]byte))
	} else {
		k = KeyType{Val: []byte{}}
		v = nil
	}
	m := &MemTableIterator{
		ele:          ele,
		upper:        upper,
		currentKey:   k,
		currentValue: v,
	}
	return m
}

func CreateMemTableIterator(list *skiplist.SkipList, lower, upper KeyBound) *MemTableIterator {
	if lower.Type == Unbounded {
		// return &MemTableIterator{ele: list.Front(), upper: upper}
		return newMemTableIterator(list.Front(), upper)
	} else if lower.Type == Included {
		// return &MemTableIterator{ele: list.Find(lower.Val), upper: upper}
		return newMemTableIterator(list.Find(lower.Val), upper)
	} else {
		ele := list.Find(lower.Val)
		if ele != nil && ele.Key().(KeyType).Compare(lower.Val) == 0 {
			ele = ele.Next()
		}
		return newMemTableIterator(ele, upper)
	}
}

func (m *MemTableIterator) Value() []byte {
	v := m.currentValue
	return v
}

func (m *MemTableIterator) Key() KeyType {
	k := m.currentKey
	return k
}

func (m *MemTableIterator) IsValid() bool {
	return len(m.currentKey.Val) != 0
}

func (m *MemTableIterator) Next() error {
	m.ele = m.ele.Next()
	if m.ele != nil {
		if m.upper.Type == Unbounded {
			m.currentKey, m.currentValue = entryToKeyAndValue(m.ele)
			return nil
		}
		//compare := bytes.Compare(m.ele.Key().(KeyType).Val, m.upper.Val.Val)
		compare := m.ele.Key().(KeyType).Compare(m.upper.Val)
		if m.upper.Type == Excluded && compare == 0 {
			m.ele = nil
		} else if compare == 1 {
			m.ele = nil
		}
	}
	m.currentKey, m.currentValue = entryToKeyAndValue(m.ele)
	return nil
}

func entryToKeyAndValue(entry *skiplist.Element) (KeyType, []byte) {
	if entry == nil {
		return KeyType{Val: []byte{}}, nil
	} else {
		return Key(utils.Copy(entry.Key().(KeyType).Val)), utils.Copy(entry.Value.([]byte))
	}
}

func (m *MemTableIterator) NumActiveIterators() int {
	return 1
}
