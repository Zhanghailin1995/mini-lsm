package mini_lsm

import (
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"github.com/huandu/skiplist"
	"slices"
	"sync"
	"sync/atomic"
)

type MemTable struct {
	rwLock          sync.RWMutex
	skipMap         *skiplist.SkipList
	id              uint32
	approximateSize uint32
	wal             *Wal
}

var CompareKeyFunc = skiplist.GreaterThanFunc(func(a, b interface{}) int {
	return a.(KeyBytes).Compare(b.(KeyBytes))
})

func CreateMemTable(id uint32) *MemTable {
	return &MemTable{
		skipMap:         skiplist.New(CompareKeyFunc),
		id:              id,
		approximateSize: 0,
		wal:             nil,
	}
}

func CreateWithWal(id uint32, p string) (*MemTable, error) {
	wal, err := NewWal(p)
	if err != nil {
		return nil, err
	}
	m := &MemTable{
		skipMap:         skiplist.New(CompareKeyFunc),
		id:              id,
		approximateSize: 0,
		wal:             wal,
	}
	return m, nil
}

func RecoverFromWal(id uint32, p string) (*MemTable, error) {
	skipMap := skiplist.New(CompareKeyFunc)
	wal, err := RecoverWal(p, skipMap)
	if err != nil {
		return nil, err
	}
	m := &MemTable{
		skipMap:         skipMap,
		id:              id,
		approximateSize: 0,
		wal:             wal,
	}
	return m, nil
}

func (m *MemTable) Close() error {
	if m.wal != nil {
		return m.wal.Close()
	}
	return nil
}

func (m *MemTable) Put(key KeyBytes, value []byte) error {
	estimatedSize := uint32(len(key.Val) + len(value))
	m.rwLock.Lock()
	m.skipMap.Set(key.Clone(), slices.Clone(value))
	m.rwLock.Unlock()
	atomic.AddUint32(&m.approximateSize, estimatedSize)
	if m.wal != nil {
		err := m.wal.Put(key, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *MemTable) Get(key KeyBytes) []byte {
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()
	v, ok := m.skipMap.GetValue(key)
	if ok {
		return utils.Copy(v.([]byte))
	}
	return nil
}

func (m *MemTable) SyncWal() error {
	if m.wal != nil {
		return m.wal.Sync()
	}
	return nil
}

func (m *MemTable) Scan(lower, upper KeyBound) *MemTableIterator {
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()
	return CreateMemTableIterator(m.skipMap, lower, upper)
}

func (m *MemTable) ForTestingScanSlice(lower, upper BytesBound) *MemTableIterator {
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()
	return CreateMemTableIterator(m.skipMap, MapKeyBoundPlusTs(lower, TsDefault), MapKeyBoundPlusTs(upper, TsDefault))
}

func (m *MemTable) Flush(builder *SsTableBuilder) error {
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()
	iter := m.skipMap.Front()
	for iter != nil {
		builder.Add(iter.Key().(KeyBytes), iter.Value.([]byte))
		iter = iter.Next()
	}
	return nil
}

func (m *MemTable) ApproximateSize() uint32 {
	return atomic.LoadUint32(&m.approximateSize)
}

func (m *MemTable) ForTestingPutSlice(key []byte, value []byte) error {
	return m.Put(KeyFromBytesWithTs(key, TsDefault), value)
}

func (m *MemTable) ForTestingGetSlice(key []byte) []byte {
	return m.Get(KeyFromBytesWithTs(key, TsDefault))
}

func (m *MemTable) IsEmpty() bool {
	return m.skipMap.Len() == 0
}

type MemTableIterator struct {
	ele          *skiplist.Element
	upper        KeyBound
	currentKey   KeyBytes
	currentValue []byte
}

func newMemTableIterator(ele *skiplist.Element, upper KeyBound) *MemTableIterator {
	var k KeyBytes
	var v []byte
	if ele != nil {
		k = ele.Key().(KeyBytes).Clone()
		v = slices.Clone(ele.Value.([]byte))
	} else {
		k = KeyBytes{Val: []byte{}}
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
		return newMemTableIterator(list.Front(), upper)
	} else if lower.Type == Included {
		return newMemTableIterator(list.Find(lower.Val), upper)
	} else {
		ele := list.Find(lower.Val)
		if ele != nil && ele.Key().(KeyBytes).Compare(lower.Val) == 0 {
			ele = ele.Next()
		}
		return newMemTableIterator(ele, upper)
	}
	//if lower.Type == Unbounded {
	//	// return &MemTableIterator{ele: list.Front(), upper: upper}
	//	return newMemTableIterator(list.Front(), upper)
	//} else {
	//	return newMemTableIterator(list.Find(lower.Val), upper)
	//}
}

func (m *MemTableIterator) Value() []byte {
	v := m.currentValue
	return v
}

func (m *MemTableIterator) Key() IteratorKey {
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
		//compare := bytes.Compare(m.ele.KeyOf().(KeyBytes).Val, m.upper.Val.Val)
		compare := m.ele.Key().(KeyBytes).Compare(m.upper.Val)
		if m.upper.Type == Excluded && compare == 0 {
			m.ele = nil
		} else if compare == 1 {
			m.ele = nil
		}
	}
	m.currentKey, m.currentValue = entryToKeyAndValue(m.ele)
	return nil
}

func entryToKeyAndValue(entry *skiplist.Element) (KeyBytes, []byte) {
	if entry == nil {
		return KeyBytes{Val: []byte{}}, nil
	} else {
		return entry.Key().(KeyBytes).Clone(), slices.Clone(entry.Value.([]byte))
	}
}

func (m *MemTableIterator) NumActiveIterators() int {
	return 1
}
