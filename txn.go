package mini_lsm

import (
	"bytes"
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"github.com/huandu/skiplist"
	"slices"
	"sync"
	"sync/atomic"
)

type Transaction struct {
	ReadTs         uint64
	Inner          *LsmStorageInner
	LocalStorage   *skiplist.SkipList
	committed      atomic.Bool
	KeyHashesMutex sync.Mutex
	KeyHashes      *Tuple[map[uint32]struct{}, map[uint32]struct{}]
}

func (t *Transaction) Commit() error {
	if !t.committed.CompareAndSwap(false, true) {
		panic("cannot commit a transaction twice")
	}
	batch := make([]WriteBatchRecord, 0)
	elem := t.LocalStorage.Front()
	for {
		if elem == nil {
			break
		}
		key := elem.Key().([]byte)
		value := elem.Value.([]byte)
		if len(value) == 0 {
			batch = append(batch, &DelOp{K: key})
		} else {
			batch = append(batch, &PutOp{slices.Clone(key), slices.Clone(value)})
		}
		elem = elem.Next()
	}
	return t.Inner.WriteBatch(batch)
}

func (t *Transaction) Get(key []byte) ([]byte, error) {
	if t.committed.Load() {
		panic("cannot operate on committed transaction")
	}
	elem := t.LocalStorage.Get(key)
	if elem != nil {
		return slices.Clone(elem.Value.([]byte)), nil
	}
	return t.Inner.GetWithTs(key, t.ReadTs)
}

func (t *Transaction) Put(key []byte, value []byte) {
	if t.committed.Load() {
		panic("cannot operate on committed transaction")
	}
	t.LocalStorage.Set(slices.Clone(key), slices.Clone(value))
	if t.KeyHashes != nil {
		t.KeyHashesMutex.Lock()
		defer t.KeyHashesMutex.Unlock()
		writeHashes := t.KeyHashes.First
		writeHashes[utils.Crc32(key)] = struct{}{}
	}
}

func (t *Transaction) Delete(key []byte) {
	if t.committed.Load() {
		panic("cannot operate on committed transaction")
	}
	t.LocalStorage.Set(slices.Clone(key), []byte{})
	if t.KeyHashes != nil {
		t.KeyHashesMutex.Lock()
		defer t.KeyHashesMutex.Unlock()
		writeHashes := t.KeyHashes.First
		writeHashes[utils.Crc32(key)] = struct{}{}
	}
}

func (t *Transaction) RemoveReader() {
	t.Inner.Mvcc().TsLock.Lock()
	defer t.Inner.Mvcc().TsLock.Unlock()
	ts := t.Inner.Mvcc().Ts
	ts.Second.RemoveReader(t.ReadTs)
}

func (t *Transaction) End() {
	t.RemoveReader()
}

func (t *Transaction) Scan(start BytesBound, end BytesBound) (*TxnIterator, error) {
	if t.committed.Load() {
		panic("cannot operate on committed transaction")
	}
	locaIter := CreateTxnLocalIterator(t.LocalStorage, start, end)
	iterator, err := t.Inner.ScanWithTs(start, end, t.ReadTs)
	if err != nil {
		return nil, err
	}
	mergeIterator, err := CreateTwoMergeIterator(locaIter, iterator)
	if err != nil {
		return nil, err
	}

	return CreateTxnIterator(t, mergeIterator)
}

type TxnLocalIterator struct {
	ele       *skiplist.Element
	upper     BytesBound
	itemKey   []byte
	itemValue []byte
}

func newTxnLocalIterator(ele *skiplist.Element, upper BytesBound) *TxnLocalIterator {
	var k []byte
	var v []byte
	if ele != nil {
		k = slices.Clone(ele.Key().([]byte))
		v = slices.Clone(ele.Value.([]byte))
	} else {
		k = []byte{}
		v = nil
	}
	m := &TxnLocalIterator{
		ele:       ele,
		upper:     upper,
		itemKey:   k,
		itemValue: v,
	}
	return m
}

func CreateTxnLocalIterator(list *skiplist.SkipList, lower, upper BytesBound) *TxnLocalIterator {
	if lower.Type == Unbounded {
		return newTxnLocalIterator(list.Front(), upper)
	} else if lower.Type == Included {
		return newTxnLocalIterator(list.Find(lower.Val), upper)
	} else {
		ele := list.Find(lower.Val)
		if ele != nil && bytes.Compare(ele.Key().([]byte), lower.Val) == 0 {
			ele = ele.Next()
		}
		return newTxnLocalIterator(ele, upper)
	}
}

func entryToItem(entry *skiplist.Element) ([]byte, []byte) {
	if entry == nil {
		return []byte{}, nil
	} else {
		return slices.Clone(entry.Key().([]byte)), slices.Clone(entry.Value.([]byte))
	}
}

func (t *TxnLocalIterator) Next() error {
	t.ele = t.ele.Next()
	if t.ele != nil {
		if t.upper.Type == Unbounded {
			t.itemKey, t.itemValue = entryToItem(t.ele)
			return nil
		}
		compare := bytes.Compare(t.ele.Key().([]byte), t.upper.Val)
		if t.upper.Type == Excluded && compare == 0 {
			t.ele = nil
		} else if compare == 1 {
			t.ele = nil
		}
	}
	t.itemKey, t.itemValue = entryToItem(t.ele)
	return nil
}

func (t *TxnLocalIterator) IsValid() bool {
	return len(t.itemKey) != 0
}

func (t *TxnLocalIterator) Key() IteratorKey {
	return KeySlice(t.itemKey)
}

func (t *TxnLocalIterator) Value() []byte {
	return t.itemValue
}

func (t *TxnLocalIterator) NumActiveIterators() int {
	return 1
}

type TxnIterator struct {
	txn  *Transaction
	iter *TwoMergeIterator
}

func (t *TxnIterator) SkipDeleted() error {
	for t.iter.IsValid() && len(t.iter.Value()) == 0 {
		if err := t.iter.Next(); err != nil {
			return err
		}
	}
	return nil
}

func CreateTxnIterator(txn *Transaction, iter *TwoMergeIterator) (*TxnIterator, error) {
	res := &TxnIterator{txn: txn, iter: iter}
	if err := res.SkipDeleted(); err != nil {
		return nil, err
	}
	return res, nil
}

func (t *TxnIterator) Next() error {
	if err := t.iter.Next(); err != nil {
		return err
	}
	return t.SkipDeleted()
}

func (t *TxnIterator) IsValid() bool {
	return t.iter.IsValid()
}

func (t *TxnIterator) Key() IteratorKey {
	return t.iter.Key()
}

func (t *TxnIterator) Value() []byte {
	return t.iter.Value()
}

func (t *TxnIterator) NumActiveIterators() int {
	return t.iter.NumActiveIterators()
}
