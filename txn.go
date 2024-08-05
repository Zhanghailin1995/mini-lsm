package mini_lsm

import (
	"bytes"
	"errors"
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"github.com/dgryski/go-farm"
	"github.com/huandu/skiplist"
	"slices"
	"sync"
	"sync/atomic"
)

var ErrSerializableCheckFailed = errors.New("serializable check failed")

type Transaction struct {
	ReadTs            uint64
	Inner             *LsmStorageInner
	LocalStorageMutex sync.RWMutex
	LocalStorage      *skiplist.SkipList
	committed         atomic.Bool
	KeyHashesMutex    sync.Mutex
	KeyHashes         *Tuple[map[uint32]struct{}, map[uint32]struct{}]
}

func (t *Transaction) Commit() error {
	if !t.committed.CompareAndSwap(false, true) {
		panic("cannot commit a transaction twice")
	}
	t.Inner.Mvcc().CommitLock.Lock()
	defer t.Inner.Mvcc().CommitLock.Unlock()
	serializabilityCheck := true
	if t.KeyHashes != nil {
		t.KeyHashesMutex.Lock()
		writeHashes := t.KeyHashes.First
		readHashes := t.KeyHashes.Second
		if len(writeHashes) != 0 {
			t.Inner.Mvcc().CommittedTxnsLock.Lock()
			var serializableCheckSuccess = true
			x := &serializableCheckSuccess
			t.Inner.Mvcc().CommittedTxns.Ascend(t.ReadTs+1, func(key uint64, value *CommittedTxnData) bool {
				for k, _ := range readHashes {
					if _, ok := value.KeyHashes[k]; ok {
						*x = false
						return false
					}
				}
				return true
			})
			t.Inner.Mvcc().CommittedTxnsLock.Unlock()
			if !*x {
				return ErrSerializableCheckFailed
			}
		}
		t.KeyHashesMutex.Unlock()
	} else {
		serializabilityCheck = false
	}
	batch := make([]WriteBatchRecord, 0)
	t.LocalStorageMutex.RLock()
	elem := t.LocalStorage.Front()
	t.LocalStorageMutex.RUnlock()
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
		t.LocalStorageMutex.RLock()
		elem = elem.Next()
		t.LocalStorageMutex.RUnlock()
	}
	ts, err := t.Inner.writeBatchInner(batch)
	if err != nil {
		return err
	}
	if serializabilityCheck {
		t.Inner.Mvcc().CommittedTxnsLock.Lock()
		defer t.Inner.Mvcc().CommittedTxnsLock.Unlock()
		committedTxns := t.Inner.Mvcc().CommittedTxns
		t.KeyHashesMutex.Lock()
		defer t.KeyHashesMutex.Unlock()
		writeHashes := t.KeyHashes.First
		_, ok := committedTxns.Set(ts, &CommittedTxnData{
			KeyHashes: writeHashes,
			ReadTs:    t.ReadTs,
			CommitTs:  ts,
		})
		utils.Assert(!ok, "commit ts should be unique")

		watermark := t.Inner.Mvcc().Watermark()
		for {
			k, _, b2 := committedTxns.Min()
			if b2 {
				if k < watermark {
					committedTxns.Delete(k)
				} else {
					break
				}
			} else {
				break
			}
		}
	}
	return nil
}

func (t *Transaction) Get(key []byte) ([]byte, error) {
	if t.committed.Load() {
		panic("cannot operate on committed transaction")
	}
	if t.KeyHashes != nil {
		t.KeyHashesMutex.Lock()
		readHashes := t.KeyHashes.Second
		readHashes[farm.Hash32(key)] = struct{}{}
		t.KeyHashesMutex.Unlock()
	}
	t.LocalStorageMutex.RLock()
	elem := t.LocalStorage.Get(key)
	t.LocalStorageMutex.RUnlock()
	if elem != nil {
		return slices.Clone(elem.Value.([]byte)), nil
	}
	return t.Inner.GetWithTs(key, t.ReadTs)
}

func (t *Transaction) Put(key []byte, value []byte) {
	if t.committed.Load() {
		panic("cannot operate on committed transaction")
	}
	t.LocalStorageMutex.Lock()
	t.LocalStorage.Set(slices.Clone(key), slices.Clone(value))
	t.LocalStorageMutex.Unlock()
	if t.KeyHashes != nil {
		t.KeyHashesMutex.Lock()
		defer t.KeyHashesMutex.Unlock()
		writeHashes := t.KeyHashes.First
		writeHashes[farm.Hash32(key)] = struct{}{}
	}
}

func (t *Transaction) Delete(key []byte) {
	if t.committed.Load() {
		panic("cannot operate on committed transaction")
	}
	t.LocalStorageMutex.Lock()
	t.LocalStorage.Set(slices.Clone(key), []byte{})
	t.LocalStorageMutex.Unlock()
	if t.KeyHashes != nil {
		t.KeyHashesMutex.Lock()
		defer t.KeyHashesMutex.Unlock()
		writeHashes := t.KeyHashes.First
		writeHashes[farm.Hash32(key)] = struct{}{}
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
	locaIter := CreateTxnLocalIterator(t, start, end)
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
	t         *Transaction
	ele       *skiplist.Element
	upper     BytesBound
	itemKey   []byte
	itemValue []byte
}

func newTxnLocalIterator(t *Transaction, ele *skiplist.Element, upper BytesBound) *TxnLocalIterator {
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

func CreateTxnLocalIterator(t *Transaction, lower, upper BytesBound) *TxnLocalIterator {
	t.LocalStorageMutex.RLock()
	defer t.LocalStorageMutex.RUnlock()
	if lower.Type == Unbounded {
		return newTxnLocalIterator(t, t.LocalStorage.Front(), upper)
	} else if lower.Type == Included {
		return newTxnLocalIterator(t, t.LocalStorage.Find(lower.Val), upper)
	} else {
		l := t.LocalStorage
		ele := l.Find(lower.Val)
		if ele != nil && bytes.Compare(ele.Key().([]byte), lower.Val) == 0 {
			ele = ele.Next()
		}
		return newTxnLocalIterator(t, ele, upper)
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
	t.t.LocalStorageMutex.RLock()
	t.ele = t.ele.Next()
	t.t.LocalStorageMutex.RUnlock()
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

func (t *TxnIterator) AddToReadSet(key []byte) {
	if t.txn.KeyHashes != nil {
		t.txn.KeyHashesMutex.Lock()
		readHashes := t.txn.KeyHashes.Second
		readHashes[farm.Hash32(key)] = struct{}{}
		t.txn.KeyHashesMutex.Unlock()
	}
}

func CreateTxnIterator(txn *Transaction, iter *TwoMergeIterator) (*TxnIterator, error) {
	res := &TxnIterator{txn: txn, iter: iter}
	if err := res.SkipDeleted(); err != nil {
		return nil, err
	}
	if iter.IsValid() {
		res.AddToReadSet(iter.Key().KeyRef())
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
