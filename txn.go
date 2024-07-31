package mini_lsm

import (
	"github.com/huandu/skiplist"
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
	panic("implement me")
}

func (t *Transaction) Get(key []byte) ([]byte, error) {
	return t.Inner.GetWithTs(key, t.ReadTs)
}

func (t *Transaction) Put(key []byte, value []byte) {
	panic("implement me")
}

func (t *Transaction) Delete(key []byte) {
	panic("implement me")
}

func (t *Transaction) Scan(start BytesBound, end BytesBound) (*TxnIterator, error) {
	iterator, err := t.Inner.ScanWithTs(start, end, t.ReadTs)
	if err != nil {
		return nil, err
	}
	return CreateTxnIterator(t, iterator)
}

type TxnLocalIterator struct {
	ele       *skiplist.Element
	upper     []byte
	itemKey   []byte
	itemValue []byte
}

func (t *TxnLocalIterator) Next() error {
	panic("implement me")
}

func (t *TxnLocalIterator) IsValid() bool {
	panic("implement me")
}

func (t *TxnLocalIterator) Key() IteratorKey {
	panic("implement me")
}

func (t *TxnLocalIterator) Value() []byte {
	panic("implement me")
}

func (t *TxnLocalIterator) NumActiveIterators() int {
	return 1
}

type TxnIterator struct {
	txn  *Transaction
	iter *FusedIterator
}

func CreateTxnIterator(txn *Transaction, iter *FusedIterator) (*TxnIterator, error) {
	if _, ok := iter.iter.(*LsmIterator); !ok {
		panic("iter should be LsmIterator")
	}
	return &TxnIterator{txn: txn, iter: iter}, nil
}

func (t *TxnIterator) Next() error {
	return t.iter.Next()
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
