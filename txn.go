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

func (t *Transaction) Get(key []byte) ([]byte, bool, error) {
	panic("implement me")
}

func (t *Transaction) Put(key []byte, value []byte) {
	panic("implement me")
}

func (t *Transaction) Delete(key []byte) {
	panic("implement me")
}

func (t *Transaction) Scan(start []byte, end []byte) (*TxnIterator, error) {
	panic("implement me")
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
	iter *TwoMergeIterator
}

func CreateTxnIterator(txn *Transaction, iter *TwoMergeIterator) *TxnIterator {
	panic("implement me")
}

func (t *TxnIterator) Next() error {
	panic("implement me")
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
