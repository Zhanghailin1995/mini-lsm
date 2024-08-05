package mini_lsm

import (
	"github.com/huandu/skiplist"
	"github.com/tidwall/btree"
	"sync"
)

type CommittedTxnData struct {
	KeyHashes map[uint32]struct{}
	ReadTs    uint64
	CommitTs  uint64
}

type LsmMvccInner struct {
	WriteLock         sync.Mutex
	CommitLock        sync.Mutex
	TsLock            sync.Mutex
	Ts                *Tuple[uint64, *Watermark]
	CommittedTxnsLock sync.Mutex
	CommittedTxns     *btree.Map[uint64, *CommittedTxnData]
}

func NewLsmMvccInner(initTs uint64) *LsmMvccInner {
	return &LsmMvccInner{
		Ts:            &Tuple[uint64, *Watermark]{initTs, NewWaterMark()},
		CommittedTxns: btree.NewMap[uint64, *CommittedTxnData](2),
	}
}

func (l *LsmMvccInner) LatestCommitTs() uint64 {
	l.TsLock.Lock()
	defer l.TsLock.Unlock()
	return l.Ts.First
}

func (l *LsmMvccInner) UpdateCommitTs(newTs uint64) {
	l.TsLock.Lock()
	defer l.TsLock.Unlock()
	l.Ts.First = newTs
}

func (l *LsmMvccInner) Watermark() uint64 {
	l.TsLock.Lock()
	defer l.TsLock.Unlock()
	if w, ok := l.Ts.Second.Watermark(); ok {
		return w
	} else {
		return l.Ts.First
	}
}

func (l *LsmMvccInner) NewTxn(inner *LsmStorageInner, serializable bool) *Transaction {
	l.TsLock.Lock()
	defer l.TsLock.Unlock()
	readTs := l.Ts.First
	l.Ts.Second.AddReader(readTs)
	var keyHashes *Tuple[map[uint32]struct{}, map[uint32]struct{}]
	if serializable {
		keyHashes = &Tuple[map[uint32]struct{}, map[uint32]struct{}]{make(map[uint32]struct{}), make(map[uint32]struct{})}
	} else {
		keyHashes = nil
	}
	res := &Transaction{
		ReadTs:       readTs,
		Inner:        inner,
		LocalStorage: skiplist.New(skiplist.Bytes),
		KeyHashes:    keyHashes,
	}
	res.committed.Store(false)
	return res
}
