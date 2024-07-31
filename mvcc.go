package mini_lsm

import (
	"github.com/huandu/skiplist"
	"sync"
)

type CommittedTxnData struct {
	KeyHashes map[uint32]struct{}
	ReadTs    uint64
	CommitTs  uint64
}

type LsmMvccInner struct {
	WriteLock         sync.Mutex
	TsLock            sync.Mutex
	Ts                *Tuple[uint64, *Watermark]
	CommittedTxnsLock sync.Mutex
	CommittedTxns     map[uint64]*CommittedTxnData
}

func NewLsmMvccInner(initTs uint64) *LsmMvccInner {
	return &LsmMvccInner{
		Ts:            &Tuple[uint64, *Watermark]{initTs, NewWaterMark()},
		CommittedTxns: make(map[uint64]*CommittedTxnData),
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
	res := &Transaction{
		ReadTs:       readTs,
		Inner:        inner,
		LocalStorage: skiplist.New(skiplist.Bytes),
		KeyHashes:    nil,
	}
	res.committed.Store(false)
	return res
}
