package mini_lsm

import (
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"github.com/sirupsen/logrus"
	"sync"
	"sync/atomic"
)

type MiniLsm struct {
	inner *LsmStorageInner
}

type LsmStorageInner struct {
	rwLock     sync.RWMutex
	stateLock  sync.Mutex
	path       string
	blockCache *BlockCache
	nextSstId  uint32
	state      *LsmStorageState
	options    *LsmStorageOptions
}

type LsmStorageState struct {
	memTable    *MemTable
	immMemTable []*MemTable
	l0SsTables  []uint32
	sstables    map[uint32]*SsTable
}

func (lsm *LsmStorageState) snapshot() *LsmStorageState {
	immMemtable := make([]*MemTable, len(lsm.immMemTable))
	copy(immMemtable, lsm.immMemTable)
	l0SsTables := make([]uint32, len(lsm.l0SsTables))
	copy(l0SsTables, lsm.l0SsTables)
	sstables := make(map[uint32]*SsTable)
	for k, v := range lsm.sstables {
		sstables[k] = v
	}
	return &LsmStorageState{
		memTable:    lsm.memTable,
		immMemTable: immMemtable,
		l0SsTables:  l0SsTables,
		sstables:    sstables,
	}
}

func CreateLsmStorageState(options *LsmStorageOptions) *LsmStorageState {
	return &LsmStorageState{
		memTable:    CreateMemTable(0),
		immMemTable: make([]*MemTable, 0),
		l0SsTables:  make([]uint32, 0),
		sstables:    make(map[uint32]*SsTable),
	}
}

type LsmStorageOptions struct {
	blockSize           uint32
	targetSstSize       uint32
	numberMemTableLimit uint32
	compactionOptions   *CompactionOptions
	enableWal           bool
}

func DefaultForWeek1Test() *LsmStorageOptions {
	return &LsmStorageOptions{
		blockSize:           4096,
		targetSstSize:       2 << 20,
		numberMemTableLimit: 50,
		compactionOptions: &CompactionOptions{
			compactionType: NoCompaction,
			opt:            nil,
		},
		enableWal: false,
	}
}

func OpenLsmStorageInner(path string, options *LsmStorageOptions) (*LsmStorageInner, error) {
	storage := &LsmStorageInner{
		state:      CreateLsmStorageState(options),
		path:       path,
		blockCache: NewBlockCache(),
		nextSstId:  0,
		options:    options,
	}
	return storage, nil
}

func (lsm *LsmStorageInner) Close() error {
	for id, sst := range lsm.state.sstables {
		err := sst.CloseSstFile()
		if err != nil {
			logrus.Errorf("close sst file %d error: %v", id, err)
		}
	}
	return nil
}

func (lsm *LsmStorageInner) tryFreeze(estimatedSize uint32) error {
	if estimatedSize >= lsm.options.targetSstSize {
		lsm.stateLock.Lock()
		defer lsm.stateLock.Unlock()
		lsm.rwLock.RLock()
		if lsm.state.memTable.ApproximateSize() >= lsm.options.targetSstSize {
			lsm.rwLock.RUnlock()
			_ = lsm.ForceFreezeMemTable()
		}
	}
	return nil
}

func (lsm *LsmStorageInner) getNextSstId() uint32 {
	sstId := atomic.AddUint32(&lsm.nextSstId, 1) - 1
	return sstId
}

func (lsm *LsmStorageInner) ForceFreezeMemTable() error {
	memtableId := lsm.getNextSstId()
	memtable := CreateMemTable(memtableId)

	lsm.rwLock.Lock()
	defer lsm.rwLock.Unlock()
	// 使用快照机制来减少锁的粒度
	snapshot := lsm.state.snapshot()
	snapshot.immMemTable = append([]*MemTable{snapshot.memTable}, snapshot.immMemTable...)
	snapshot.memTable = memtable
	lsm.state = snapshot
	return nil
}

func (lsm *LsmStorageInner) Get(key KeyType) ([]byte, error) {
	lsm.rwLock.RLock()
	snapshot := lsm.state
	lsm.rwLock.RUnlock()
	// 在读取时我们只需要获取读时快照就行了，这避免了长时间持久锁，我们使用写时复制的方法
	// 写数据时会复制一份状态副本（非数据副本），通过在副本上更新数据，更新完成后将原状态替换为副本（写时持有写锁，所以不用担心数据竞争问题）
	// 读取时我们只需要获取状态副本并且保持为一个本地变量，这样就不会受到写时复制的影响
	v := snapshot.memTable.Get(key)
	// 空值和nil值的区别
	// length 为 0的空值代表数据被删除了，nil值代表数据根本不存在
	if v != nil {
		if len(v) == 0 {
			return nil, nil
		}
		return v, nil
	}
	for _, mt := range snapshot.immMemTable {
		if v := mt.Get(key); v != nil {
			if len(v) == 0 {
				return nil, nil
			}
			return v, nil
		}
	}
	iters := make([]StorageIterator, 0, len(snapshot.l0SsTables))
	for _, id := range snapshot.l0SsTables {
		sstIter, err := CreateSsTableIteratorAndSeekToKey(snapshot.sstables[id], key)
		if err != nil {
			return nil, err
		}
		iters = append(iters, sstIter)
	}
	mergeIter := CreateMergeIterator(iters)
	if mergeIter.IsValid() && mergeIter.Key().Compare(key) == 0 && len(mergeIter.Value()) > 0 {
		return utils.Copy(mergeIter.Value()), nil
	}

	return nil, nil
}

func (lsm *LsmStorageInner) Put(key KeyType, value []byte) error {
	utils.Assert(len(value) != 0, "value cannot be empty")
	utils.Assert(len(key.Val) != 0, "key cannot be empty")
	var size uint32
	lsm.rwLock.RLock()
	err := lsm.state.memTable.Put(key, value)
	if err != nil {
		lsm.rwLock.RUnlock()
		return err
	}
	size = lsm.state.memTable.ApproximateSize()
	lsm.rwLock.RUnlock()
	err = lsm.tryFreeze(size)
	if err != nil {
		return err
	}
	return nil
}

func (lsm *LsmStorageInner) Delete(key KeyType) error {
	utils.Assert(len(key.Val) != 0, "key cannot be empty")
	var size uint32
	lsm.rwLock.RLock()
	err := lsm.state.memTable.Put(key, []byte{})
	if err != nil {
		lsm.rwLock.RUnlock()
		return err
	}
	size = lsm.state.memTable.ApproximateSize()
	lsm.rwLock.RUnlock()

	err = lsm.tryFreeze(size)
	if err != nil {
		return err
	}
	return nil
}

func (lsm *LsmStorageInner) Scan(lower, upper KeyBound) (*FusedIterator, error) {
	// 才用写时复制的策略，避免一直持有锁
	lsm.rwLock.RLock()
	snapshot := lsm.state
	lsm.rwLock.RUnlock()

	var memtableIters = make([]StorageIterator, 0, 1+len(snapshot.immMemTable))
	memtableIters = append(memtableIters, snapshot.memTable.Scan(lower, upper))
	for _, mt := range snapshot.immMemTable {
		memtableIters = append(memtableIters, mt.Scan(lower, upper))
	}
	memtableMergeIter := CreateMergeIterator(memtableIters)

	var sstableIters = make([]StorageIterator, 0, len(snapshot.l0SsTables))
	for _, id := range snapshot.l0SsTables {
		sst := snapshot.sstables[id]
		var sstableIter *SsTableIterator
		var err error
		if lower.Type == Included {
			sstableIter, err = CreateSsTableIteratorAndSeekToKey(sst, lower.Val)
			if err != nil {
				return nil, err
			}
		} else if lower.Type == Excluded {
			sstableIter, err = CreateSsTableIteratorAndSeekToKey(sst, lower.Val)
			if err != nil {
				return nil, err
			}
			if sstableIter.IsValid() && sstableIter.Key().Compare(lower.Val) == 0 {
				err := sstableIter.Next()
				if err != nil {
					return nil, err
				}
			}
		} else {
			sstableIter, err = CreateSsTableIteratorAndSeekToFirst(sst)
			if err != nil {
				return nil, err
			}
		}
		sstableIters = append(sstableIters, sstableIter)

	}

	sstMergeIter := CreateMergeIterator(sstableIters)

	iter, err := CreateTwoMergeIterator(memtableMergeIter, sstMergeIter)
	if err != nil {
		return nil, err
	}
	lsmIterator, err := CreateLsmIterator(iter, upper)
	if err != nil {
		return nil, err
	}
	return CreateFusedIterator(lsmIterator), nil
}
