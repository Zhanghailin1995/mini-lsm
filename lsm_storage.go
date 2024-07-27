package mini_lsm

import (
	"fmt"
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"github.com/dgryski/go-farm"
	"github.com/sirupsen/logrus"
	"os"
	"path"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
)

type MiniLsm struct {
	inner *LsmStorageInner
	// Notifies the L0 flush thread to stop working.
	flushShutdownNotifier          chan struct{}
	flushShutdownDoneNotifier      chan struct{}
	compactionShutdownNotifier     chan struct{}
	compactionShutdownDoneNotifier chan struct{}
}

func (lsm *MiniLsm) Shutdown() error {
	err := lsm.inner.SyncDir()
	if err != nil {
		return err
	}
	lsm.flushShutdownNotifier <- struct{}{}
	lsm.compactionShutdownNotifier <- struct{}{}
	<-lsm.compactionShutdownDoneNotifier
	<-lsm.flushShutdownDoneNotifier

	if lsm.inner.options.EnableWal {
		if err := lsm.inner.Sync(); err != nil {
			return err
		}
		if err := lsm.inner.SyncDir(); err != nil {
			return err
		}
		return lsm.inner.Close()
	}
	if !ReadLsmStorageState(lsm.inner, func(state *LsmStorageState) bool {
		return state.memTable.IsEmpty()
	}) {
		err := lsm.inner.FreezeMemtableWithMemtable(CreateMemTable(lsm.inner.getNextSstId()))
		if err != nil {
			return err
		}
	}
	for {
		lsm.inner.rwLock.RLock()
		x := len(lsm.inner.state.immMemTable) == 0
		lsm.inner.rwLock.RUnlock()
		if !x {
			err := lsm.inner.ForceFlushNextImmMemtable()
			if err != nil {
				return err
			}
		} else {
			break
		}
	}
	err = lsm.inner.SyncDir()
	if err != nil {
		return err
	}
	return lsm.inner.Close()
}

func Open(p string, options *LsmStorageOptions) (*MiniLsm, error) {
	inner, err := OpenLsmStorageInner(p, options)
	if err != nil {
		return nil, err
	}
	lsm := &MiniLsm{
		inner:                          inner,
		flushShutdownNotifier:          make(chan struct{}),
		flushShutdownDoneNotifier:      make(chan struct{}),
		compactionShutdownNotifier:     make(chan struct{}),
		compactionShutdownDoneNotifier: make(chan struct{}),
	}
	inner.SpawnCompactionThread(lsm.compactionShutdownNotifier, lsm.compactionShutdownDoneNotifier)
	inner.SpawnFlushThread(lsm.flushShutdownNotifier, lsm.flushShutdownDoneNotifier)
	return lsm, nil
}

func (lsm *MiniLsm) Sync() error {
	return lsm.inner.Sync()
}

func (lsm *MiniLsm) Get(key []byte) ([]byte, error) {
	return lsm.inner.Get(Key(key))
}

func (lsm *MiniLsm) Put(key, value []byte) error {
	return lsm.inner.Put(Key(key), value)
}

func (lsm *MiniLsm) Delete(key []byte) error {
	return lsm.inner.Delete(Key(key))
}

func (lsm *MiniLsm) Scan(lower, upper BytesBound) (*FusedIterator, error) {
	return lsm.inner.Scan(MapBound(lower), MapBound(upper))
}

func (lsm *MiniLsm) ForceFlush() error {
	lsm.inner.rwLock.RLock()
	if !lsm.inner.state.memTable.IsEmpty() {
		lsm.inner.rwLock.RUnlock()

		lsm.inner.stateLock.Lock()
		err := lsm.inner.ForceFreezeMemTable()
		lsm.inner.stateLock.Unlock()
		if err != nil {
			return err
		}
	} else {
		lsm.inner.rwLock.RUnlock()
	}

	lsm.inner.rwLock.RLock()
	if len(lsm.inner.state.immMemTable) > 0 {
		lsm.inner.rwLock.RUnlock()
		return lsm.inner.ForceFlushNextImmMemtable()
	} else {
		lsm.inner.rwLock.RUnlock()
		return nil
	}
}

func (lsm *MiniLsm) ForceFullCompaction() error {
	return lsm.inner.ForceFullCompaction()
}

type LsmStorageInner struct {
	rwLock               sync.RWMutex
	stateLock            sync.Mutex
	path                 string
	blockCache           *BlockCache
	nextSstId            uint32
	options              *LsmStorageOptions
	state                *LsmStorageState
	compactionController *CompactionController
	manifest             *Manifest
}

func ReadLsmStorageState[T any](lsm *LsmStorageInner, f func(*LsmStorageState) T) T {
	lsm.rwLock.RLock()
	defer lsm.rwLock.RUnlock()
	return f(lsm.state)
}

func UpdateLsmStorageState(lsm *LsmStorageInner, f func(*LsmStorageInner)) {
	lsm.rwLock.Lock()
	defer lsm.rwLock.Unlock()
	f(lsm)
}

type LevelSsTables struct {
	level    uint32
	ssTables []uint32
}

func (ls *LevelSsTables) Clone() *LevelSsTables {
	ssTables := make([]uint32, len(ls.ssTables))
	copy(ssTables, ls.ssTables)
	return &LevelSsTables{
		level:    ls.level,
		ssTables: ssTables,
	}
}

type LsmStorageState struct {
	memTable    *MemTable
	immMemTable []*MemTable
	l0SsTables  []uint32
	levels      []*LevelSsTables
	sstables    map[uint32]*SsTable
}

func (lsm *LsmStorageState) snapshot() *LsmStorageState {
	immMemtable := make([]*MemTable, len(lsm.immMemTable))
	copy(immMemtable, lsm.immMemTable)
	l0SsTables := make([]uint32, len(lsm.l0SsTables))
	copy(l0SsTables, lsm.l0SsTables)
	levels := make([]*LevelSsTables, 0, len(lsm.levels))
	for _, level := range lsm.levels {
		ssTables := make([]uint32, len(level.ssTables))
		copy(ssTables, level.ssTables)
		levels = append(levels, &LevelSsTables{
			level:    level.level,
			ssTables: ssTables,
		})
	}

	sstables := make(map[uint32]*SsTable)
	for k, v := range lsm.sstables {
		sstables[k] = v
	}
	return &LsmStorageState{
		memTable:    lsm.memTable,
		immMemTable: immMemtable,
		l0SsTables:  l0SsTables,
		levels:      levels,
		sstables:    sstables,
	}
}

func CreateLsmStorageState(options *LsmStorageOptions) *LsmStorageState {
	var levels []*LevelSsTables
	switch options.CompactionOptions.CompactionType {
	case NoCompaction:
		levels = make([]*LevelSsTables, 1)
		levels[0] = &LevelSsTables{
			level:    1,
			ssTables: make([]uint32, 0),
		}
		break
	case Simple:
		opt := options.CompactionOptions.Opt.(*SimpleLeveledCompactionOptions)
		levels = make([]*LevelSsTables, opt.MaxLevels)
		for i := 0; i < int(opt.MaxLevels); i++ {
			levels[i] = &LevelSsTables{
				level:    uint32(i + 1),
				ssTables: make([]uint32, 0),
			}
		}
		break
	case Tiered:
		opt := options.CompactionOptions.Opt.(*TieredCompactionOptions)
		levels = make([]*LevelSsTables, 0, opt.NumTiers)
		break
	case Leveled:
		opt := options.CompactionOptions.Opt.(*LeveledCompactionOptions)
		levels = make([]*LevelSsTables, opt.MaxLevels)
		for i := 0; i < int(opt.MaxLevels); i++ {
			levels[i] = &LevelSsTables{
				level:    uint32(i + 1),
				ssTables: make([]uint32, 0),
			}
		}
	default:
		panic("unsupported compaction type")
	}
	return &LsmStorageState{
		memTable:    CreateMemTable(0),
		immMemTable: make([]*MemTable, 0),
		l0SsTables:  make([]uint32, 0),
		levels:      levels,
		sstables:    make(map[uint32]*SsTable),
	}
}

type LsmStorageOptions struct {
	BlockSize           uint32
	TargetSstSize       uint32
	NumberMemTableLimit uint32
	CompactionOptions   *CompactionOptions
	EnableWal           bool
	Serializable        bool
}

func DefaultForWeek1Test() *LsmStorageOptions {
	return &LsmStorageOptions{
		BlockSize:           4096,
		TargetSstSize:       2 << 20,
		NumberMemTableLimit: 50,
		CompactionOptions: &CompactionOptions{
			CompactionType: NoCompaction,
			Opt:            nil,
		},
		EnableWal:    false,
		Serializable: false,
	}
}

func DefaultForWeek1Day6Test() *LsmStorageOptions {
	return &LsmStorageOptions{
		BlockSize:           4096,
		TargetSstSize:       2 << 20,
		NumberMemTableLimit: 2,
		CompactionOptions: &CompactionOptions{
			CompactionType: NoCompaction,
			Opt:            nil,
		},
		EnableWal:    false,
		Serializable: false,
	}
}

func DefaultForWeek2Test(compactionOptions *CompactionOptions) *LsmStorageOptions {
	return &LsmStorageOptions{
		BlockSize:           4096,
		TargetSstSize:       1 << 20,
		NumberMemTableLimit: 2,
		CompactionOptions:   compactionOptions,
		EnableWal:           false,
		Serializable:        false,
	}
}

func OpenLsmStorageInner(p string, options *LsmStorageOptions) (*LsmStorageInner, error) {

	// if p is not exist, create it
	err := utils.CreateDirIfNotExist(p)
	state := CreateLsmStorageState(options)
	if err != nil {
		return nil, err
	}
	var cc *CompactionController
	if options.CompactionOptions.CompactionType == NoCompaction {
		cc = &CompactionController{
			CompactionType: NoCompaction,
			Controller:     nil,
		}
	} else if options.CompactionOptions.CompactionType == Simple {
		cc = &CompactionController{
			CompactionType: Simple,
			Controller:     NewSimpleLeveledCompactionController(options.CompactionOptions.Opt.(*SimpleLeveledCompactionOptions)),
		}
	} else if options.CompactionOptions.CompactionType == Tiered {
		cc = &CompactionController{
			CompactionType: Tiered,
			Controller:     NewTieredCompactionController(options.CompactionOptions.Opt.(*TieredCompactionOptions)),
		}
	} else if options.CompactionOptions.CompactionType == Leveled {
		cc = &CompactionController{
			CompactionType: Leveled,
			Controller:     NewLeveledCompactionController(options.CompactionOptions.Opt.(*LeveledCompactionOptions)),
		}
	} else {
		panic("unsupported compaction type")
	}
	blockCache := NewBlockCache()
	var manifest *Manifest
	var createManifestErr error
	var nextSstId uint32 = 1
	manifestPath := path.Join(p, "MANIFEST")
	if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
		// 创建manifest文件
		if options.EnableWal {
			state.memTable, err = CreateWithWal(state.memTable.id, pathOfWalStatic(p, state.memTable.id))
			if err != nil {
				return nil, err
			}
		}
		manifest, createManifestErr = NewManifest(manifestPath)
		if createManifestErr != nil {
			panic("failed to create manifest file")
		}
		if err := manifest.AddRecord(&NewMemtableRecord{NewMemtable: state.memTable.id}); err != nil {
			return nil, err
		}
	} else if err == nil {
		m, records, err := RecoverManifest(manifestPath)
		if err != nil {
			return nil, err
		}
		memtables := make(map[uint32]struct{})
		// 从manifest中恢复状态
		for _, record := range records {
			switch record.recordType() {
			case Flush:
				flush := record.(*FlushRecord)
				_, ok := memtables[flush.Flush]
				delete(memtables, flush.Flush)
				utils.Assert(ok, "memtable not found")
				if cc.FlushToL0() {
					state.l0SsTables = append([]uint32{flush.Flush}, state.l0SsTables...)
				} else {
					// In tiered compaction, create a new tier
					state.levels = append([]*LevelSsTables{{level: flush.Flush, ssTables: []uint32{flush.Flush}}}, state.levels...)
				}
				nextSstId = utils.MaxU32(nextSstId, flush.Flush)
			case NewMemtable:
				nextSstId = utils.MaxU32(nextSstId, record.(*NewMemtableRecord).NewMemtable)
				memtables[record.(*NewMemtableRecord).NewMemtable] = struct{}{}
			case Compaction:
				compaction := record.(*CompactionRecord)
				newState, _ := cc.ApplyCompactionResult(state, compaction.CompactionTask, compaction.SstIds)
				state = newState
				nextSstId = utils.MaxU32(nextSstId, slices.Max(compaction.SstIds))
			default:
				panic("unknown record type")
			}
		}
		sstCnt := 0
		sstIds := make([]uint32, 0)
		sstIds = append(sstIds, state.l0SsTables...)
		for _, level := range state.levels {
			sstIds = append(sstIds, level.ssTables...)
		}
		for _, id := range sstIds {
			fileObj := utils.Unwrap(OpenFileObject(pathOfSstStatic(p, id)))
			sst, err := OpenSsTable(id, blockCache, fileObj)
			if err != nil {
				return nil, err
			}
			state.sstables[id] = sst
			sstCnt++
		}
		fmt.Printf("%d SSTs opened\r\n", sstCnt)
		nextSstId++
		if options.EnableWal {
			walCnt := 0
			// memtables中剩下的是没有被flush的memtable
			notFlushedMemtables := make([]uint32, 0, len(memtables))
			for id := range memtables {
				notFlushedMemtables = append(notFlushedMemtables, id)
			}
			slices.Sort(notFlushedMemtables)
			for _, id := range notFlushedMemtables {
				memtable, err := RecoverFromWal(id, pathOfWalStatic(p, id))
				if err != nil {
					return nil, err
				}
				if !memtable.IsEmpty() {
					state.immMemTable = append([]*MemTable{memtable}, state.immMemTable...)
					walCnt++
				} else {
					err := memtable.Close()
					if err != nil {
						return nil, err
					}
					if err := os.Remove(pathOfWalStatic(p, id)); err != nil {
						return nil, err
					}
				}
			}
			fmt.Printf("%d WALs recovered\r\n", walCnt)
			state.memTable, err = CreateWithWal(nextSstId, pathOfWalStatic(p, nextSstId))
			if err != nil {
				return nil, err
			}
		} else {
			state.memTable = CreateMemTable(nextSstId)
		}
		if err := m.AddRecordWhenInit(&NewMemtableRecord{NewMemtable: state.memTable.id}); err != nil {
			return nil, err
		}
		nextSstId++
		manifest = m
	}

	storage := &LsmStorageInner{
		state:                state,
		path:                 p,
		blockCache:           blockCache,
		nextSstId:            nextSstId,
		compactionController: cc,
		options:              options,
		manifest:             manifest,
	}
	err = storage.SyncDir()
	if err != nil {
		return nil, err
	}
	return storage, nil
}

func (lsm *LsmStorageInner) Close() error {
	lsm.stateLock.Lock()
	lsm.rwLock.Lock()
	snapshot := lsm.state.snapshot()
	// 这里有数据竞争的问题，需要考虑一下，在rust中可以使用drop机制来关闭file
	for id, sst := range snapshot.sstables {
		err := sst.CloseSstFile()
		if err != nil {
			logrus.Errorf("close sst file %d error: %v", id, err)
		}
	}
	lsm.state = snapshot
	err := lsm.manifest.Close()
	if err != nil {
		return err
	}
	if lsm.options.EnableWal {
		err = lsm.state.memTable.Close()
		if err != nil {
			return err
		}
		for _, memtable := range lsm.state.immMemTable {
			err = memtable.Close()
			if err != nil {
				return err
			}
		}
	}
	lsm.rwLock.Unlock()
	lsm.stateLock.Unlock()
	return nil
}

func (lsm *LsmStorageInner) Sync() error {
	memtable := ReadLsmStorageState(lsm, func(state *LsmStorageState) *MemTable {
		return state.memTable
	})
	return memtable.SyncWal()
}

func (lsm *LsmStorageInner) tryFreeze(estimatedSize uint32) error {
	if estimatedSize >= lsm.options.TargetSstSize {
		lsm.stateLock.Lock()
		defer lsm.stateLock.Unlock()
		lsm.rwLock.RLock()
		if lsm.state.memTable.ApproximateSize() >= lsm.options.TargetSstSize {
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

func (lsm *LsmStorageInner) FreezeMemtableWithMemtable(memtable *MemTable) error {
	lsm.rwLock.Lock()
	// 使用快照机制来减少锁的粒度
	snapshot := lsm.state.snapshot()
	oldMemtable := snapshot.memTable
	snapshot.immMemTable = append([]*MemTable{oldMemtable}, snapshot.immMemTable...)
	snapshot.memTable = memtable
	lsm.state = snapshot
	lsm.rwLock.Unlock()
	// 只在memtable被冻结时才Sync,这似乎不妥，会在掉电时丢失数据
	if err := oldMemtable.SyncWal(); err != nil {
		return err
	}
	return nil
}

func (lsm *LsmStorageInner) ForceFreezeMemTable() error {
	memtableId := lsm.getNextSstId()
	var memtable *MemTable
	if lsm.options.EnableWal {
		if m, err := CreateWithWal(memtableId, lsm.pathOfWal(memtableId)); err != nil {
			return err
		} else {
			memtable = m
		}
	} else {
		memtable = CreateMemTable(memtableId)
	}

	if err := lsm.FreezeMemtableWithMemtable(memtable); err != nil {
		return err
	}
	if err := lsm.manifest.AddRecord(&NewMemtableRecord{NewMemtable: memtableId}); err != nil {
		return err
	}
	if err := lsm.SyncDir(); err != nil {
		return err
	}
	return nil
}

func (lsm *LsmStorageInner) ForceFlushNextImmMemtable() error {
	lsm.stateLock.Lock()
	defer lsm.stateLock.Unlock()

	var flushMemtable *MemTable
	{
		lsm.rwLock.RLock()
		if len(lsm.state.immMemTable) == 0 {
			panic("no imm memtable to flush")
		}
		flushMemtable = lsm.state.immMemTable[len(lsm.state.immMemTable)-1]
		lsm.rwLock.RUnlock()
	}

	builder := NewSsTableBuilder(lsm.options.BlockSize)
	err := flushMemtable.Flush(builder)
	if err != nil {
		return err
	}
	sstId := flushMemtable.id
	sst, err := builder.Build(sstId, lsm.blockCache, lsm.pathOfSst(sstId))
	if err != nil {
		return err
	}

	{
		lsm.rwLock.Lock()
		snapshot := lsm.state.snapshot()
		// remove the memtable from the immMemTable
		mem := snapshot.immMemTable[len(snapshot.immMemTable)-1]
		snapshot.immMemTable = snapshot.immMemTable[:len(snapshot.immMemTable)-1]
		utils.Assert(mem.id == sstId, "sst id not match")
		if lsm.compactionController.FlushToL0() {
			snapshot.l0SsTables = append([]uint32{sstId}, snapshot.l0SsTables...)
		} else {
			// In tiered compaction, create a new tier
			snapshot.levels = append([]*LevelSsTables{{level: sstId, ssTables: []uint32{sstId}}}, snapshot.levels...)
		}
		fmt.Printf("flushed %d.sst with size=%d\r\n", sstId, sst.TableSize())
		snapshot.sstables[sstId] = sst
		lsm.state = snapshot
		lsm.rwLock.Unlock()
	}
	if lsm.options.EnableWal {
		// remove the wal file
		if err := flushMemtable.Close(); err != nil {
			return err
		}
		if err := os.Remove(lsm.pathOfWal(sstId)); err != nil {
			return err
		}
	}
	err = lsm.manifest.AddRecord(&FlushRecord{Flush: sstId})
	if err != nil {
		return err
	}
	if err = lsm.SyncDir(); err != nil {
		return err
	}
	return nil

}

func (lsm *LsmStorageInner) SyncDir() error {
	//println("sync dir", lsm.path)
	if runtime.GOOS == "windows" {
		return nil
	}
	dir, err := os.OpenFile(lsm.path, os.O_RDONLY, 0666)
	//dir, err := os.Open(lsm.path)
	if err != nil {
		return err
	}
	//if err := syscall.Fsync(syscall.Handle(dir.Fd())); err != nil {
	//	return err
	//}
	if err := dir.Sync(); err != nil {
		return err
	}
	return dir.Close()
}

func pathOfSstStatic(path0 string, id uint32) string {
	return path.Join(path0, fmt.Sprintf("%05d.sst", id))
}

func (lsm *LsmStorageInner) pathOfSst(id uint32) string {
	return pathOfSstStatic(lsm.path, id)
}

func pathOfWalStatic(path0 string, id uint32) string {
	return path.Join(path0, fmt.Sprintf("%05d.wal", id))
}

func (lsm *LsmStorageInner) pathOfWal(id uint32) string {
	return pathOfWalStatic(lsm.path, id)
}

func keyWithin(userKey, tableBegin, tableEnd KeyType) bool {
	return tableBegin.Compare(userKey) <= 0 && userKey.Compare(tableEnd) <= 0
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
	// 从 immutable memtable 中查找
	for _, mt := range snapshot.immMemTable {
		if v := mt.Get(key); v != nil {
			if len(v) == 0 {
				return nil, nil
			}
			return v, nil
		}
	}
	// 没有的话，再下钻到 sstables 中查找
	// 从l0 和 l1 的sstables 中构建一个iterator
	// l0合并成 merge iterator
	// l1合并成 concat iterator
	// 在合并成 two merge iterator
	l0iters := make([]StorageIterator, 0, len(snapshot.l0SsTables))

	keepTable := func(key KeyType, table *SsTable) bool {
		if keyWithin(key, table.FirstKey(), table.LastKey()) {
			if table.bloom != nil {
				if table.bloom.MayContain(farm.Fingerprint32(key.Val)) {
					return true
				}
			} else {
				return true
			}
		}
		return false
	}

	for _, id := range snapshot.l0SsTables {
		sst := snapshot.sstables[id]
		if keepTable(key, sst) {
			sstIter, err := CreateSsTableIteratorAndSeekToKey(sst, key)
			if err != nil {
				return nil, err
			}
			l0iters = append(l0iters, sstIter)
		}
	}
	l0Iter := CreateMergeIterator(l0iters)
	levelIters := make([]StorageIterator, 0, len(snapshot.levels))
	for _, level := range snapshot.levels {
		levelSsts := make([]*SsTable, 0, len(level.ssTables))
		for _, id := range level.ssTables {
			sst := snapshot.sstables[id]
			if keepTable(key, sst) {
				levelSsts = append(levelSsts, sst)
			}
		}
		levelIter, err := CreateSstConcatIteratorAndSeekToKey(levelSsts, key)
		if err != nil {
			return nil, err
		}
		levelIters = append(levelIters, levelIter)
	}
	levelIter := CreateMergeIterator(levelIters)
	twoMergeIter, _ := CreateTwoMergeIterator(l0Iter, levelIter)
	if twoMergeIter.IsValid() && twoMergeIter.Key().Compare(key) == 0 && len(twoMergeIter.Value()) > 0 {
		return utils.Copy(twoMergeIter.Value()), nil
	}

	return nil, nil
}

const (
	Put uint8 = 1
	Del uint8 = 2
)

type WriteBatchRecord interface {
	Type() uint8
	IsWriteOp() bool
}

type PutOp struct {
	K []byte
	V []byte
}

func (p *PutOp) Type() uint8 {
	return Put
}

func (p *PutOp) IsWriteOp() bool {
	return true
}

type DelOp struct {
	K []byte
}

func (d *DelOp) Type() uint8 {
	return Del
}

func (d *DelOp) IsWriteOp() bool {
	return true
}

func (lsm *LsmStorageInner) WriteBatch(batch []WriteBatchRecord) error {
	for _, record := range batch {
		switch record.Type() {
		case Del:
			delOp := record.(*DelOp)
			utils.Assert(len(delOp.K) != 0, "key cannot be empty")
			var size uint32
			lsm.rwLock.RLock()
			err := lsm.state.memTable.Put(Key(delOp.K), []byte{})
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
		case Put:
			putOp := record.(*PutOp)
			utils.Assert(len(putOp.K) != 0, "key cannot be empty")
			utils.Assert(len(putOp.V) != 0, "value cannot be empty")

			var size uint32
			lsm.rwLock.RLock()
			err := lsm.state.memTable.Put(Key(putOp.K), putOp.V)
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
		}
	}
	return nil
}

func (lsm *LsmStorageInner) Put(key KeyType, value []byte) error {
	r := &PutOp{
		K: key.Val,
		V: value,
	}
	return lsm.WriteBatch([]WriteBatchRecord{r})
}

func (lsm *LsmStorageInner) Delete(key KeyType) error {
	r := &DelOp{
		K: key.Val,
	}
	return lsm.WriteBatch([]WriteBatchRecord{r})
}

func rangeOverlap(userBegin, userEnd KeyBound, tableBegin, tableEnd KeyType) bool {
	if userEnd.Type == Excluded && userEnd.Val.Compare(tableBegin) <= 0 {
		return false
	} else if userEnd.Type == Included && userEnd.Val.Compare(tableBegin) < 0 {
		return false
	}
	if userBegin.Type == Excluded && userBegin.Val.Compare(tableEnd) >= 0 {
		return false
	} else if userBegin.Type == Included && userBegin.Val.Compare(tableEnd) > 0 {
		return false
	}

	return true
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

	var l0Iters = make([]StorageIterator, 0, len(snapshot.l0SsTables))
	for _, id := range snapshot.l0SsTables {
		sst := snapshot.sstables[id]
		if rangeOverlap(lower, upper, sst.FirstKey(), sst.LastKey()) {
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
			l0Iters = append(l0Iters, sstableIter)
		}
	}
	// 构建l0 merge iter
	l0Iter := CreateMergeIterator(l0Iters)

	levelIters := make([]StorageIterator, 0, len(snapshot.levels))
	for _, level := range snapshot.levels {
		levelSsts := make([]*SsTable, 0, len(level.ssTables))
		for _, id := range level.ssTables {
			sst := snapshot.sstables[id]
			if rangeOverlap(lower, upper, sst.FirstKey(), sst.LastKey()) {
				levelSsts = append(levelSsts, sst)
			}
		}
		if lower.Type == Included {
			levelIter, err := CreateSstConcatIteratorAndSeekToKey(levelSsts, lower.Val)
			if err != nil {
				return nil, err
			}
			levelIters = append(levelIters, levelIter)
		} else if lower.Type == Excluded {
			levelIter, err := CreateSstConcatIteratorAndSeekToKey(levelSsts, lower.Val)
			if err != nil {
				return nil, err
			}
			if levelIter.IsValid() && levelIter.Key().Compare(lower.Val) == 0 {
				err := levelIter.Next()
				if err != nil {
					return nil, err
				}
			}
			levelIters = append(levelIters, levelIter)
		} else {
			levelIter, err := CreateSstConcatIteratorAndSeekToFirst(levelSsts)
			if err != nil {
				return nil, err
			}
			levelIters = append(levelIters, levelIter)
		}
	}
	levelIter := CreateMergeIterator(levelIters)

	// PrintIter(memtableMergeIter)

	// 构建memtable 和l0 的two merge iter
	twoMergeIter, err := CreateTwoMergeIterator(memtableMergeIter, l0Iter)
	if err != nil {
		return nil, err
	}
	// PrintIter(twoMergeIter)
	// 再和l1的concat iter 合并
	lsmIteratorInner, err := CreateTwoMergeIterator(twoMergeIter, levelIter)
	if err != nil {
		return nil, err
	}

	// PrintIter(lsmIterator)
	lsmIterator, err := CreateLsmIterator(lsmIteratorInner, upper)
	if err != nil {
		return nil, err
	}

	return CreateFusedIterator(lsmIterator), nil
}
