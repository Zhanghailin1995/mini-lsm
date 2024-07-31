package mini_lsm

import (
	"errors"
	"fmt"
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"github.com/sirupsen/logrus"
	"os"
	"slices"
	"time"
)

type CompactionType int

const (
	NoCompaction CompactionType = iota
	Leveled
	Tiered
	Simple
	ForceFull
)

type CompactionTask struct {
	CompactionType CompactionType `json:"compactionType"`
	Task           interface{}    `json:"task"`
}

type ForceFullCompactionTask struct {
	L0Sstables []uint32 `json:"l0Sstables"`
	L1Sstables []uint32 `json:"l1Sstables"`
}

func (ct *CompactionTask) compactToBottomLevel() bool {
	if ct.CompactionType == ForceFull {
		return true
	} else if ct.CompactionType == Leveled {
		task := ct.Task.(*LeveledCompactionTask)
		return task.IsLowerLevelBottomLevel
	} else if ct.CompactionType == Simple {
		task := ct.Task.(*SimpleLeveledCompactionTask)
		return task.IsLowerLevelBottomLevel
	} else if ct.CompactionType == Tiered {
		task := ct.Task.(*TieredCompactionTask)
		return task.BottomTierIncluded
	}
	return false
}

type CompactionController struct {
	CompactionType CompactionType
	Controller     interface{}
}

func (cc *CompactionController) GenerateCompactionTask(snapshot *LsmStorageState) *CompactionTask {
	switch cc.CompactionType {
	case Simple:
		task := cc.Controller.(*SimpleLeveledCompactionController).GenerateCompactionTask(snapshot)
		return &CompactionTask{
			CompactionType: Simple,
			Task:           task,
		}
	case Tiered:
		task := cc.Controller.(*TieredCompactionController).GenerateCompactionTask(snapshot)
		return &CompactionTask{
			CompactionType: Tiered,
			Task:           task,
		}
	case Leveled:
		task := cc.Controller.(*LeveledCompactionController).GenerateCompactionTask(snapshot)
		return &CompactionTask{
			CompactionType: Leveled,
			Task:           task,
		}
	case NoCompaction:
		panic("unreachable!")
	default:
		panic("no compaction controller")
	}
}

func (cc *CompactionController) ApplyCompactionResult(snapshot *LsmStorageState, task *CompactionTask, output []uint32) (*LsmStorageState, []uint32) {
	utils.Assert(task.CompactionType == cc.CompactionType, "compaction type not match")
	switch cc.CompactionType {
	case Simple:
		slcc := cc.Controller.(*SimpleLeveledCompactionController)
		slct, ok := task.Task.(*SimpleLeveledCompactionTask)
		if !ok {
			panic("controller type not match")
		}
		return slcc.ApplyCompactionResult(snapshot, slct, output)
	case Tiered:
		tcc := cc.Controller.(*TieredCompactionController)
		tct, ok := task.Task.(*TieredCompactionTask)
		if !ok {
			panic("controller type not match")
		}
		return tcc.ApplyCompactionResult(snapshot, tct, output)
	case Leveled:
		lcc := cc.Controller.(*LeveledCompactionController)
		lct, ok := task.Task.(*LeveledCompactionTask)
		if !ok {
			panic("controller type not match")
		}
		return lcc.ApplyCompactionResult(snapshot, lct, output)
	default:
		panic("unreachable!")
	}
}

func (cc *CompactionController) FlushToL0() bool {
	if cc.CompactionType == Simple || cc.CompactionType == NoCompaction || cc.CompactionType == Leveled {
		return true
	}
	return false
}

type CompactionOptions struct {
	CompactionType CompactionType
	Opt            interface{}
}

func (lsm *LsmStorageInner) compactGenerateSstFromIter(iter StorageIterator, compactToBottomLevel bool) ([]*SsTable, error) {
	var builder *SsTableBuilder = nil
	newSst := make([]*SsTable, 0)
	watermark := lsm.Mvcc().Watermark()
	lastKey := make([]byte, 0)
	// 这个参数的作用是记录一个key如果有多个版本，那么他的最新版本是不是在watermark之下，如果是，也是要保留的
	firstKeyBelowWatermark := false
	for iter.IsValid() {
		if builder == nil {
			builder = NewSsTableBuilder(lsm.options.BlockSize)
		}
		sameAsLastKey := slices.Equal(iter.Key().(KeyBytes).KeyRef(), lastKey)
		if !sameAsLastKey {
			firstKeyBelowWatermark = true
		}
		//Now that we have a watermark for the system, we can clean up unused versions during the compaction process.
		//
		//	If a version of a key is above watermark, keep it.
		//	For all versions of a key below or equal to the watermark, keep the latest version.
		//	For example, if we have watermark=3 and the following data:
		//
		//
		//a@4=del <- above watermark
		//a@3=3   <- latest version below or equal to watermark
		//a@2=2   <- can be removed, no one will read it
		//a@1=1   <- can be removed, no one will read it
		//b@1=1   <- latest version below or equal to watermark
		//c@4=4   <- above watermark
		//d@3=del <- can be removed if compacting to bottom-most level
		//d@2=2   <- can be removed
		//If we do a compaction over these keys, we will get:
		//
		//a@4=del
		//a@3=3
		//b@1=1
		//c@4=4
		//d@3=del (can be removed if compacting to bottom-most level)
		//Assume these are all keys in the engine. If we do a scan at ts=3, we will get a=3,b=1,c=4
		//before/after compaction. If we do a scan at ts=4, we will get b=1,c=4 before/after compaction.
		//Compaction will not and should not affect transactions with read timestamp >= watermark.
		// 这个条件就是如果这个key是删除操作，且他的ts小于watermark, 而且compactToBottomLevel，则可以直接丢弃这个key了
		if compactToBottomLevel &&
			!sameAsLastKey &&
			iter.Key().(KeyBytes).Ts <= watermark &&
			len(iter.Value()) == 0 {
			lastKey = append(lastKey[:0], iter.Key().(KeyBytes).KeyRef()...)
			err := iter.Next()
			if err != nil {
				return nil, err
			}
			firstKeyBelowWatermark = false
			continue
		}

		if sameAsLastKey && iter.Key().(KeyBytes).Ts <= watermark {
			if !firstKeyBelowWatermark {
				err := iter.Next()
				if err != nil {
					return nil, err
				}
				continue
			}
			firstKeyBelowWatermark = false
		}
		if builder.EstimatedSize() >= int(lsm.options.TargetSstSize) && !sameAsLastKey {
			sstId := lsm.getNextSstId()
			oldBuilder := builder
			sst, err := oldBuilder.Build(sstId, lsm.blockCache, lsm.pathOfSst(sstId))
			if err != nil {
				return nil, err
			}
			newSst = append(newSst, sst)
			builder = NewSsTableBuilder(lsm.options.BlockSize)
		}
		builder.Add(iter.Key().(KeyBytes), iter.Value())
		if !sameAsLastKey {
			lastKey = append(lastKey[:0], iter.Key().(KeyBytes).KeyRef()...)
		}
		err := iter.Next()
		if err != nil {
			return nil, err
		}
	}
	if builder != nil {
		sstId := lsm.getNextSstId()
		sst, err := builder.Build(sstId, lsm.blockCache, lsm.pathOfSst(sstId))
		if err != nil {
			return nil, err
		}
		newSst = append(newSst, sst)
	}
	return newSst, nil
}

func (lsm *LsmStorageInner) compact(task *CompactionTask) ([]*SsTable, error) {
	lsm.rwLock.RLock()
	snapshot := lsm.state
	lsm.rwLock.RUnlock()

	var storageIter StorageIterator

	switch task.CompactionType {
	case ForceFull:
		fcTask := task.Task.(*ForceFullCompactionTask)
		l0Iters := make([]StorageIterator, 0, len(fcTask.L0Sstables))
		for _, id := range fcTask.L0Sstables {
			sstIter, err := CreateSsTableIteratorAndSeekToFirst(snapshot.sstables[id])
			if err != nil {
				return nil, err
			}
			l0Iters = append(l0Iters, sstIter)
		}
		l1Ssts := make([]*SsTable, 0, len(fcTask.L1Sstables))
		for _, id := range fcTask.L1Sstables {
			sst, ok := snapshot.sstables[id]
			if !ok {
				return nil, errors.New(fmt.Sprintf("sstable %d not found", id))
			}
			l1Ssts = append(l1Ssts, sst)
		}
		sstConcatIter, err := CreateSstConcatIteratorAndSeekToFirst(l1Ssts)
		if err != nil {
			return nil, err
		}
		twoMergeIter, err := CreateTwoMergeIterator(CreateMergeIterator(l0Iters), sstConcatIter)
		if err != nil {
			return nil, err
		}
		storageIter = twoMergeIter
		return lsm.compactGenerateSstFromIter(storageIter, task.compactToBottomLevel())
	case Simple:
		slct := task.Task.(*SimpleLeveledCompactionTask)
		if slct.UpperLevel != nil {
			upperSsts := make([]*SsTable, 0, len(slct.UpperLevelSstIds))
			for _, id := range slct.UpperLevelSstIds {
				sst, ok := snapshot.sstables[id]
				if !ok {
					return nil, errors.New(fmt.Sprintf("sstable %d not found", id))
				}
				upperSsts = append(upperSsts, sst)
			}
			upperIter, err := CreateSstConcatIteratorAndSeekToFirst(upperSsts)
			if err != nil {
				return nil, err
			}
			lowerSsts := make([]*SsTable, 0, len(slct.LowerLevelSstIds))
			for _, id := range slct.LowerLevelSstIds {
				sst, ok := snapshot.sstables[id]
				if !ok {
					return nil, errors.New(fmt.Sprintf("sstable %d not found", id))
				}
				lowerSsts = append(lowerSsts, sst)
			}
			lowerIter, err := CreateSstConcatIteratorAndSeekToFirst(lowerSsts)
			if err != nil {
				return nil, err
			}
			twoMergeIter, err := CreateTwoMergeIterator(upperIter, lowerIter)
			if err != nil {
				return nil, err
			}
			storageIter = twoMergeIter
			return lsm.compactGenerateSstFromIter(storageIter, task.compactToBottomLevel())
		} else {
			upperIters := make([]StorageIterator, 0, len(slct.UpperLevelSstIds))
			for _, id := range slct.UpperLevelSstIds {
				sst, ok := snapshot.sstables[id]
				if !ok {
					return nil, errors.New(fmt.Sprintf("sstable %d not found", id))
				}
				sstIter, err := CreateSsTableIteratorAndSeekToFirst(sst)
				if err != nil {
					return nil, err
				}
				upperIters = append(upperIters, sstIter)
			}
			upperIter := CreateMergeIterator(upperIters)
			lowerSsts := make([]*SsTable, 0, len(slct.LowerLevelSstIds))
			for _, id := range slct.LowerLevelSstIds {
				sst, ok := snapshot.sstables[id]
				if !ok {
					return nil, errors.New(fmt.Sprintf("sstable %d not found", id))
				}
				lowerSsts = append(lowerSsts, sst)
			}
			lowerIter, err := CreateSstConcatIteratorAndSeekToFirst(lowerSsts)
			if err != nil {
				return nil, err
			}
			twoMergeIter, err := CreateTwoMergeIterator(upperIter, lowerIter)
			if err != nil {
				return nil, err
			}
			storageIter = twoMergeIter
			return lsm.compactGenerateSstFromIter(storageIter, task.compactToBottomLevel())
		}
	case Leveled:
		lct := task.Task.(*LeveledCompactionTask)
		if lct.UpperLevel != nil {
			upperSsts := make([]*SsTable, 0, len(lct.UpperLevelSstIds))
			for _, id := range lct.UpperLevelSstIds {
				sst, ok := snapshot.sstables[id]
				if !ok {
					return nil, errors.New(fmt.Sprintf("sstable %d not found", id))
				}
				upperSsts = append(upperSsts, sst)
			}
			upperIter, err := CreateSstConcatIteratorAndSeekToFirst(upperSsts)
			if err != nil {
				return nil, err
			}
			lowerSsts := make([]*SsTable, 0, len(lct.LowerLevelSstIds))
			for _, id := range lct.LowerLevelSstIds {
				sst, ok := snapshot.sstables[id]
				if !ok {
					return nil, errors.New(fmt.Sprintf("sstable %d not found", id))
				}
				lowerSsts = append(lowerSsts, sst)
			}
			lowerIter, err := CreateSstConcatIteratorAndSeekToFirst(lowerSsts)
			if err != nil {
				return nil, err
			}
			twoMergeIter, err := CreateTwoMergeIterator(upperIter, lowerIter)
			if err != nil {
				return nil, err
			}
			storageIter = twoMergeIter
			return lsm.compactGenerateSstFromIter(storageIter, task.compactToBottomLevel())
		} else {
			upperIters := make([]StorageIterator, 0, len(lct.UpperLevelSstIds))
			for _, id := range lct.UpperLevelSstIds {
				sst, ok := snapshot.sstables[id]
				if !ok {
					return nil, errors.New(fmt.Sprintf("sstable %d not found", id))
				}
				sstIter, err := CreateSsTableIteratorAndSeekToFirst(sst)
				if err != nil {
					return nil, err
				}
				upperIters = append(upperIters, sstIter)
			}
			upperIter := CreateMergeIterator(upperIters)
			lowerSsts := make([]*SsTable, 0, len(lct.LowerLevelSstIds))
			for _, id := range lct.LowerLevelSstIds {
				sst, ok := snapshot.sstables[id]
				if !ok {
					return nil, errors.New(fmt.Sprintf("sstable %d not found", id))
				}
				lowerSsts = append(lowerSsts, sst)
			}
			lowerIter, err := CreateSstConcatIteratorAndSeekToFirst(lowerSsts)
			if err != nil {
				return nil, err
			}
			twoMergeIter, err := CreateTwoMergeIterator(upperIter, lowerIter)
			if err != nil {
				return nil, err
			}
			storageIter = twoMergeIter
			return lsm.compactGenerateSstFromIter(storageIter, task.compactToBottomLevel())
		}
	case Tiered:
		tct := task.Task.(*TieredCompactionTask)
		tiers := make([]StorageIterator, 0, len(tct.Tiers))
		for _, tier := range tct.Tiers {
			ssts := make([]*SsTable, 0, len(tier.SstIds))
			for _, id := range tier.SstIds {
				sst, ok := snapshot.sstables[id]
				if !ok {
					return nil, errors.New(fmt.Sprintf("sstable %d not found", id))
				}
				ssts = append(ssts, sst)
			}
			iter, err := CreateSstConcatIteratorAndSeekToFirst(ssts)
			if err != nil {
				return nil, err
			}
			tiers = append(tiers, iter)
		}
		return lsm.compactGenerateSstFromIter(CreateMergeIterator(tiers), task.compactToBottomLevel())
	default:
		return nil, errors.New("unsupported compaction type")
	}
}

func (lsm *LsmStorageInner) ForceFullCompaction() error {
	if lsm.options.CompactionOptions.CompactionType != NoCompaction {
		panic("full compaction can only be called with compaction is not enabled")
	}

	lsm.rwLock.RLock()
	snapshot := lsm.state
	lsm.rwLock.RUnlock()
	// 简单描述一下不加锁为什么是安全的，因为我们采用的是写时复制，只需要保证在创建快照时是安全的即可
	// 上面的snapshot是一个对lsm state的引用，并且是本地的变量，不会被其他线程修改
	// 我们对其访问完全无需加锁，因为其他写线程并不会更新这个引用，写线程通过写锁完全复制一个快照，这个快照的引用是不指向
	// 我们拿到的这个引用快照的，写线程在快照上更新状态，最后把这个快照更新到lsm state上，即使已经更新了lsm state，我们拿到的这个引用快照依旧是有效的
	// 写线程全程持有state lock所以是安全的

	l0SsTables := make([]uint32, 0, len(snapshot.sstables))
	l0SsTables = append(l0SsTables, snapshot.l0SsTables...)
	l1SsTables := make([]uint32, 0, len(snapshot.levels[0].ssTables))
	l1SsTables = append(l1SsTables, snapshot.levels[0].ssTables...)
	compactionTask := &CompactionTask{
		CompactionType: ForceFull,
		Task: &ForceFullCompactionTask{
			L0Sstables: l0SsTables,
			L1Sstables: l1SsTables,
		},
	}
	ssts, err := lsm.compact(compactionTask)
	if err != nil {
		return err
	}

	removeSsts := lsm.updateState(ssts, l0SsTables, l1SsTables)
	err = lsm.removeCompactedSst(removeSsts)
	if err != nil {
		return err
	}
	return nil
}

func (lsm *LsmStorageInner) removeCompactedSst(ssts []*SsTable) error {
	for _, sst := range ssts {
		err := sst.CloseSstFile()
		if err != nil {
			return err
		}
		err = os.Remove(lsm.pathOfSst(sst.Id()))
		if err != nil {
			return err
		}
	}
	return nil
}

func (lsm *LsmStorageInner) updateState(ssts []*SsTable, l0Sstables []uint32, l1Sstables []uint32) []*SsTable {
	lsm.stateLock.Lock()
	defer lsm.stateLock.Unlock()
	lsm.rwLock.RLock()
	state := lsm.state.snapshot()
	lsm.rwLock.RUnlock()

	chains := make([]uint32, 0, len(l0Sstables)+len(l1Sstables))
	chains = append(chains, l0Sstables...)
	chains = append(chains, l1Sstables...)
	removeSsts := make([]*SsTable, 0, len(chains))
	for _, sstId := range chains {
		sst, ok := state.sstables[sstId]
		if !ok {
			panic(fmt.Sprintf("sstable %d not found", sstId))
		}
		delete(state.sstables, sstId)
		removeSsts = append(removeSsts, sst)
	}
	ids := make([]uint32, 0, len(ssts))
	for _, sst := range ssts {
		ids = append(ids, sst.Id())
		if _, ok := state.sstables[sst.Id()]; ok {
			panic(fmt.Sprintf("sstable %d already exists", sst.Id()))
		}
		state.sstables[sst.Id()] = sst
	}
	// 这里因为持有锁 state lock,所以 levels的内容不可能被改变
	utils.Assert(slices.Equal(state.levels[0].ssTables, l1Sstables), "l1 sstables not match")
	state.levels[0].ssTables = ids
	l0SsTablesMap := make(map[uint32]struct{})
	for _, id := range l0Sstables {
		l0SsTablesMap[id] = struct{}{}
	}
	newL0SsTables := make([]uint32, 0, len(state.l0SsTables))
	for _, id := range state.l0SsTables {
		if _, ok := l0SsTablesMap[id]; !ok {
			newL0SsTables = append(newL0SsTables, id)
		}
		delete(l0SsTablesMap, id)
	}
	utils.Assert(len(l0SsTablesMap) == 0, "l0 sstables not match")
	state.l0SsTables = newL0SsTables
	UpdateLsmStorageState(lsm, func(inner *LsmStorageInner) {
		inner.state = state
	})
	return removeSsts
}

func (lsm *LsmStorageInner) TriggerCompaction() error {
	snapshot := ReadLsmStorageState(lsm, func(state *LsmStorageState) *LsmStorageState {
		return state
	})
	task := lsm.compactionController.GenerateCompactionTask(snapshot)
	if utils.IsNil(task.Task) {
		return nil
	}
	println("running compaction task")
	ssts, err := lsm.compact(task)
	if err != nil {
		return err
	}
	filesAdded := len(ssts)
	output := make([]uint32, 0, filesAdded)
	for _, sst := range ssts {
		output = append(output, sst.Id())
	}
	sstsToRemove, filesToRemove, err := lsm.triggerCompaction(ssts, task, output)
	if err != nil {
		return err
	}
	fmt.Printf("compaction done, %d files added, %d files removed\n", filesAdded, len(filesToRemove))
	err = lsm.removeCompactedSst(sstsToRemove)
	if err != nil {
		return err
	}
	err = lsm.SyncDir()
	if err != nil {
		return err
	}
	return nil
}

func (lsm *LsmStorageInner) triggerCompaction(ssts []*SsTable, task *CompactionTask, output []uint32) ([]*SsTable, []uint32, error) {
	lsm.stateLock.Lock()
	defer lsm.stateLock.Unlock()
	snapshot := ReadLsmStorageState(lsm, func(state *LsmStorageState) *LsmStorageState {
		return state.snapshot()
	})
	newSstIds := make([]uint32, 0)
	for _, sst := range ssts {
		newSstIds = append(newSstIds, sst.Id())
		_, ok := snapshot.sstables[sst.Id()]
		utils.Assert(!ok, "sstables already exists")
		snapshot.sstables[sst.Id()] = sst
	}
	snapshot, filesToRemove := lsm.compactionController.ApplyCompactionResult(snapshot, task, output)
	sstsToRemove := make([]*SsTable, 0, len(filesToRemove))
	for _, id := range filesToRemove {
		sst, ok := snapshot.sstables[id]
		if !ok {
			panic(fmt.Sprintf("sstable %d not found", id))
		}
		sstsToRemove = append(sstsToRemove, sst)
		delete(snapshot.sstables, id)
	}
	lsm.rwLock.Lock()
	lsm.state = snapshot
	lsm.rwLock.Unlock()
	err := lsm.SyncDir()
	if err != nil {
		return nil, nil, err
	}

	err = lsm.manifest.AddRecord(&CompactionRecord{
		CompactionTask: task,
		SstIds:         output,
	})
	if err != nil {
		return nil, nil, err
	}
	return sstsToRemove, filesToRemove, nil
}

func (lsm *LsmStorageInner) triggerFlush() error {
	lsm.rwLock.RLock()
	flush := len(lsm.state.immMemTable) >= int(lsm.options.NumberMemTableLimit)
	lsm.rwLock.RUnlock()
	if flush {
		return lsm.ForceFlushNextImmMemtable()
	}
	return nil
}

func (lsm *LsmStorageInner) SpawnFlushThread(rx, tx chan struct{}) {
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				//logrus.Infof("trigger flush")
				err := lsm.triggerFlush()
				if err != nil {
					logrus.Errorf("trigger flush error: %v", err)
				}
			case <-rx:
				logrus.Infof("recv shutdown flush thread signal")
				tx <- struct{}{}
				return
			}
		}
	}()
}

func (lsm *LsmStorageInner) SpawnCompactionThread(rx, tx chan struct{}) {
	if lsm.options.CompactionOptions.CompactionType == NoCompaction {
		go func() {
			<-rx
			tx <- struct{}{}
		}()
		return
	}
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				err := lsm.TriggerCompaction()
				if err != nil {
					logrus.Errorf("trigger compaction error: %v", err)
				}
			case <-rx:
				logrus.Infof("recv shutdown compaction thread signal")
				tx <- struct{}{}
				return
			}
		}
	}()
}
