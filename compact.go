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
	compactionType CompactionType
	task           interface{}
}

type ForceFullCompactionTask struct {
	l0Sstables []uint32
	l1Sstables []uint32
}

func (ct *CompactionTask) compactToBottomLevel() bool {
	if ct.compactionType == ForceFull {
		return true
	} else if ct.compactionType == Leveled {
		task := ct.task.(*SimpleLeveledCompactionTask)
		return task.IsLowerLevelBottomLevel
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
			compactionType: Simple,
			task:           task,
		}
	case NoCompaction:
		panic("unreachable!")
	default:
		panic("no compaction controller")
	}
}

func (cc *CompactionController) ApplyCompactionResult(task *CompactionTask, snapshot *LsmStorageState, output []uint32) (*LsmStorageState, []uint32) {
	utils.Assert(task.compactionType == cc.CompactionType, "compaction type not match")
	switch cc.CompactionType {
	case Simple:
		slcc := cc.Controller.(*SimpleLeveledCompactionController)
		slct := task.task.(*SimpleLeveledCompactionTask)
		return slcc.ApplyCompactionResult(slct, snapshot, output)
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
	for iter.IsValid() {
		if builder == nil {
			builder = NewSsTableBuilder(lsm.options.BlockSize)
		}
		if compactToBottomLevel {
			if len(iter.Value()) > 0 {
				builder.Add(iter.Key(), iter.Value())
			}
		} else {
			builder.Add(iter.Key(), iter.Value())
		}
		err := iter.Next()
		if err != nil {
			return nil, err
		}
		if builder.EstimatedSize() >= int(lsm.options.TargetSstSize) {
			sstId := lsm.getNextSstId()
			sst, err := builder.Build(sstId, lsm.blockCache, lsm.pathOfSst(sstId))
			if err != nil {
				return nil, err
			}
			newSst = append(newSst, sst)
			builder = nil
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

	switch task.compactionType {
	case ForceFull:
		fcTask := task.task.(*ForceFullCompactionTask)
		l0Iters := make([]StorageIterator, 0, len(fcTask.l0Sstables))
		for _, id := range fcTask.l0Sstables {
			sstIter, err := CreateSsTableIteratorAndSeekToFirst(snapshot.sstables[id])
			if err != nil {
				return nil, err
			}
			l0Iters = append(l0Iters, sstIter)
		}
		l1Ssts := make([]*SsTable, 0, len(fcTask.l1Sstables))
		for _, id := range fcTask.l1Sstables {
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
		slct := task.task.(*SimpleLeveledCompactionTask)
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

	l0SsTables := make([]uint32, 0, len(snapshot.sstables))
	l0SsTables = append(l0SsTables, snapshot.l0SsTables...)
	l1SsTables := make([]uint32, 0, len(snapshot.levels[0].ssTables))
	l1SsTables = append(l1SsTables, snapshot.levels[0].ssTables...)
	compactionTask := &CompactionTask{
		compactionType: ForceFull,
		task: &ForceFullCompactionTask{
			l0Sstables: l0SsTables,
			l1Sstables: l1SsTables,
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
	lsm.state = state
	return removeSsts
}

func (lsm *LsmStorageInner) TriggerCompaction() error {
	snapshot := ReadLsmStorageState(lsm, func(state *LsmStorageState) *LsmStorageState {
		return state
	})
	task := lsm.compactionController.GenerateCompactionTask(snapshot)
	if utils.IsNil(task.task) {
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
	lsm.stateLock.Lock()
	snapshot = ReadLsmStorageState(lsm, func(state *LsmStorageState) *LsmStorageState {
		return state.snapshot()
	})
	newSstIds := make([]uint32, 0)
	for _, sst := range ssts {
		newSstIds = append(newSstIds, sst.Id())
		_, ok := snapshot.sstables[sst.Id()]
		utils.Assert(!ok, "sstables already exists")
		snapshot.sstables[sst.Id()] = sst
	}
	snapshot, filesToRemove := lsm.compactionController.ApplyCompactionResult(task, snapshot, output)
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
	lsm.stateLock.Unlock()
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
