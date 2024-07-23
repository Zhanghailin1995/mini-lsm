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
	}
	return false
}

const (
	NoCompaction CompactionType = iota
	Leveled
	Tiered
	Simple
	ForceFull
)

type CompactionOptions struct {
	CompactionType CompactionType
	Opt            interface{}
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
		break
	default:
		return nil, errors.New("unsupported compaction type")
	}
	var builder *SsTableBuilder = nil
	ssts := make([]*SsTable, 0)
	ctbl := task.compactToBottomLevel()

	for storageIter.IsValid() {
		if builder == nil {
			builder = NewSsTableBuilder(lsm.options.BlockSize)
		}
		if ctbl {
			if len(storageIter.Value()) > 0 {
				builder.Add(storageIter.Key(), storageIter.Value())
			}
		} else {
			builder.Add(storageIter.Key(), storageIter.Value())
		}
		err := storageIter.Next()
		if err != nil {
			return nil, err
		}
		if builder.EstimatedSize() >= int(lsm.options.TargetSstSize) {
			sstId := lsm.getNextSstId()
			sst, err := builder.Build(sstId, lsm.blockCache, lsm.pathOfSst(sstId))
			if err != nil {
				return nil, err
			}
			ssts = append(ssts, sst)
			builder = nil
		}
	}

	if builder != nil {
		sstId := lsm.getNextSstId()
		sst, err := builder.Build(sstId, lsm.blockCache, lsm.pathOfSst(sstId))
		if err != nil {
			return nil, err
		}
		ssts = append(ssts, sst)
	}
	return ssts, nil
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

//func (lsm *LsmStorageInner) removeCompactedSst(l0SsTables []uint32, l1SsTables []uint32) error {
//	chains := make([]uint32, 0, len(l0SsTables)+len(l1SsTables))
//	chains = append(chains, l0SsTables...)
//	chains = append(chains, l1SsTables...)
//	if runtime.GOOS == "linux" {
//		for _, id := range chains {
//			err := os.Remove(lsm.pathOfSst(id))
//			if err != nil {
//				return err
//			}
//		}
//	} else if runtime.GOOS == "windows" {
//		// TODO windows下的删除文件
//		lsm.stateLock.Lock()
//		// defer竟然不是离开作用域执行，简直是屎一样的设计
//		lsm.rwLock.Lock()
//		snapshot := lsm.state.snapshot()
//		removeSsts := make([]*SsTable, 0, len(chains))
//		for _, id := range chains {
//			table, ok := snapshot.sstables[id]
//			if ok {
//				delete(snapshot.sstables, id)
//				removeSsts = append(removeSsts, table)
//			}
//		}
//		lsm.state = snapshot
//		lsm.rwLock.Unlock()
//		lsm.stateLock.Unlock()
//
//		for _, sst := range removeSsts {
//			err := sst.CloseSstFile()
//			if err != nil {
//				return err
//			}
//			err = os.Remove(lsm.pathOfSst(sst.Id()))
//			if err != nil {
//				return err
//			}
//		}
//	}
//	return nil
//}

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
