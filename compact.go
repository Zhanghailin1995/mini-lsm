package mini_lsm

import (
	"github.com/sirupsen/logrus"
	"time"
)

type CompactionType int

type CompactionTask struct {
	compactionType CompactionType
	task           interface{}
}

type ForceFullCompactionTask struct {
	sstIds []uint32
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
	compactionType CompactionType
	opt            interface{}
}

func (lsm *LsmStorageInner) triggerFlush() error {
	lsm.rwLock.RLock()
	flush := len(lsm.state.immMemTable) >= int(lsm.options.numberMemTableLimit)
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
				logrus.Infof("trigger flush")
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
