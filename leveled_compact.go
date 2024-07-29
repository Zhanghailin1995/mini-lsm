package mini_lsm

import (
	"fmt"
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"slices"
	"sort"
)

type LeveledCompactionTask struct {
	UpperLevel              *uint32
	UpperLevelSstIds        []uint32
	LowerLevel              uint32
	LowerLevelSstIds        []uint32
	IsLowerLevelBottomLevel bool
}

type LeveledCompactionOptions struct {
	LevelSizeMultiplier            uint32
	Level0FileNumCompactionTrigger uint32
	MaxLevels                      uint32
	BaseLevelSizeMb                uint32
}

type LeveledCompactionController struct {
	Options *LeveledCompactionOptions
}

func NewLeveledCompactionController(options *LeveledCompactionOptions) *LeveledCompactionController {
	return &LeveledCompactionController{
		Options: options,
	}
}

func (lcc *LeveledCompactionController) FindOverlappingSsts(snapshot *LsmStorageState, sstIds []uint32, inLevel uint32) []uint32 {
	var beginKey, endKey KeyBytes
	minKeySet, maxKeySet := false, false

	for _, id := range sstIds {
		sst := snapshot.sstables[id]
		if !minKeySet || sst.FirstKey().Compare(beginKey) < 0 {
			beginKey = sst.FirstKey()
			minKeySet = true
		}
		if !maxKeySet || sst.LastKey().Compare(endKey) > 0 {
			endKey = sst.LastKey()
			maxKeySet = true
		}
	}

	var overlapSsts []uint32
	for _, sstId := range snapshot.levels[inLevel-1].ssTables {
		sst := snapshot.sstables[sstId]
		firstKey := sst.FirstKey()
		lastKey := sst.LastKey()
		if !(lastKey.Compare(beginKey) < 0 || firstKey.Compare(endKey) > 0) {
			overlapSsts = append(overlapSsts, sstId)
		}
	}
	return overlapSsts
}

func (lcc *LeveledCompactionController) GenerateCompactionTask(snapshot *LsmStorageState) *LeveledCompactionTask {
	targetLevelSize := make([]uint32, lcc.Options.MaxLevels)
	realLevelSize := make([]uint32, 0, lcc.Options.MaxLevels)
	baseLevel := lcc.Options.MaxLevels
	baseLevelSizeBytes := lcc.Options.BaseLevelSizeMb * 1024 * 1024

	for i := uint32(0); i < lcc.Options.MaxLevels; i++ {
		tableSize := int64(0)
		for _, sstId := range snapshot.levels[i].ssTables {
			tableSize += snapshot.sstables[sstId].TableSize()
		}
		realLevelSize = append(realLevelSize, uint32(tableSize))

	}
	targetLevelSize[lcc.Options.MaxLevels-1] = utils.MaxU32(realLevelSize[lcc.Options.MaxLevels-1], baseLevelSizeBytes)

	for i := int(lcc.Options.MaxLevels) - 1 - 1; i >= 0; i-- {
		nextLevelSize := targetLevelSize[i+1]
		thisLevelSize := nextLevelSize / lcc.Options.LevelSizeMultiplier
		if nextLevelSize > baseLevelSizeBytes {
			targetLevelSize[i] = thisLevelSize
		}
		if targetLevelSize[i] > 0 {
			baseLevel = uint32(i) + 1
		}
	}

	if len(snapshot.l0SsTables) >= int(lcc.Options.Level0FileNumCompactionTrigger) {
		fmt.Printf("flush L0 SST to base level %d\n", baseLevel)
		return &LeveledCompactionTask{
			UpperLevel:              nil,
			UpperLevelSstIds:        slices.Clone(snapshot.l0SsTables),
			LowerLevel:              baseLevel,
			LowerLevelSstIds:        lcc.FindOverlappingSsts(snapshot, snapshot.l0SsTables, baseLevel),
			IsLowerLevelBottomLevel: baseLevel == lcc.Options.MaxLevels,
		}
	}

	priorities := make([]struct {
		Prio  float64
		Level uint32
	}, 0, lcc.Options.MaxLevels)
	for level := uint32(0); level < lcc.Options.MaxLevels; level++ {
		prio := float64(realLevelSize[level]) / float64(targetLevelSize[level])
		if prio > 1.0 {
			priorities = append(priorities, struct {
				Prio  float64
				Level uint32
			}{Prio: prio, Level: level + 1})
		}
	}
	sort.Slice(priorities, func(i, j int) bool {
		return priorities[i].Prio > priorities[j].Prio
	})

	if len(priorities) > 0 {
		fmt.Printf("target level sizes: %v, real level sizes: %v, base_level: %d\n", targetLevelSize, realLevelSize, baseLevel)
		level := priorities[0].Level
		selectedSst := slices.Min(snapshot.levels[level-1].ssTables) // select the oldest sst to compact
		fmt.Printf("compaction triggered by priority: level %d out of %v, select %d for compaction\n", level, priorities, selectedSst)
		return &LeveledCompactionTask{
			UpperLevel:              &level,
			UpperLevelSstIds:        []uint32{selectedSst},
			LowerLevel:              level + 1,
			LowerLevelSstIds:        lcc.FindOverlappingSsts(snapshot, []uint32{selectedSst}, level+1),
			IsLowerLevelBottomLevel: level+1 == lcc.Options.MaxLevels,
		}
	}
	return nil
}

func (lcc *LeveledCompactionController) ApplyCompactionResult(snapshot *LsmStorageState, task *LeveledCompactionTask, output []uint32) (*LsmStorageState, []uint32) {
	newSnapshot := snapshot.snapshot() // Assuming snapshot() is a deep copy method
	var filesToRemove []uint32
	upperLevelSstIdsSet := make(map[uint32]struct{})
	for _, id := range task.UpperLevelSstIds {
		upperLevelSstIdsSet[id] = struct{}{}
	}
	lowerLevelSstIdsSet := make(map[uint32]struct{})
	for _, id := range task.LowerLevelSstIds {
		lowerLevelSstIdsSet[id] = struct{}{}
	}
	if task.UpperLevel != nil {
		var newUpperLevelSsts []uint32
		for _, id := range newSnapshot.levels[*task.UpperLevel-1].ssTables {
			if _, ok := upperLevelSstIdsSet[id]; ok {
				delete(upperLevelSstIdsSet, id)
			} else {
				newUpperLevelSsts = append(newUpperLevelSsts, id)
			}
		}
		if len(upperLevelSstIdsSet) != 0 {
			panic("upper level sst ids set is not empty")
		}
		newSnapshot.levels[*task.UpperLevel-1].ssTables = newUpperLevelSsts
	} else {
		var newL0Ssts []uint32
		for _, id := range newSnapshot.l0SsTables {
			if _, ok := upperLevelSstIdsSet[id]; ok {
				delete(upperLevelSstIdsSet, id)
			} else {
				newL0Ssts = append(newL0Ssts, id)
			}
		}
		if len(upperLevelSstIdsSet) != 0 {
			panic("upper level sst ids set is not empty")
		}
		newSnapshot.l0SsTables = newL0Ssts
	}

	filesToRemove = append(filesToRemove, task.UpperLevelSstIds...)
	filesToRemove = append(filesToRemove, task.LowerLevelSstIds...)

	var newLowerLevelSsts []uint32
	for _, id := range newSnapshot.levels[task.LowerLevel-1].ssTables {
		if _, ok := lowerLevelSstIdsSet[id]; ok {
			delete(lowerLevelSstIdsSet, id)
		} else {
			newLowerLevelSsts = append(newLowerLevelSsts, id)
		}
	}
	if len(lowerLevelSstIdsSet) != 0 {
		panic("lower level sst ids set is not empty")
	}
	newLowerLevelSsts = append(newLowerLevelSsts, output...)
	sort.Slice(newLowerLevelSsts, func(i, j int) bool {
		return newSnapshot.sstables[newLowerLevelSsts[i]].FirstKey().Compare(newSnapshot.sstables[newLowerLevelSsts[j]].FirstKey()) < 0
	})
	newSnapshot.levels[task.LowerLevel-1].ssTables = newLowerLevelSsts
	return newSnapshot, filesToRemove
}
