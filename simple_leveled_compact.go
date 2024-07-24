package mini_lsm

import (
	"fmt"
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"slices"
)

type SimpleLeveledCompactionOptions struct {
	SizeRatioPercent               uint32
	Level0FileNumCompactionTrigger uint32
	MaxLevels                      uint32
}

type SimpleLeveledCompactionTask struct {
	UpperLevel              *uint32
	UpperLevelSstIds        []uint32
	LowerLevel              uint32
	LowerLevelSstIds        []uint32
	IsLowerLevelBottomLevel bool
}

type SimpleLeveledCompactionController struct {
	Options *SimpleLeveledCompactionOptions
}

func NewSimpleLeveledCompactionController(options *SimpleLeveledCompactionOptions) *SimpleLeveledCompactionController {
	return &SimpleLeveledCompactionController{
		Options: options,
	}
}

func (slcc *SimpleLeveledCompactionController) GenerateCompactionTask(snapshot *LsmStorageState) *SimpleLeveledCompactionTask {
	levelSizes := make([]uint32, 0, 1+len(snapshot.levels))
	levelSizes = append(levelSizes, uint32(len(snapshot.l0SsTables)))
	for _, level := range snapshot.levels {
		levelSizes = append(levelSizes, uint32(len(level.ssTables)))
	}
	for i := uint32(0); i < slcc.Options.MaxLevels; i++ {
		if i == 0 && len(snapshot.l0SsTables) < int(slcc.Options.Level0FileNumCompactionTrigger) {
			continue
		}

		lowerLevel := i + 1
		sizeRatio := float64(levelSizes[lowerLevel]) / float64(levelSizes[i])
		if sizeRatio < float64(slcc.Options.SizeRatioPercent)/100.0 {
			fmt.Printf("compaction triggered at level %d and %d with size ration %f\n", i, lowerLevel, sizeRatio)
			var upperLevelPtr *uint32
			var upperLevelSstIds []uint32
			lowerLevelSstIds := make([]uint32, 0, len(snapshot.levels[lowerLevel-1].ssTables))
			lowerLevelSstIds = append(lowerLevelSstIds, snapshot.levels[lowerLevel-1].ssTables...)
			upperLevel := i
			if i == 0 {
				upperLevelPtr = nil
				upperLevelSstIds = make([]uint32, 0, len(snapshot.l0SsTables))
				upperLevelSstIds = append(upperLevelSstIds, snapshot.l0SsTables...)
			} else {
				upperLevelPtr = &upperLevel
				upperLevelSstIds = make([]uint32, 0, len(snapshot.levels[i-1].ssTables))
				upperLevelSstIds = append(upperLevelSstIds, snapshot.levels[i-1].ssTables...)
			}
			return &SimpleLeveledCompactionTask{
				UpperLevel:              upperLevelPtr,
				UpperLevelSstIds:        upperLevelSstIds,
				LowerLevel:              lowerLevel,
				LowerLevelSstIds:        lowerLevelSstIds,
				IsLowerLevelBottomLevel: lowerLevel == slcc.Options.MaxLevels,
			}
		}
	}
	return nil
}

func (slcc *SimpleLeveledCompactionController) ApplyCompactionResult(task *SimpleLeveledCompactionTask, snapshot *LsmStorageState, output []uint32) (*LsmStorageState, []uint32) {
	snapshot = snapshot.snapshot()
	filesToRemove := make([]uint32, 0)
	if task.UpperLevel != nil {
		utils.Assert(slices.Equal(task.UpperLevelSstIds, snapshot.levels[*task.UpperLevel-1].ssTables), "sst mismatched")
		filesToRemove = append(filesToRemove, snapshot.levels[*task.UpperLevel-1].ssTables...)
		snapshot.levels[*task.UpperLevel-1].ssTables = make([]uint32, 0)
	} else {
		filesToRemove = append(filesToRemove, task.UpperLevelSstIds...)
		l0SstsCompacted := make(map[uint32]struct{})
		for _, sstId := range task.UpperLevelSstIds {
			l0SstsCompacted[sstId] = struct{}{}
		}
		newL0SsTables := make([]uint32, 0, len(snapshot.l0SsTables))
		for _, sstId := range snapshot.l0SsTables {
			if _, ok := l0SstsCompacted[sstId]; !ok {
				newL0SsTables = append(newL0SsTables, sstId)
			}
			delete(l0SstsCompacted, sstId)
		}
		utils.Assert(len(l0SstsCompacted) == 0, "sst mismatched")
		snapshot.l0SsTables = newL0SsTables
	}
	utils.Assert(slices.Equal(task.LowerLevelSstIds, snapshot.levels[task.LowerLevel-1].ssTables), "sst mismatched")
	filesToRemove = append(filesToRemove, snapshot.levels[task.LowerLevel-1].ssTables...)
	snapshot.levels[task.LowerLevel-1].ssTables = make([]uint32, 0, len(output))
	snapshot.levels[task.LowerLevel-1].ssTables = append(snapshot.levels[task.LowerLevel-1].ssTables, output...)
	return snapshot, filesToRemove
}
