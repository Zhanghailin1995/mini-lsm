package mini_lsm

import (
	"fmt"
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"slices"
)

type Tier struct {
	Tier   uint32
	SstIds []uint32
}

type TieredCompactionTask struct {
	Tiers              []*Tier
	BottomTierIncluded bool
}

type TieredCompactionOptions struct {
	NumTiers                    uint32
	MaxSizeAmplificationPercent uint32
	SizeRatio                   uint32
	MinMergeWidth               uint32
}

type TieredCompactionController struct {
	Options *TieredCompactionOptions
}

func NewTieredCompactionController(options *TieredCompactionOptions) *TieredCompactionController {
	return &TieredCompactionController{
		Options: options,
	}
}

func (tcc *TieredCompactionController) GenerateCompactionTask(snapshot *LsmStorageState) *TieredCompactionTask {
	utils.Assert(len(snapshot.l0SsTables) == 0, "l0SsTables should be empty")
	if uint32(len(snapshot.levels)) < tcc.Options.NumTiers {
		return nil
	}

	size := 0
	for id := 0; id < len(snapshot.levels)-1; id++ {
		size += len(snapshot.levels[id].ssTables)
	}
	spaceAmpRatio := float64(size) / float64(len(snapshot.levels[len(snapshot.levels)-1].ssTables)) * 100.0
	if spaceAmpRatio >= float64(tcc.Options.MaxSizeAmplificationPercent) {
		fmt.Printf("compaction triggered by space amplification ratio: %f\r\n", spaceAmpRatio)
		tiers := make([]*Tier, 0, len(snapshot.levels))
		for _, level := range snapshot.levels {
			tier := &Tier{
				Tier:   level.level,
				SstIds: slices.Clone(level.ssTables),
			}
			tiers = append(tiers, tier)
		}
		return &TieredCompactionTask{
			Tiers:              tiers,
			BottomTierIncluded: true,
		}
	}
	size = 0
	sizeRatioTrigger := (100.0 + float64(tcc.Options.SizeRatio)) / 100.0
	for id := 0; id < len(snapshot.levels)-1; id++ {
		size += len(snapshot.levels[id].ssTables)
		nextLevelSize := len(snapshot.levels[id+1].ssTables)
		currentSizeRatio := float64(size) / float64(nextLevelSize)
		if currentSizeRatio >= sizeRatioTrigger && uint32(id)+2 >= tcc.Options.MinMergeWidth {
			fmt.Printf("compaction triggered by size ratio: %f\r\n", currentSizeRatio)
			tiers := make([]*Tier, 0, id+2)
			x := id + 2
			if id+2 >= len(snapshot.levels) {
				x = len(snapshot.levels)
			}
			for i := 0; i < x; i++ {
				level := snapshot.levels[i]
				tier := &Tier{
					Tier:   level.level,
					SstIds: slices.Clone(level.ssTables),
				}
				tiers = append(tiers, tier)
			}
			return &TieredCompactionTask{
				Tiers:              tiers,
				BottomTierIncluded: id+2 >= len(snapshot.levels),
			}
		}
	}

	numTiersToTake := len(snapshot.levels) - int(tcc.Options.NumTiers) + 2
	println("compaction triggered by reducing sorted runs")
	tiers := make([]*Tier, 0, numTiersToTake)
	x := numTiersToTake
	if numTiersToTake >= len(snapshot.levels) {
		x = len(snapshot.levels)
	}
	for i := 0; i < x; i++ {
		level := snapshot.levels[i]
		tier := &Tier{
			Tier:   level.level,
			SstIds: slices.Clone(level.ssTables),
		}
		tiers = append(tiers, tier)
	}
	return &TieredCompactionTask{
		Tiers:              tiers,
		BottomTierIncluded: numTiersToTake <= len(snapshot.levels),
	}
}

func (tcc *TieredCompactionController) ApplyCompactionResult(snapshot *LsmStorageState, task *TieredCompactionTask, output []uint32) (*LsmStorageState, []uint32) {
	if len(snapshot.l0SsTables) != 0 {
		panic("should not add l0 ssts in tiered compaction")
	}

	newSnapshot := snapshot.snapshot() // Assuming snapshot() is a deep copy method
	tierToRemove := make(map[uint32][]uint32)
	for _, tier := range task.Tiers {
		tierToRemove[tier.Tier] = tier.SstIds
	}

	var levels []*LevelSsTables
	newTierAdded := false
	var filesToRemove []uint32

	for _, level := range newSnapshot.levels {
		if files, ok := tierToRemove[level.level]; ok {
			delete(tierToRemove, level.level)
			utils.Assert(slices.Equal(files, level.ssTables), "file changed after issuing compaction task")
			filesToRemove = append(filesToRemove, files...)
		} else {
			levels = append(levels, level.Clone())
		}
		if len(tierToRemove) == 0 && !newTierAdded {
			newTierAdded = true
			levels = append(levels, &LevelSsTables{
				level:    output[0],
				ssTables: slices.Clone(output),
			})
		}
	}

	if len(tierToRemove) != 0 {
		panic("some tiers not found??")
	}

	newSnapshot.levels = levels
	return newSnapshot, filesToRemove
}
