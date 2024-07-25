package mini_lsm

import (
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIntegration2(t *testing.T) {
	dir := t.TempDir()
	lsm := utils.Unwrap(Open(dir, DefaultForWeek2Test(&CompactionOptions{
		CompactionType: Leveled,
		Opt: &LeveledCompactionOptions{
			Level0FileNumCompactionTrigger: 2,
			LevelSizeMultiplier:            2,
			BaseLevelSizeMb:                1,
			MaxLevels:                      4,
		},
	})))
	CompactionBench(lsm, t)
	assert.NoError(t, lsm.Shutdown())
}
