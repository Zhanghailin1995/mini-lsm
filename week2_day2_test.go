package mini_lsm

import (
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIntegration(t *testing.T) {
	dir := t.TempDir()
	lsm := utils.Unwrap(Open(dir, DefaultForWeek2Test(&CompactionOptions{
		CompactionType: Simple,
		Opt: &SimpleLeveledCompactionOptions{
			Level0FileNumCompactionTrigger: 2,
			MaxLevels:                      3,
			SizeRatioPercent:               200,
		},
	})))
	CompactionBench(lsm, t)
	assert.NoError(t, lsm.Shutdown())
}
