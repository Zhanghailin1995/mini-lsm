package mini_lsm

import (
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIntegration1(t *testing.T) {
	dir := t.TempDir()
	lsm := utils.Unwrap(Open(dir, DefaultForWeek2Test(&CompactionOptions{
		CompactionType: Tiered,
		Opt: &TieredCompactionOptions{
			NumTiers:                    3,
			MaxSizeAmplificationPercent: 200,
			SizeRatio:                   1,
			MinMergeWidth:               2,
		},
	})))
	CompactionBench(lsm, t)
	assert.NoError(t, lsm.Shutdown())
}
