package mini_lsm

import (
	"fmt"
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIntegrationLeveledW2D6(t *testing.T) {
	IntegrationW2D6(&CompactionOptions{
		CompactionType: Leveled,
		Opt: &LeveledCompactionOptions{
			Level0FileNumCompactionTrigger: 2,
			LevelSizeMultiplier:            2,
			BaseLevelSizeMb:                1,
			MaxLevels:                      3,
		},
	}, t)
}

func TestIntegrationTieredW2D6(t *testing.T) {
	IntegrationW2D6(&CompactionOptions{
		CompactionType: Tiered,
		Opt: &TieredCompactionOptions{
			NumTiers:                    3,
			MaxSizeAmplificationPercent: 200,
			SizeRatio:                   1,
			MinMergeWidth:               3,
		},
	}, t)

}

func TestIntegrationSimpleW2D6(t *testing.T) {
	IntegrationW2D6(&CompactionOptions{
		CompactionType: Simple,
		Opt: &SimpleLeveledCompactionOptions{
			Level0FileNumCompactionTrigger: 2,
			MaxLevels:                      3,
			SizeRatioPercent:               200,
		},
	}, t)
}

func IntegrationW2D6(compactionOptions *CompactionOptions, t *testing.T) {
	dir := t.TempDir()
	options := DefaultForWeek2Test(compactionOptions)
	options.EnableWal = true
	storage := utils.Unwrap(Open(dir, options))
	for i := 0; i <= 20; i++ {
		assert.NoError(t, storage.Put(b("0"), []byte(fmt.Sprintf("v%d", i))))
		if i%2 == 0 {
			assert.NoError(t, storage.Put(b("1"), []byte(fmt.Sprintf("v%d", i))))
		} else {
			assert.NoError(t, storage.Delete(b("1")))
		}
		if i%2 == 1 {
			assert.NoError(t, storage.Put(b("2"), []byte(fmt.Sprintf("v%d", i))))
		} else {
			assert.NoError(t, storage.Delete(b("2")))
		}
		storage.inner.stateLock.Lock()
		assert.NoError(t, storage.inner.ForceFreezeMemTable())
		storage.inner.stateLock.Unlock()
	}
	assert.NoError(t, storage.Shutdown())
	x1 := ReadLsmStorageState(storage.inner, func(state *LsmStorageState) bool {
		return state.memTable.IsEmpty()
	})
	x2 := ReadLsmStorageState(storage.inner, func(state *LsmStorageState) int {
		return len(state.immMemTable)
	})

	assert.True(t,
		!x1 || !(x2 == 0),
	)
	storage.DumpStructure()
	dumpFilesInDir(dir)

	storage = utils.Unwrap(Open(dir, options))
	assert.Equal(t, "v20", string(utils.Unwrap(storage.Get(b("0")))))
	assert.Equal(t, "v20", string(utils.Unwrap(storage.Get(b("1")))))
	assert.Equal(t, "", string(utils.Unwrap(storage.Get(b("2")))))
	assert.NoError(t, storage.Shutdown())
}
