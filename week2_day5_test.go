package mini_lsm

import (
	"fmt"
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"github.com/stretchr/testify/assert"
	"path"
	"testing"
)

func TestRecoverManifest(t *testing.T) {
	p := t.TempDir()
	p = path.Join(p, "MANIFEST")
	m := utils.Unwrap(NewManifest(p))
	r1 := &FlushRecord{
		Flush: 2,
	}
	assert.NoError(t, m.AddRecord(r1))
	r2 := &NewMemtableRecord{
		NewMemtable: 3,
	}
	assert.NoError(t, m.AddRecord(r2))

	assert.NoError(t, m.Close())

	manifest, records, err := RecoverManifest(p)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(records))
	assert.Equal(t, r1, records[0])
	assert.Equal(t, r2, records[1])
	r3 := &CompactionRecord{
		CompactionTask: &CompactionTask{
			CompactionType: ForceFull,
			Task: &ForceFullCompactionTask{
				L0Sstables: []uint32{1, 2, 3},
				L1Sstables: []uint32{1, 2, 3},
			},
		},
		SstIds: []uint32{1, 2, 3},
	}
	assert.NoError(t, manifest.AddRecord(r3))
	assert.NoError(t, manifest.Close())
	manifest, records, err = RecoverManifest(p)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(records))
	assert.Equal(t, r1, records[0])
	assert.Equal(t, r2, records[1])
	assert.Equal(t, r3, records[2])
	assert.NoError(t, manifest.Close())
}

func TestIntegrationLeveled(t *testing.T) {
	Integration(&CompactionOptions{
		CompactionType: Leveled,
		Opt: &LeveledCompactionOptions{
			Level0FileNumCompactionTrigger: 2,
			LevelSizeMultiplier:            2,
			BaseLevelSizeMb:                1,
			MaxLevels:                      3,
		},
	}, t)
}

func TestIntegrationTiered(t *testing.T) {
	Integration(&CompactionOptions{
		CompactionType: Tiered,
		Opt: &TieredCompactionOptions{
			NumTiers:                    3,
			MaxSizeAmplificationPercent: 200,
			SizeRatio:                   1,
			MinMergeWidth:               3,
		},
	}, t)

}

func TestIntegrationSimple(t *testing.T) {
	Integration(&CompactionOptions{
		CompactionType: Simple,
		Opt: &SimpleLeveledCompactionOptions{
			Level0FileNumCompactionTrigger: 2,
			MaxLevels:                      3,
			SizeRatioPercent:               200,
		},
	}, t)
}

func Integration(compactionOptions *CompactionOptions, t *testing.T) {
	dir := t.TempDir()
	storage := utils.Unwrap(Open(dir, DefaultForWeek2Test(compactionOptions)))
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
	assert.True(t,
		ReadLsmStorageState(storage.inner, func(state *LsmStorageState) int {
			return len(state.immMemTable)
		}) == 0,
	)
	assert.True(t,
		ReadLsmStorageState(storage.inner, func(state *LsmStorageState) bool {
			return state.memTable.IsEmpty()
		}),
	)
	storage.DumpStructure()
	dumpFilesInDir(dir)

	storage = utils.Unwrap(Open(dir, DefaultForWeek2Test(compactionOptions)))
	assert.Equal(t, "v20", string(utils.Unwrap(storage.Get(b("0")))))
	assert.Equal(t, "v20", string(utils.Unwrap(storage.Get(b("1")))))
	assert.Equal(t, "", string(utils.Unwrap(storage.Get(b("2")))))
	assert.NoError(t, storage.Shutdown())
}
