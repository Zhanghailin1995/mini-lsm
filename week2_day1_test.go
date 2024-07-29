package mini_lsm

import (
	"fmt"
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"github.com/stretchr/testify/assert"
	"path"
	"testing"
)

func TestTask1FullCompaction(t *testing.T) {
	dir := t.TempDir()
	storage := utils.Unwrap(OpenLsmStorageInner(dir, DefaultForWeek1Test()))
	assert.NoError(t, storage.Put([]byte("0"), []byte("v1")))
	Sync(storage)
	assert.NoError(t, storage.Put([]byte("0"), []byte("v2")))
	assert.NoError(t, storage.Put([]byte("1"), []byte("v2")))
	assert.NoError(t, storage.Put([]byte("2"), []byte("v2")))
	Sync(storage)
	assert.NoError(t, storage.Delete([]byte("0")))
	assert.NoError(t, storage.Delete([]byte("2")))
	Sync(storage)

	assert.Equal(t, 3, ReadLsmStorageState(storage, func(state *LsmStorageState) int {
		return len(state.l0SsTables)
	}))

	iter := ReadLsmStorageState(storage, func(state *LsmStorageState) *MergeIterator {
		return ConstructMergeIteratorOverStorage(state)
	})

	if TsEnabled {
		CheckIterResultByKey1(t, iter, []StringKeyValuePair{
			{"0", ""},
			{"0", "v2"},
			{"0", "v1"},
			{"1", "v2"},
			{"2", ""},
			{"2", "v2"},
		})
	} else {
		CheckIterResultByKey1(t, iter, []StringKeyValuePair{
			{"0", ""},
			{"1", "v2"},
			{"2", ""},
		})
	}

	assert.NoError(t, storage.ForceFullCompaction())

	assert.Equal(t, 0, ReadLsmStorageState(storage, func(state *LsmStorageState) int {
		return len(state.l0SsTables)
	}))

	iter = ReadLsmStorageState(storage, func(state *LsmStorageState) *MergeIterator {
		return ConstructMergeIteratorOverStorage(state)
	})

	if TsEnabled {
		CheckIterResultByKey1(t, iter, []StringKeyValuePair{
			{"0", ""},
			{"0", "v2"},
			{"0", "v1"},
			{"1", "v2"},
			{"2", ""},
			{"2", "v2"},
		})
	} else {
		CheckIterResultByKey1(t, iter, []StringKeyValuePair{
			{"1", "v2"},
		})
	}

	assert.NoError(t, storage.Put([]byte("0"), []byte("v3")))
	assert.NoError(t, storage.Put([]byte("2"), []byte("v3")))
	Sync(storage)
	assert.NoError(t, storage.Delete([]byte("1")))
	Sync(storage)
	iter = ReadLsmStorageState(storage, func(state *LsmStorageState) *MergeIterator {
		return ConstructMergeIteratorOverStorage(state)
	})

	if TsEnabled {
		CheckIterResultByKey1(t, iter, []StringKeyValuePair{
			{"0", "v3"},
			{"0", ""},
			{"0", "v2"},
			{"0", "v1"},
			{"1", ""},
			{"1", "v2"},
			{"2", "v3"},
			{"2", ""},
			{"2", "v2"},
		})
	} else {
		CheckIterResultByKey1(t, iter, []StringKeyValuePair{
			{"0", "v3"},
			{"1", ""},
			{"2", "v3"},
		})
	}

	assert.NoError(t, storage.ForceFullCompaction())
	assert.Equal(t, 0, ReadLsmStorageState(storage, func(state *LsmStorageState) int {
		return len(state.l0SsTables)
	}))

	iter = ReadLsmStorageState(storage, func(state *LsmStorageState) *MergeIterator {
		return ConstructMergeIteratorOverStorage(state)
	})

	if TsEnabled {
		CheckIterResultByKey1(t, iter, []StringKeyValuePair{
			{"0", "v3"},
			{"0", ""},
			{"0", "v2"},
			{"0", "v1"},
			{"1", ""},
			{"1", "v2"},
			{"2", "v3"},
			{"2", ""},
			{"2", "v2"},
		})
	} else {
		CheckIterResultByKey1(t, iter, []StringKeyValuePair{
			{"0", "v3"},
			{"2", "v3"},
		})
	}

	assert.NoError(t, storage.Close())

}

func generateConcatSstForW2D1(startKey uint32, endKey uint32, dir string, id uint32) *SsTable {
	builder := NewSsTableBuilder(128)
	for i := startKey; i < endKey; i++ {
		key := fmt.Sprintf("%05d", i)
		builder.Add(KeyFromBytesWithTs([]byte(key), TsDefault), []byte("test"))
	}
	p := path.Join(dir, fmt.Sprintf("%d.sst", id))
	sst := utils.Unwrap(builder.buildForTest(p))
	return sst
}

func TestTask2ConcatIterator(t *testing.T) {
	dir := t.TempDir()
	sstables := make([]*SsTable, 0)
	// 00000 - 00009
	// 00010 - 00019
	// 00020 - 00029
	// ...
	// 00100 - 00109
	for i := 1; i <= 10; i++ {
		sst := generateConcatSstForW2D1(uint32(i*10), uint32((i+1)*10), dir, uint32(i))
		sstables = append(sstables, sst)
	}

	for key := 0; key < 120; key++ {
		iter, err := CreateSstConcatIteratorAndSeekToKey(sstables, KeyFromBytesWithTs([]byte(fmt.Sprintf("%05d", key)), TsDefault))
		if err != nil {
			println(key)
		}
		if key < 10 {
			assert.True(t, iter.IsValid())
			assert.Equal(t, "00010", string(iter.Key().KeyRef()))
		} else if key >= 110 {
			assert.False(t, iter.IsValid())
		} else {
			assert.True(t, iter.IsValid())
			assert.Equal(t, fmt.Sprintf("%05d", key), string(iter.Key().KeyRef()))
		}
	}
	iter := utils.Unwrap(CreateSstConcatIteratorAndSeekToFirst(sstables))
	assert.True(t, iter.IsValid())
	assert.Equal(t, "00010", string(iter.Key().KeyRef()))
	for _, sst := range sstables {
		assert.NoError(t, sst.CloseSstFile())
	}
}

func TestTask3Integration(t *testing.T) {
	dir := t.TempDir()
	storage := utils.Unwrap(OpenLsmStorageInner(dir, DefaultForWeek1Test()))
	assert.NoError(t, storage.Put([]byte("0"), []byte("2333333")))
	assert.NoError(t, storage.Put([]byte("00"), []byte("2333333")))
	assert.NoError(t, storage.Put([]byte("4"), []byte("23")))
	Sync(storage)
	assert.NoError(t, storage.Delete([]byte("4")))
	Sync(storage)

	assert.NoError(t, storage.ForceFullCompaction())

	assert.Equal(t, 0, ReadLsmStorageState(storage, func(state *LsmStorageState) int {
		return len(state.l0SsTables)
	}))
	assert.True(t, ReadLsmStorageState(storage, func(state *LsmStorageState) int {
		return len(state.levels[0].ssTables)
	}) > 0)

	assert.NoError(t, storage.Put([]byte("1"), []byte("233")))
	assert.NoError(t, storage.Put([]byte("2"), []byte("2333")))
	Sync(storage)

	assert.NoError(t, storage.Put([]byte("00"), []byte("2333")))
	assert.NoError(t, storage.Put([]byte("3"), []byte("23333")))
	assert.NoError(t, storage.Delete([]byte("1")))
	Sync(storage)
	assert.NoError(t, storage.ForceFullCompaction())

	assert.Equal(t, 0, ReadLsmStorageState(storage, func(state *LsmStorageState) int {
		return len(state.l0SsTables)
	}))
	assert.True(t, ReadLsmStorageState(storage, func(state *LsmStorageState) int {
		return len(state.levels[0].ssTables)
	}) > 0)

	iter := utils.Unwrap(storage.Scan(UnboundBytes(), UnboundBytes()))
	CheckLsmIterResultByKey1(t, iter, []StringKeyValuePair{
		{"0", "2333333"},
		{"00", "2333"},
		{"2", "2333"},
		{"3", "23333"},
	})

	assert.Equal(t, "2333333", string(utils.Unwrap(storage.Get([]byte("0")))))
	assert.Equal(t, "2333", string(utils.Unwrap(storage.Get([]byte("00")))))
	assert.Equal(t, "2333", string(utils.Unwrap(storage.Get([]byte("2")))))
	assert.Equal(t, "23333", string(utils.Unwrap(storage.Get([]byte("3")))))

	assert.NoError(t, storage.Close())
}
