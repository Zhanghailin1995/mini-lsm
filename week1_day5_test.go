package mini_lsm

import (
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"github.com/stretchr/testify/assert"
	"path"
	"testing"
)

func TestTask1Merge1(t *testing.T) {
	i1 := NewMockIteratorWithStringKVPair([]StringKeyValuePair{
		{"a", "1.1"},
		{"b", "2.1"},
		{"c", "3.1"},
	})

	i2 := NewMockIteratorWithStringKVPair([]StringKeyValuePair{
		{"a", "1.2"},
		{"b", "2.2"},
		{"c", "3.2"},
		{"d", "4.2"},
	})
	iter := utils.Unwrap(CreateTwoMergeIterator(i1, i2))
	CheckIterResultByKey1(t, iter, []StringKeyValuePair{
		{"a", "1.1"},
		{"b", "2.1"},
		{"c", "3.1"},
		{"d", "4.2"},
	})
}

func TestTask1Merge2(t *testing.T) {
	i2 := NewMockIteratorWithStringKVPair([]StringKeyValuePair{
		{"a", "1.1"},
		{"b", "2.1"},
		{"c", "3.1"},
	})

	i1 := NewMockIteratorWithStringKVPair([]StringKeyValuePair{
		{"a", "1.2"},
		{"b", "2.2"},
		{"c", "3.2"},
		{"d", "4.2"},
	})
	iter := utils.Unwrap(CreateTwoMergeIterator(i1, i2))
	CheckIterResultByKey1(t, iter, []StringKeyValuePair{
		{"a", "1.2"},
		{"b", "2.2"},
		{"c", "3.2"},
		{"d", "4.2"},
	})
}

func TestTask1Merge3(t *testing.T) {
	i2 := NewMockIteratorWithStringKVPair([]StringKeyValuePair{
		{"a", "1.1"},
		{"b", "2.1"},
		{"c", "3.1"},
	})

	i1 := NewMockIteratorWithStringKVPair([]StringKeyValuePair{
		{"b", "2.2"},
		{"c", "3.2"},
		{"d", "4.2"},
	})
	iter := utils.Unwrap(CreateTwoMergeIterator(i1, i2))
	CheckIterResultByKey1(t, iter, []StringKeyValuePair{
		{"a", "1.1"},
		{"b", "2.2"},
		{"c", "3.2"},
		{"d", "4.2"},
	})
}

func TestTask1Merge4(t *testing.T) {
	i2 := NewMockIteratorWithStringKVPair([]StringKeyValuePair{})

	i1 := NewMockIteratorWithStringKVPair([]StringKeyValuePair{
		{"b", "2.2"},
		{"c", "3.2"},
		{"d", "4.2"},
	})

	iter := utils.Unwrap(CreateTwoMergeIterator(i1, i2))
	CheckIterResultByKey1(t, iter, []StringKeyValuePair{
		{"b", "2.2"},
		{"c", "3.2"},
		{"d", "4.2"},
	})

	i1 = NewMockIteratorWithStringKVPair([]StringKeyValuePair{})
	i2 = NewMockIteratorWithStringKVPair([]StringKeyValuePair{
		{"b", "2.2"},
		{"c", "3.2"},
		{"d", "4.2"},
	})
	iter = utils.Unwrap(CreateTwoMergeIterator(i1, i2))
	CheckIterResultByKey1(t, iter, []StringKeyValuePair{
		{"b", "2.2"},
		{"c", "3.2"},
		{"d", "4.2"},
	})
}

func TestTask1Merge5(t *testing.T) {
	i1 := NewMockIteratorWithStringKVPair([]StringKeyValuePair{})
	i2 := NewMockIteratorWithStringKVPair([]StringKeyValuePair{})
	iter := utils.Unwrap(CreateTwoMergeIterator(i1, i2))
	CheckIterResultByKey1(t, iter, []StringKeyValuePair{})
}

func TestTask2StorageScan(t *testing.T) {
	dir := t.TempDir()
	storage := utils.Unwrap(OpenLsmStorageInner(dir, DefaultForWeek1Test()))
	assert.NoError(t, storage.Put(StringKey("1"), []byte("233")))
	assert.NoError(t, storage.Put(StringKey("2"), []byte("2333")))
	assert.NoError(t, storage.Put(StringKey("00"), []byte("2333")))
	storage.stateLock.Lock()
	assert.NoError(t, storage.ForceFreezeMemTable())
	storage.stateLock.Unlock()
	assert.NoError(t, storage.Put(StringKey("3"), []byte("23333")))
	assert.NoError(t, storage.Delete(StringKey("1")))

	sst1 := GenerateSst(10,
		path.Join(dir, "10.sst"),
		[]StringKeyValuePair{
			{"0", "2333333"},
			{"00", "2333333"},
			{"4", "23"},
		},
		storage.blockCache,
	)
	sst2 := GenerateSst(11,
		path.Join(dir, "11.sst"),
		[]StringKeyValuePair{
			{"4", ""},
		},
		storage.blockCache,
	)
	{
		storage.rwLock.Lock()
		snapshot := storage.state.snapshot()
		snapshot.l0SsTables = append(snapshot.l0SsTables, sst2.Id())
		snapshot.l0SsTables = append(snapshot.l0SsTables, sst1.Id())
		snapshot.sstables[sst2.Id()] = sst2
		snapshot.sstables[sst1.Id()] = sst1
		storage.state = snapshot
		storage.rwLock.Unlock()
	}

	CheckLsmIterResultByKey1(t, utils.Unwrap(storage.Scan(Unbound(), Unbound())), []StringKeyValuePair{
		{"0", "2333333"},
		{"00", "2333"},
		{"2", "2333"},
		{"3", "23333"},
	})

	CheckLsmIterResultByKey1(t, utils.Unwrap(storage.Scan(Include(StringKey("1")), Include(StringKey("2")))), []StringKeyValuePair{
		{"2", "2333"},
	})
	CheckLsmIterResultByKey1(t, utils.Unwrap(storage.Scan(Exclude(StringKey("1")), Exclude(StringKey("3")))), []StringKeyValuePair{
		{"2", "2333"},
	})
	CheckLsmIterResultByKey1(t, utils.Unwrap(storage.Scan(Include(StringKey("0")), Include(StringKey("1")))), []StringKeyValuePair{
		{"0", "2333333"},
		{"00", "2333"},
	})
	CheckLsmIterResultByKey1(t, utils.Unwrap(storage.Scan(Exclude(StringKey("0")), Include(StringKey("1")))), []StringKeyValuePair{
		{"00", "2333"},
	})

	assert.NoError(t, storage.Close())
}

func TestTask2StorageGet(t *testing.T) {
	dir := t.TempDir()
	storage := utils.Unwrap(OpenLsmStorageInner(dir, DefaultForWeek1Test()))
	assert.NoError(t, storage.Put(StringKey("1"), []byte("233")))
	assert.NoError(t, storage.Put(StringKey("2"), []byte("2333")))
	assert.NoError(t, storage.Put(StringKey("00"), []byte("2333")))
	storage.stateLock.Lock()
	assert.NoError(t, storage.ForceFreezeMemTable())
	storage.stateLock.Unlock()
	assert.NoError(t, storage.Put(StringKey("3"), []byte("23333")))
	assert.NoError(t, storage.Delete(StringKey("1")))

	sst1 := GenerateSst(10,
		path.Join(dir, "10.sst"),
		[]StringKeyValuePair{
			{"0", "2333333"},
			{"00", "2333333"},
			{"4", "23"},
		},
		storage.blockCache,
	)
	sst2 := GenerateSst(11,
		path.Join(dir, "11.sst"),
		[]StringKeyValuePair{
			{"4", ""},
		},
		storage.blockCache,
	)
	{
		storage.rwLock.Lock()
		snapshot := storage.state.snapshot()
		snapshot.l0SsTables = append(snapshot.l0SsTables, sst2.Id())
		snapshot.l0SsTables = append(snapshot.l0SsTables, sst1.Id())
		snapshot.sstables[sst2.Id()] = sst2
		snapshot.sstables[sst1.Id()] = sst1
		storage.state = snapshot
		storage.rwLock.Unlock()
	}

	assert.Equal(t, utils.Unwrap(storage.Get(StringKey("0"))), []byte("2333333"))
	assert.Equal(t, utils.Unwrap(storage.Get(StringKey("00"))), []byte("2333"))
	assert.Equal(t, utils.Unwrap(storage.Get(StringKey("2"))), []byte("2333"))
	assert.Equal(t, utils.Unwrap(storage.Get(StringKey("3"))), []byte("23333"))

	assert.Equal(t, utils.Unwrap(storage.Get(StringKey("4"))), []byte(nil))
	assert.Equal(t, utils.Unwrap(storage.Get(StringKey("--"))), []byte(nil))
	assert.Equal(t, utils.Unwrap(storage.Get(StringKey("555"))), []byte(nil))

	assert.NoError(t, storage.Close())
}
