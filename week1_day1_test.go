package mini_lsm

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTask1MemTableGet(t *testing.T) {
	memTable := CreateMemTable(0)
	err := memTable.ForTestingPutSlice(Key([]byte("key1")), []byte("value1"))
	if err != nil {
		t.Errorf("Put error: %v", err)
	}
	err = memTable.ForTestingPutSlice(Key([]byte("key2")), []byte("value2"))
	if err != nil {
		t.Errorf("Put error: %v", err)
	}
	err = memTable.ForTestingPutSlice(Key([]byte("key3")), []byte("value3"))
	if err != nil {
		t.Errorf("Put error: %v", err)
	}
	assert.Equalf(t, memTable.ForTestingGetSlice(Key([]byte("key1"))), []byte("value1"), "Expected value1, got %s", memTable.ForTestingGetSlice(Key([]byte("key1"))))
	assert.Equalf(t, memTable.ForTestingGetSlice(Key([]byte("key2"))), []byte("value2"), "Expected value2, got %s", memTable.ForTestingGetSlice(Key([]byte("key2"))))
	assert.Equalf(t, memTable.ForTestingGetSlice(Key([]byte("key3"))), []byte("value3"), "Expected value3, got %s", memTable.ForTestingGetSlice(Key([]byte("key3"))))
}

func TestTask1MemTableOverwrite(t *testing.T) {
	memTable := CreateMemTable(0)
	err := memTable.ForTestingPutSlice(Key([]byte("key1")), []byte("value1"))
	if err != nil {
		t.Errorf("Put error: %v", err)
	}
	err = memTable.ForTestingPutSlice(Key([]byte("key2")), []byte("value2"))
	if err != nil {
		t.Errorf("Put error: %v", err)
	}
	err = memTable.ForTestingPutSlice(Key([]byte("key3")), []byte("value3"))
	if err != nil {
		t.Errorf("Put error: %v", err)
	}
	err = memTable.ForTestingPutSlice(Key([]byte("key1")), []byte("value11"))
	if err != nil {
		t.Errorf("Put error: %v", err)
	}
	err = memTable.ForTestingPutSlice(Key([]byte("key2")), []byte("value22"))
	if err != nil {
		t.Errorf("Put error: %v", err)
	}
	err = memTable.ForTestingPutSlice(Key([]byte("key3")), []byte("value33"))
	if err != nil {
		t.Errorf("Put error: %v", err)
	}
	assert.Equal(t, memTable.ForTestingGetSlice(Key([]byte("key1"))), []byte("value11"))
	assert.Equal(t, memTable.ForTestingGetSlice(Key([]byte("key2"))), []byte("value22"))
	assert.Equal(t, memTable.ForTestingGetSlice(Key([]byte("key3"))), []byte("value33"))
}

func TestTask2StorageIntegration(t *testing.T) {
	dir := t.TempDir()
	storage, err := OpenLsmStorageInner(dir, DefaultForWeek1Test())
	if err != nil {
		t.Errorf("OpenLsmStorageInner error: %v", err)
	}
	v, err := storage.Get(Key([]byte("0")))
	assert.True(t, err == nil && v == nil)
	err = storage.Put(Key([]byte("1")), []byte("233"))
	assert.True(t, err == nil)
	err = storage.Put(Key([]byte("2")), []byte("2333"))
	assert.True(t, err == nil)
	err = storage.Put(Key([]byte("3")), []byte("23333"))
	assert.True(t, err == nil)
	v, err = storage.Get(Key([]byte("1")))
	assert.True(t, err == nil)
	assert.Equal(t, v, []byte("233"))
	v, err = storage.Get(Key([]byte("2")))
	assert.True(t, err == nil)
	assert.Equal(t, v, []byte("2333"))
	v, err = storage.Get(Key([]byte("3")))
	assert.True(t, err == nil)
	assert.Equal(t, v, []byte("23333"))
	err = storage.Delete(Key([]byte("2")))
	assert.True(t, err == nil)
	v, err = storage.Get(Key([]byte("2")))
	assert.True(t, err == nil && v == nil)
}

func TestTask3StorageIntegration(t *testing.T) {
	dir := t.TempDir()
	storage, err := OpenLsmStorageInner(dir, DefaultForWeek1Test())
	if err != nil {
		t.Errorf("OpenLsmStorageInner error: %v", err)
	}

	err = storage.Put(Key([]byte("1")), []byte("233"))
	if err != nil {
		t.Errorf("Put error: %v", err)
	}

	err = storage.Put(Key([]byte("2")), []byte("2333"))
	if err != nil {
		t.Errorf("Put error: %v", err)
	}

	err = storage.Put(Key([]byte("3")), []byte("23333"))
	if err != nil {
		t.Errorf("Put error: %v", err)
	}
	storage.stateLock.Lock()
	err = storage.ForceFreezeMemTable()
	storage.stateLock.Unlock()
	if err != nil {
		t.Errorf("ForceFreezeMemTable error: %v", err)
	}
	storage.rwLock.RLock()
	if len(storage.state.immMemTable) != 1 {
		t.Errorf("Expected 1, got %d", len(storage.state.immMemTable))
	}
	storage.rwLock.RUnlock()

	storage.rwLock.RLock()
	previousApproximateSize := storage.state.immMemTable[0].ApproximateSize()
	storage.rwLock.RUnlock()
	if previousApproximateSize < 15 {
		t.Errorf("Expected >= 15, got %d", previousApproximateSize)
	}

	err = storage.Put(Key([]byte("1")), []byte("2333"))
	if err != nil {
		t.Errorf("Put error: %v", err)
	}

	err = storage.Put(Key([]byte("2")), []byte("23333"))
	if err != nil {
		t.Errorf("Put error: %v", err)
	}

	err = storage.Put(Key([]byte("3")), []byte("233333"))
	if err != nil {
		t.Errorf("Put error: %v", err)
	}

	storage.stateLock.Lock()
	err = storage.ForceFreezeMemTable()
	storage.stateLock.Unlock()
	if err != nil {
		t.Errorf("ForceFreezeMemTable error: %v", err)
	}
	storage.rwLock.RLock()
	if len(storage.state.immMemTable) != 2 {
		t.Errorf("Expected 2, got %d", len(storage.state.immMemTable))
	}
	storage.rwLock.RUnlock()

	storage.rwLock.RLock()
	if storage.state.immMemTable[1].ApproximateSize() != previousApproximateSize {
		t.Errorf("Wrong order of memtables?")
	}
	storage.rwLock.RUnlock()
	storage.rwLock.RLock()
	if storage.state.immMemTable[0].ApproximateSize() <= previousApproximateSize {
		t.Errorf("Expected > %d, got %d", previousApproximateSize, storage.state.immMemTable[0].ApproximateSize())
	}
	storage.rwLock.RUnlock()
}

func TestTask3FreezeOnCapacity(t *testing.T) {
	dir := t.TempDir()
	options := DefaultForWeek1Test()
	options.targetSstSize = 1024
	options.numberMemTableLimit = 1000
	storage, err := OpenLsmStorageInner(dir, options)
	if err != nil {
		t.Errorf("OpenLsmStorageInner error: %v", err)
	}
	for i := 0; i < 1000; i++ {
		err = storage.Put(Key([]byte("1")), []byte("2333"))
		if err != nil {
			t.Errorf("Put error: %v", err)
		}
	}
	storage.rwLock.RLock()
	numImmMemTables := len(storage.state.immMemTable)
	storage.rwLock.RUnlock()
	if numImmMemTables < 1 {
		t.Errorf("Expected >= 1, got %d", numImmMemTables)
	}
	for i := 0; i < 1000; i++ {
		err := storage.Delete(Key([]byte("1")))
		if err != nil {
			t.Errorf("Delete error: %v", err)
		}
	}
	storage.rwLock.RLock()
	if len(storage.state.immMemTable) <= numImmMemTables {
		t.Errorf("no more memtable frozen? %d, %d", len(storage.state.immMemTable), numImmMemTables)
	}
}

func TestTask4StorageIntegration(t *testing.T) {
	dir := t.TempDir()
	storage, err := OpenLsmStorageInner(dir, DefaultForWeek1Test())
	if err != nil {
		t.Errorf("OpenLsmStorageInner error: %v", err)
	}
	v, err := storage.Get(Key([]byte("0")))
	assert.True(t, err == nil && v == nil)
	err = storage.Put(Key([]byte("1")), []byte("233"))
	assert.True(t, err == nil)
	err = storage.Put(Key([]byte("2")), []byte("2333"))
	assert.True(t, err == nil)
	err = storage.Put(Key([]byte("3")), []byte("23333"))
	assert.True(t, err == nil)
	storage.stateLock.Lock()
	err = storage.ForceFreezeMemTable()
	storage.stateLock.Unlock()
	if err != nil {
		t.Errorf("ForceFreezeMemTable error: %v", err)
	}
	err = storage.Delete(Key([]byte("1")))
	assert.True(t, err == nil)
	err = storage.Delete(Key([]byte("2")))
	assert.True(t, err == nil)
	err = storage.Put(Key([]byte("3")), []byte("2333"))
	assert.True(t, err == nil)
	assert.NoError(t, storage.Put(Key([]byte("4")), []byte("23333")))
	storage.stateLock.Lock()
	assert.NoError(t, storage.ForceFreezeMemTable())
	storage.stateLock.Unlock()
	assert.NoError(t, storage.Put(Key([]byte("1")), []byte("233333")))
	assert.NoError(t, storage.Put(Key([]byte("3")), []byte("2333333")))
	storage.rwLock.RLock()
	assert.Equal(t, len(storage.state.immMemTable), 2)
	storage.rwLock.RUnlock()
	v, err = storage.Get(Key([]byte("1")))
	assert.True(t, err == nil)
	assert.Equal(t, v, []byte("233333"))
	v, err = storage.Get(Key([]byte("2")))
	assert.True(t, err == nil && v == nil)
	v, err = storage.Get(Key([]byte("3")))
	assert.True(t, err == nil)
	assert.Equal(t, v, []byte("2333333"))
	v, err = storage.Get(Key([]byte("4")))
	assert.True(t, err == nil)
	assert.Equal(t, v, []byte("23333"))
}
