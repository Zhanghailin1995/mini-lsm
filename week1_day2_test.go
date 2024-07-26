package mini_lsm

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTask1MemTableIter(t *testing.T) {
	memTable := CreateMemTable(0)
	assert.NoError(t, memTable.ForTestingPutSlice(Key([]byte("key1")), []byte("value1")))
	assert.NoError(t, memTable.ForTestingPutSlice(Key([]byte("key2")), []byte("value2")))
	assert.NoError(t, memTable.ForTestingPutSlice(Key([]byte("key3")), []byte("value3")))

	{
		iter := memTable.Scan(Unbound(), Unbound())
		assert.Equal(t, iter.IsValid(), true)
		assert.Equal(t, iter.Key().Val, []byte("key1"))
		assert.Equal(t, iter.Value(), []byte("value1"))
		assert.NoError(t, iter.Next())
		assert.Equal(t, iter.IsValid(), true)
		assert.Equal(t, iter.Key().Val, []byte("key2"))
		assert.Equal(t, iter.Value(), []byte("value2"))
		assert.NoError(t, iter.Next())
		assert.Equal(t, iter.IsValid(), true)
		assert.Equal(t, iter.Key().Val, []byte("key3"))
		assert.Equal(t, iter.Value(), []byte("value3"))
		assert.NoError(t, iter.Next())
		assert.Equal(t, iter.IsValid(), false)
	}

	{
		lower := Include(Key([]byte("key1")))
		upper := Include(Key([]byte("key2")))
		iter := memTable.Scan(lower, upper)
		assert.Equal(t, iter.IsValid(), true)
		assert.Equal(t, iter.Key().Val, []byte("key1"))
		assert.Equal(t, iter.Value(), []byte("value1"))
		assert.NoError(t, iter.Next())
		assert.Equal(t, iter.IsValid(), true)
		assert.Equal(t, iter.Key().Val, []byte("key2"))
		assert.Equal(t, iter.Value(), []byte("value2"))
		assert.NoError(t, iter.Next())
		assert.Equal(t, iter.IsValid(), false)
	}

	{
		lower := Exclude(Key([]byte("key1")))
		upper := Exclude(Key([]byte("key3")))
		iter := memTable.Scan(lower, upper)
		assert.Equal(t, iter.IsValid(), true)
		assert.Equal(t, iter.Key().Val, []byte("key2"))
		assert.Equal(t, iter.Value(), []byte("value2"))
		assert.NoError(t, iter.Next())
		assert.Equal(t, iter.IsValid(), false)
	}
}

func TestTask1EmptyMemTableIter(t *testing.T) {
	memTable := CreateMemTable(0)
	iter := memTable.Scan(Unbound(), Unbound())
	assert.Equal(t, iter.IsValid(), false)

	lower := Exclude(Key([]byte("key1")))
	upper := Exclude(Key([]byte("key3")))
	iter = memTable.Scan(lower, upper)
	assert.Equal(t, iter.IsValid(), false)

	lower = Include(Key([]byte("key1")))
	upper = Include(Key([]byte("key2")))
	assert.Equal(t, iter.IsValid(), false)
}

func TestTask2Merge1(t *testing.T) {
	i1 := NewMockIterator([]struct {
		K KeyType
		V []byte
	}{
		{K: Key([]byte("a")), V: []byte("1.1")},
		{K: Key([]byte("b")), V: []byte("2.1")},
		{K: Key([]byte("c")), V: []byte("3.1")},
		{K: Key([]byte("e")), V: []byte("")},
	})

	i2 := NewMockIterator([]struct {
		K KeyType
		V []byte
	}{
		{K: Key([]byte("a")), V: []byte("1.2")},
		{K: Key([]byte("b")), V: []byte("2.2")},
		{K: Key([]byte("c")), V: []byte("3.2")},
		{K: Key([]byte("d")), V: []byte("4.2")},
	})

	i3 := NewMockIterator([]struct {
		K KeyType
		V []byte
	}{
		{K: Key([]byte("b")), V: []byte("2.3")},
		{K: Key([]byte("c")), V: []byte("3.3")},
		{K: Key([]byte("d")), V: []byte("4.3")},
	})

	iter := []StorageIterator{i1.Clone(), i2.Clone(), i3.Clone()}
	mergeIter := CreateMergeIterator(iter)
	CheckIterResultByKey(t, mergeIter, []struct {
		K KeyType
		V []byte
	}{
		{K: Key([]byte("a")), V: []byte("1.1")},
		{K: Key([]byte("b")), V: []byte("2.1")},
		{K: Key([]byte("c")), V: []byte("3.1")},
		{K: Key([]byte("d")), V: []byte("4.2")},
		{K: Key([]byte("e")), V: []byte("")},
	})

	iter1 := []StorageIterator{i3, i1, i2}
	mergeIter1 := CreateMergeIterator(iter1)
	CheckIterResultByKey(t, mergeIter1, []struct {
		K KeyType
		V []byte
	}{
		{K: Key([]byte("a")), V: []byte("1.1")},
		{K: Key([]byte("b")), V: []byte("2.3")},
		{K: Key([]byte("c")), V: []byte("3.3")},
		{K: Key([]byte("d")), V: []byte("4.3")},
		{K: Key([]byte("e")), V: []byte("")},
	})

}

func TestTask2Merge2(t *testing.T) {
	i1 := NewMockIterator([]struct {
		K KeyType
		V []byte
	}{
		{K: Key([]byte("a")), V: []byte("1.1")},
		{K: Key([]byte("b")), V: []byte("2.1")},
		{K: Key([]byte("c")), V: []byte("3.1")},
	})

	i2 := NewMockIterator([]struct {
		K KeyType
		V []byte
	}{
		{K: Key([]byte("d")), V: []byte("1.2")},
		{K: Key([]byte("e")), V: []byte("2.2")},
		{K: Key([]byte("f")), V: []byte("3.2")},
		{K: Key([]byte("g")), V: []byte("4.2")},
	})

	i3 := NewMockIterator([]struct {
		K KeyType
		V []byte
	}{
		{K: Key([]byte("h")), V: []byte("1.3")},
		{K: Key([]byte("i")), V: []byte("2.3")},
		{K: Key([]byte("j")), V: []byte("3.3")},
		{K: Key([]byte("k")), V: []byte("4.3")},
	})

	i4 := NewMockIterator([]struct {
		K KeyType
		V []byte
	}{})
	result := []struct {
		K KeyType
		V []byte
	}{
		{K: Key([]byte("a")), V: []byte("1.1")},
		{K: Key([]byte("b")), V: []byte("2.1")},
		{K: Key([]byte("c")), V: []byte("3.1")},
		{K: Key([]byte("d")), V: []byte("1.2")},
		{K: Key([]byte("e")), V: []byte("2.2")},
		{K: Key([]byte("f")), V: []byte("3.2")},
		{K: Key([]byte("g")), V: []byte("4.2")},
		{K: Key([]byte("h")), V: []byte("1.3")},
		{K: Key([]byte("i")), V: []byte("2.3")},
		{K: Key([]byte("j")), V: []byte("3.3")},
		{K: Key([]byte("k")), V: []byte("4.3")},
	}
	CheckIterResultByKey(t, CreateMergeIterator([]StorageIterator{i1.Clone(), i2.Clone(), i3.Clone(), i4.Clone()}), result)
	CheckIterResultByKey(t, CreateMergeIterator([]StorageIterator{i2.Clone(), i4.Clone(), i3.Clone(), i1.Clone()}), result)
	CheckIterResultByKey(t, CreateMergeIterator([]StorageIterator{i4, i3, i2, i1}), result)
}

func TestTask2MergeEmpty(t *testing.T) {
	i1 := NewMockIterator([]struct {
		K KeyType
		V []byte
	}{})
	CheckIterResultByKey(t, CreateMergeIterator([]StorageIterator{i1}), []struct {
		K KeyType
		V []byte
	}{})

	i2 := NewMockIterator([]struct {
		K KeyType
		V []byte
	}{
		{K: Key([]byte("a")), V: []byte("1.1")},
		{K: Key([]byte("b")), V: []byte("2.1")},
		{K: Key([]byte("c")), V: []byte("3.1")},
	})
	i3 := NewMockIterator([]struct {
		K KeyType
		V []byte
	}{})
	CheckIterResultByKey(t, CreateMergeIterator([]StorageIterator{i2, i3}), []struct {
		K KeyType
		V []byte
	}{
		{K: Key([]byte("a")), V: []byte("1.1")},
		{K: Key([]byte("b")), V: []byte("2.1")},
		{K: Key([]byte("c")), V: []byte("3.1")},
	})
}

func TestTask2MergeError(t *testing.T) {
	i1 := NewMockIterator([]struct {
		K KeyType
		V []byte
	}{})
	CheckIterResultByKey(t, CreateMergeIterator([]StorageIterator{i1}), []struct {
		K KeyType
		V []byte
	}{})

	i2 := NewMockIterator([]struct {
		K KeyType
		V []byte
	}{
		{K: Key([]byte("a")), V: []byte("1.1")},
		{K: Key([]byte("b")), V: []byte("2.1")},
		{K: Key([]byte("c")), V: []byte("3.1")},
	})
	i3 := NewMockIteratorWithError([]struct {
		K KeyType
		V []byte
	}{
		{K: Key([]byte("a")), V: []byte("1.1")},
		{K: Key([]byte("b")), V: []byte("2.1")},
		{K: Key([]byte("c")), V: []byte("3.1")},
	}, 1)
	// 这里为什么会得到一个error呢？
	// 根据我们的实现，我们在构造MergeIterator的时候，会先把所有的iterator都放到一个heap中，然后取出最小的一个
	// 如果heap中的iterator的Key和current的key相等时，我们会将heap中的iterator移向下一个元素，如果此时出现错误，则整个迭代器将终止并且返回这个error
	iter := CreateMergeIterator([]StorageIterator{i2.Clone(), i2, i3})
	for {
		err := iter.Next()
		if err != nil {
			break
		} else if !iter.IsValid() {
			panic("expect an error")
		}
	}
}

func TestTask3FusedIterator(t *testing.T) {
	i1 := NewMockIterator([]struct {
		K KeyType
		V []byte
	}{})
	fusedIter := CreateFusedIterator(i1)
	assert.True(t, !fusedIter.IsValid())
	assert.NoError(t, fusedIter.Next())
	assert.NoError(t, fusedIter.Next())
	assert.NoError(t, fusedIter.Next())
	assert.True(t, !fusedIter.IsValid())

	i2 := NewMockIteratorWithError([]struct {
		K KeyType
		V []byte
	}{
		{K: Key([]byte("a")), V: []byte("1.1")},
		{K: Key([]byte("a")), V: []byte("2.1")},
	}, 1)
	fusedIter = CreateFusedIterator(i2)
	assert.True(t, fusedIter.IsValid())
	assert.Error(t, fusedIter.Next())
	assert.True(t, !fusedIter.IsValid())
	assert.Error(t, fusedIter.Next())
	assert.Error(t, fusedIter.Next())
}

func TestTask4Integration(t *testing.T) {
	dir := t.TempDir()
	storage, err := OpenLsmStorageInner(dir, DefaultForWeek1Test())
	assert.NoError(t, err)
	assert.NoError(t, storage.Put(Key([]byte("1")), []byte("233")))
	assert.NoError(t, storage.Put(Key([]byte("2")), []byte("2333")))
	assert.NoError(t, storage.Put(Key([]byte("3")), []byte("23333")))
	storage.stateLock.Lock()
	err = storage.ForceFreezeMemTable()
	assert.NoError(t, err)
	storage.stateLock.Unlock()
	assert.NoError(t, storage.Delete(Key([]byte("1"))))
	assert.NoError(t, storage.Delete(Key([]byte("2"))))
	assert.NoError(t, storage.Put(Key([]byte("3")), []byte("2333")))
	assert.NoError(t, storage.Put(Key([]byte("4")), []byte("23333")))
	storage.stateLock.Lock()
	err = storage.ForceFreezeMemTable()
	assert.NoError(t, err)
	storage.stateLock.Unlock()
	assert.NoError(t, storage.Put(Key([]byte("1")), []byte("233333")))
	assert.NoError(t, storage.Put(Key([]byte("3")), []byte("233333")))
	{
		iter, err := storage.Scan(Unbound(), Unbound())
		assert.NoError(t, err)
		CheckLsmIterResultByKey(t, iter, []struct {
			K KeyType
			V []byte
		}{
			{K: Key([]byte("1")), V: []byte("233333")},
			{K: Key([]byte("3")), V: []byte("233333")},
			{K: Key([]byte("4")), V: []byte("23333")},
		})
	}

	{
		iter, err := storage.Scan(Include(Key([]byte("2"))), Include(Key([]byte("3"))))
		assert.NoError(t, err)
		CheckLsmIterResultByKey(t, iter, []struct {
			K KeyType
			V []byte
		}{
			{K: Key([]byte("3")), V: []byte("233333")},
		})
		assert.True(t, !iter.IsValid())
		assert.NoError(t, iter.Next())
		assert.NoError(t, iter.Next())
		assert.NoError(t, iter.Next())
		assert.True(t, !iter.IsValid())
	}
	assert.NoError(t, storage.Close())
}
