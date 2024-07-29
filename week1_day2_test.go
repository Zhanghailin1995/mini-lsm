package mini_lsm

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTask1MemTableIter(t *testing.T) {
	memTable := CreateMemTable(0)
	assert.NoError(t, memTable.ForTestingPutSlice(KeyOf([]byte("key1")), []byte("value1")))
	assert.NoError(t, memTable.ForTestingPutSlice(KeyOf([]byte("key2")), []byte("value2")))
	assert.NoError(t, memTable.ForTestingPutSlice(KeyOf([]byte("key3")), []byte("value3")))

	{
		iter := memTable.Scan(Unbound(), Unbound())
		assert.Equal(t, iter.IsValid(), true)
		assert.Equal(t, iter.Key().KeyRef(), []byte("key1"))
		assert.Equal(t, iter.Value(), []byte("value1"))
		assert.NoError(t, iter.Next())
		assert.Equal(t, iter.IsValid(), true)
		assert.Equal(t, iter.Key().KeyRef(), []byte("key2"))
		assert.Equal(t, iter.Value(), []byte("value2"))
		assert.NoError(t, iter.Next())
		assert.Equal(t, iter.IsValid(), true)
		assert.Equal(t, iter.Key().KeyRef(), []byte("key3"))
		assert.Equal(t, iter.Value(), []byte("value3"))
		assert.NoError(t, iter.Next())
		assert.Equal(t, iter.IsValid(), false)
	}

	{
		lower := Include(KeyOf([]byte("key1")))
		upper := Include(KeyOf([]byte("key2")))
		iter := memTable.Scan(lower, upper)
		assert.Equal(t, iter.IsValid(), true)
		assert.Equal(t, iter.Key().KeyRef(), []byte("key1"))
		assert.Equal(t, iter.Value(), []byte("value1"))
		assert.NoError(t, iter.Next())
		assert.Equal(t, iter.IsValid(), true)
		assert.Equal(t, iter.Key().KeyRef(), []byte("key2"))
		assert.Equal(t, iter.Value(), []byte("value2"))
		assert.NoError(t, iter.Next())
		assert.Equal(t, iter.IsValid(), false)
	}

	{
		lower := Exclude(KeyOf([]byte("key1")))
		upper := Exclude(KeyOf([]byte("key3")))
		iter := memTable.Scan(lower, upper)
		assert.Equal(t, iter.IsValid(), true)
		assert.Equal(t, iter.Key().KeyRef(), []byte("key2"))
		assert.Equal(t, iter.Value(), []byte("value2"))
		assert.NoError(t, iter.Next())
		assert.Equal(t, iter.IsValid(), false)
	}
}

func TestTask1EmptyMemTableIter(t *testing.T) {
	memTable := CreateMemTable(0)
	iter := memTable.Scan(Unbound(), Unbound())
	assert.Equal(t, iter.IsValid(), false)

	lower := Exclude(KeyOf([]byte("key1")))
	upper := Exclude(KeyOf([]byte("key3")))
	iter = memTable.Scan(lower, upper)
	assert.Equal(t, iter.IsValid(), false)

	lower = Include(KeyOf([]byte("key1")))
	upper = Include(KeyOf([]byte("key2")))
	assert.Equal(t, iter.IsValid(), false)
}

func TestTask2Merge1(t *testing.T) {
	i1 := NewMockIterator([]struct {
		K KeyBytes
		V []byte
	}{
		{K: KeyOf([]byte("a")), V: []byte("1.1")},
		{K: KeyOf([]byte("b")), V: []byte("2.1")},
		{K: KeyOf([]byte("c")), V: []byte("3.1")},
		{K: KeyOf([]byte("e")), V: []byte("")},
	})

	i2 := NewMockIterator([]struct {
		K KeyBytes
		V []byte
	}{
		{K: KeyOf([]byte("a")), V: []byte("1.2")},
		{K: KeyOf([]byte("b")), V: []byte("2.2")},
		{K: KeyOf([]byte("c")), V: []byte("3.2")},
		{K: KeyOf([]byte("d")), V: []byte("4.2")},
	})

	i3 := NewMockIterator([]struct {
		K KeyBytes
		V []byte
	}{
		{K: KeyOf([]byte("b")), V: []byte("2.3")},
		{K: KeyOf([]byte("c")), V: []byte("3.3")},
		{K: KeyOf([]byte("d")), V: []byte("4.3")},
	})

	iter := []StorageIterator{i1.Clone(), i2.Clone(), i3.Clone()}
	mergeIter := CreateMergeIterator(iter)
	CheckIterResultByKey(t, mergeIter, []struct {
		K KeyBytes
		V []byte
	}{
		{K: KeyOf([]byte("a")), V: []byte("1.1")},
		{K: KeyOf([]byte("b")), V: []byte("2.1")},
		{K: KeyOf([]byte("c")), V: []byte("3.1")},
		{K: KeyOf([]byte("d")), V: []byte("4.2")},
		{K: KeyOf([]byte("e")), V: []byte("")},
	})

	iter1 := []StorageIterator{i3, i1, i2}
	mergeIter1 := CreateMergeIterator(iter1)
	CheckIterResultByKey(t, mergeIter1, []struct {
		K KeyBytes
		V []byte
	}{
		{K: KeyOf([]byte("a")), V: []byte("1.1")},
		{K: KeyOf([]byte("b")), V: []byte("2.3")},
		{K: KeyOf([]byte("c")), V: []byte("3.3")},
		{K: KeyOf([]byte("d")), V: []byte("4.3")},
		{K: KeyOf([]byte("e")), V: []byte("")},
	})

}

func TestTask2Merge2(t *testing.T) {
	i1 := NewMockIterator([]struct {
		K KeyBytes
		V []byte
	}{
		{K: KeyOf([]byte("a")), V: []byte("1.1")},
		{K: KeyOf([]byte("b")), V: []byte("2.1")},
		{K: KeyOf([]byte("c")), V: []byte("3.1")},
	})

	i2 := NewMockIterator([]struct {
		K KeyBytes
		V []byte
	}{
		{K: KeyOf([]byte("d")), V: []byte("1.2")},
		{K: KeyOf([]byte("e")), V: []byte("2.2")},
		{K: KeyOf([]byte("f")), V: []byte("3.2")},
		{K: KeyOf([]byte("g")), V: []byte("4.2")},
	})

	i3 := NewMockIterator([]struct {
		K KeyBytes
		V []byte
	}{
		{K: KeyOf([]byte("h")), V: []byte("1.3")},
		{K: KeyOf([]byte("i")), V: []byte("2.3")},
		{K: KeyOf([]byte("j")), V: []byte("3.3")},
		{K: KeyOf([]byte("k")), V: []byte("4.3")},
	})

	i4 := NewMockIterator([]struct {
		K KeyBytes
		V []byte
	}{})
	result := []struct {
		K KeyBytes
		V []byte
	}{
		{K: KeyOf([]byte("a")), V: []byte("1.1")},
		{K: KeyOf([]byte("b")), V: []byte("2.1")},
		{K: KeyOf([]byte("c")), V: []byte("3.1")},
		{K: KeyOf([]byte("d")), V: []byte("1.2")},
		{K: KeyOf([]byte("e")), V: []byte("2.2")},
		{K: KeyOf([]byte("f")), V: []byte("3.2")},
		{K: KeyOf([]byte("g")), V: []byte("4.2")},
		{K: KeyOf([]byte("h")), V: []byte("1.3")},
		{K: KeyOf([]byte("i")), V: []byte("2.3")},
		{K: KeyOf([]byte("j")), V: []byte("3.3")},
		{K: KeyOf([]byte("k")), V: []byte("4.3")},
	}
	CheckIterResultByKey(t, CreateMergeIterator([]StorageIterator{i1.Clone(), i2.Clone(), i3.Clone(), i4.Clone()}), result)
	CheckIterResultByKey(t, CreateMergeIterator([]StorageIterator{i2.Clone(), i4.Clone(), i3.Clone(), i1.Clone()}), result)
	CheckIterResultByKey(t, CreateMergeIterator([]StorageIterator{i4, i3, i2, i1}), result)
}

func TestTask2MergeEmpty(t *testing.T) {
	i1 := NewMockIterator([]struct {
		K KeyBytes
		V []byte
	}{})
	CheckIterResultByKey(t, CreateMergeIterator([]StorageIterator{i1}), []struct {
		K KeyBytes
		V []byte
	}{})

	i2 := NewMockIterator([]struct {
		K KeyBytes
		V []byte
	}{
		{K: KeyOf([]byte("a")), V: []byte("1.1")},
		{K: KeyOf([]byte("b")), V: []byte("2.1")},
		{K: KeyOf([]byte("c")), V: []byte("3.1")},
	})
	i3 := NewMockIterator([]struct {
		K KeyBytes
		V []byte
	}{})
	CheckIterResultByKey(t, CreateMergeIterator([]StorageIterator{i2, i3}), []struct {
		K KeyBytes
		V []byte
	}{
		{K: KeyOf([]byte("a")), V: []byte("1.1")},
		{K: KeyOf([]byte("b")), V: []byte("2.1")},
		{K: KeyOf([]byte("c")), V: []byte("3.1")},
	})
}

func TestTask2MergeError(t *testing.T) {
	i1 := NewMockIterator([]struct {
		K KeyBytes
		V []byte
	}{})
	CheckIterResultByKey(t, CreateMergeIterator([]StorageIterator{i1}), []struct {
		K KeyBytes
		V []byte
	}{})

	i2 := NewMockIterator([]struct {
		K KeyBytes
		V []byte
	}{
		{K: KeyOf([]byte("a")), V: []byte("1.1")},
		{K: KeyOf([]byte("b")), V: []byte("2.1")},
		{K: KeyOf([]byte("c")), V: []byte("3.1")},
	})
	i3 := NewMockIteratorWithError([]struct {
		K KeyBytes
		V []byte
	}{
		{K: KeyOf([]byte("a")), V: []byte("1.1")},
		{K: KeyOf([]byte("b")), V: []byte("2.1")},
		{K: KeyOf([]byte("c")), V: []byte("3.1")},
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
		K KeyBytes
		V []byte
	}{})
	fusedIter := CreateFusedIterator(i1)
	assert.True(t, !fusedIter.IsValid())
	assert.NoError(t, fusedIter.Next())
	assert.NoError(t, fusedIter.Next())
	assert.NoError(t, fusedIter.Next())
	assert.True(t, !fusedIter.IsValid())

	i2 := NewMockIteratorWithError([]struct {
		K KeyBytes
		V []byte
	}{
		{K: KeyOf([]byte("a")), V: []byte("1.1")},
		{K: KeyOf([]byte("a")), V: []byte("2.1")},
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
	assert.NoError(t, storage.Put([]byte("1"), []byte("233")))
	assert.NoError(t, storage.Put([]byte("2"), []byte("2333")))
	assert.NoError(t, storage.Put([]byte("3"), []byte("23333")))
	storage.stateLock.Lock()
	err = storage.ForceFreezeMemTable()
	assert.NoError(t, err)
	storage.stateLock.Unlock()
	assert.NoError(t, storage.Delete([]byte("1")))
	assert.NoError(t, storage.Delete([]byte("2")))
	assert.NoError(t, storage.Put([]byte("3"), []byte("2333")))
	assert.NoError(t, storage.Put([]byte("4"), []byte("23333")))
	storage.stateLock.Lock()
	err = storage.ForceFreezeMemTable()
	assert.NoError(t, err)
	storage.stateLock.Unlock()
	assert.NoError(t, storage.Put([]byte("1"), []byte("233333")))
	assert.NoError(t, storage.Put([]byte("3"), []byte("233333")))
	{
		iter, err := storage.Scan(UnboundBytes(), UnboundBytes())
		assert.NoError(t, err)
		CheckLsmIterResultByKey1(t, iter, []StringKeyValuePair{
			{"1", "233333"},
			{"3", "233333"},
			{"4", "23333"},
		})
	}

	{
		iter, err := storage.Scan(IncludeBytes([]byte("2")), IncludeBytes([]byte("3")))
		assert.NoError(t, err)
		CheckLsmIterResultByKey1(t, iter, []StringKeyValuePair{
			{"3", "233333"},
		})
		assert.True(t, !iter.IsValid())
		assert.NoError(t, iter.Next())
		assert.NoError(t, iter.Next())
		assert.NoError(t, iter.Next())
		assert.True(t, !iter.IsValid())
	}
	assert.NoError(t, storage.Close())
}
