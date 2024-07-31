package mini_lsm

import (
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTask1WaterMark(t *testing.T) {
	watermark := NewWaterMark()
	watermark.AddReader(0)
	for i := uint64(1); i <= 1000; i++ {
		watermark.AddReader(i)
		w, ok := watermark.Watermark()
		assert.True(t, ok && w == 0)
		assert.Equal(t, uint32(i+1), watermark.NumRetainedSnapshot())
	}
	cnt := 1001
	for i := uint64(0); i < 500; i++ {
		watermark.RemoveReader(i)
		w, _ := watermark.Watermark()
		assert.Equal(t, i+1, w)
		cnt--
		assert.Equal(t, uint32(cnt), watermark.NumRetainedSnapshot())
	}

	for i := uint64(1000); i >= 501; i-- {
		watermark.RemoveReader(i)
		w, _ := watermark.Watermark()
		assert.Equal(t, uint64(500), w)
		cnt--
		assert.Equal(t, uint32(cnt), watermark.NumRetainedSnapshot())
	}
	watermark.RemoveReader(500)
	_, ok := watermark.Watermark()
	assert.False(t, ok)
	assert.Equal(t, uint32(0), watermark.NumRetainedSnapshot())
	watermark.AddReader(2000)
	watermark.AddReader(2000)
	watermark.AddReader(2001)
	assert.Equal(t, uint32(2), watermark.NumRetainedSnapshot())
	w, ok := watermark.Watermark()
	assert.True(t, ok && w == 2000)
	watermark.RemoveReader(2000)
	assert.Equal(t, uint32(2), watermark.NumRetainedSnapshot())
	w, ok = watermark.Watermark()
	assert.True(t, ok && w == 2000)
	watermark.RemoveReader(2000)
	assert.Equal(t, uint32(1), watermark.NumRetainedSnapshot())
	w, ok = watermark.Watermark()
	assert.True(t, ok && w == 2001)
}

func TestTask2SnapshotWatermark(t *testing.T) {
	dir := t.TempDir()
	options := DefaultForWeek2Test(&CompactionOptions{
		CompactionType: NoCompaction,
	})
	storage := utils.Unwrap(Open(dir, options))
	txn1 := utils.Unwrap(storage.NewTxn())
	txn2 := utils.Unwrap(storage.NewTxn())
	assert.NoError(t, storage.Put(b("233"), b("23333")))
	txn3 := utils.Unwrap(storage.NewTxn())
	watermark := storage.inner.Mvcc().Watermark()
	assert.Equal(t, txn1.ReadTs, watermark)
	txn1.End()
	watermark = storage.inner.Mvcc().Watermark()
	assert.Equal(t, txn2.ReadTs, watermark)
	txn2.End()
	watermark = storage.inner.Mvcc().Watermark()
	assert.Equal(t, txn3.ReadTs, watermark)
	txn3.End()
	watermark = storage.inner.Mvcc().Watermark()
	assert.Equal(t, storage.inner.Mvcc().LatestCommitTs(), watermark)
	assert.NoError(t, storage.Shutdown())
}

func TestTask3MvccCompaction(t *testing.T) {
	dir := t.TempDir()
	options := DefaultForWeek2Test(&CompactionOptions{
		CompactionType: NoCompaction,
	})
	storage := utils.Unwrap(Open(dir, options))
	snapshot0 := utils.Unwrap(storage.NewTxn())
	assert.NoError(t, storage.WriteBatch([]WriteBatchRecord{
		&PutOp{K: b("a"), V: b("1")},
		&PutOp{K: b("b"), V: b("1")},
	}))
	snapshot1 := utils.Unwrap(storage.NewTxn())
	assert.NoError(t, storage.WriteBatch([]WriteBatchRecord{
		&PutOp{K: b("a"), V: b("2")},
		&PutOp{K: b("d"), V: b("2")},
	}))
	snapshot2 := utils.Unwrap(storage.NewTxn())
	assert.NoError(t, storage.WriteBatch([]WriteBatchRecord{
		&PutOp{K: b("a"), V: b("3")},
		&DelOp{K: b("d")},
	}))
	snapshot3 := utils.Unwrap(storage.NewTxn())
	assert.NoError(t, storage.WriteBatch([]WriteBatchRecord{
		&PutOp{K: b("c"), V: b("4")},
		&DelOp{K: b("a")},
	}))
	assert.NoError(t, storage.ForceFlush())
	assert.NoError(t, storage.ForceFullCompaction())
	storage.inner.rwLock.RLock()
	iter := ConstructMergeIteratorOverStorage(storage.inner.state)
	storage.inner.rwLock.RUnlock()
	CheckIterResultByKey1(t, iter, []StringKeyValuePair{
		{"a", ""},
		{"a", "3"},
		{"a", "2"},
		{"a", "1"},
		{"b", "1"},
		{"c", "4"},
		{"d", ""},
		{"d", "2"},
	})

	snapshot0.End()
	assert.NoError(t, storage.ForceFullCompaction())

	storage.inner.rwLock.RLock()
	iter0 := ConstructMergeIteratorOverStorage(storage.inner.state)
	storage.inner.rwLock.RUnlock()
	CheckIterResultByKey1(t, iter0, []StringKeyValuePair{
		{"a", ""},
		{"a", "3"},
		{"a", "2"},
		{"a", "1"},
		{"b", "1"},
		{"c", "4"},
		{"d", ""},
		{"d", "2"},
	})

	storage.inner.rwLock.RLock()
	iter1 := ConstructMergeIteratorOverStorage(storage.inner.state)
	storage.inner.rwLock.RUnlock()
	CheckIterResultByKey1(t, iter1, []StringKeyValuePair{
		{"a", ""},
		{"a", "3"},
		{"a", "2"},
		{"a", "1"},
		{"b", "1"},
		{"c", "4"},
		{"d", ""},
		{"d", "2"},
	})

	snapshot1.End()

	assert.NoError(t, storage.ForceFullCompaction())
	storage.inner.rwLock.RLock()
	iter2 := ConstructMergeIteratorOverStorage(storage.inner.state)
	storage.inner.rwLock.RUnlock()
	CheckIterResultByKey1(t, iter2, []StringKeyValuePair{
		{"a", ""},
		{"a", "3"},
		{"a", "2"},
		{"b", "1"},
		{"c", "4"},
		{"d", ""},
		{"d", "2"},
	})

	snapshot2.End()

	assert.NoError(t, storage.ForceFullCompaction())
	storage.inner.rwLock.RLock()
	iter3 := ConstructMergeIteratorOverStorage(storage.inner.state)
	storage.inner.rwLock.RUnlock()
	CheckIterResultByKey1(t, iter3, []StringKeyValuePair{
		{"a", ""},
		{"a", "3"},
		{"b", "1"},
		{"c", "4"},
	})

	snapshot3.End()
	assert.NoError(t, storage.ForceFullCompaction())

	storage.inner.rwLock.RLock()
	iter4 := ConstructMergeIteratorOverStorage(storage.inner.state)
	storage.inner.rwLock.RUnlock()
	CheckIterResultByKey1(t, iter4, []StringKeyValuePair{
		{"b", "1"},
		{"c", "4"},
	})

	assert.NoError(t, storage.Shutdown())

}
