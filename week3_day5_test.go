package mini_lsm

import (
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTxnIntegration(t *testing.T) {
	dir := t.TempDir()
	options := DefaultForWeek2Test(&CompactionOptions{
		CompactionType: NoCompaction,
	})
	storage := utils.Unwrap(Open(dir, options))
	txn1 := utils.Unwrap(storage.NewTxn())
	txn2 := utils.Unwrap(storage.NewTxn())
	txn1.Put(b("test1"), b("233"))
	txn2.Put(b("test2"), b("233"))
	txnIterator1, err := txn1.Scan(UnboundBytes(), UnboundBytes())
	assert.NoError(t, err)
	CheckLsmIterResultByKey1(t, txnIterator1, []StringKeyValuePair{
		{"test1", "233"},
	})

	txnIterator2, err := txn2.Scan(UnboundBytes(), UnboundBytes())
	assert.NoError(t, err)
	CheckLsmIterResultByKey1(t, txnIterator2, []StringKeyValuePair{
		{"test2", "233"},
	})
	txn3 := utils.Unwrap(storage.NewTxn())
	txnIterator3, err := txn3.Scan(UnboundBytes(), UnboundBytes())
	assert.NoError(t, err)
	CheckLsmIterResultByKey1(t, txnIterator3, []StringKeyValuePair{})
	assert.NoError(t, txn1.Commit())
	assert.NoError(t, txn2.Commit())
	txnIterator3, err = txn3.Scan(UnboundBytes(), UnboundBytes())
	assert.NoError(t, err)
	CheckLsmIterResultByKey1(t, txnIterator3, []StringKeyValuePair{})
	txn3.End()
	storageIterator, err := storage.Scan(UnboundBytes(), UnboundBytes())
	assert.NoError(t, err)
	CheckLsmIterResultByKey1(t, storageIterator, []StringKeyValuePair{
		{"test1", "233"},
		{"test2", "233"},
	})
	txn4 := utils.Unwrap(storage.NewTxn())
	get, err := txn4.Get(b("test1"))
	assert.NoError(t, err)
	assert.Equal(t, "233", string(get))
	get, err = txn4.Get(b("test2"))
	assert.NoError(t, err)
	assert.Equal(t, "233", string(get))
	txnIterator4, err := txn4.Scan(UnboundBytes(), UnboundBytes())
	assert.NoError(t, err)
	CheckLsmIterResultByKey1(t, txnIterator4, []StringKeyValuePair{
		{"test1", "233"},
		{"test2", "233"},
	})
	txn4.Put(b("test2"), b("2333"))
	get, err = txn4.Get(b("test1"))
	assert.NoError(t, err)
	assert.Equal(t, "233", string(get))
	get, err = txn4.Get(b("test2"))
	assert.NoError(t, err)
	assert.Equal(t, "2333", string(get))
	txnIterator4, err = txn4.Scan(UnboundBytes(), UnboundBytes())
	assert.NoError(t, err)
	CheckLsmIterResultByKey1(t, txnIterator4, []StringKeyValuePair{
		{"test1", "233"},
		{"test2", "2333"},
	})
	txn4.Delete(b("test2"))
	get, err = txn4.Get(b("test1"))
	assert.NoError(t, err)
	assert.Equal(t, "233", string(get))

	get, err = txn4.Get(b("test2"))
	assert.NoError(t, err)
	assert.Equal(t, "", string(get))
	txnIterator4, err = txn4.Scan(UnboundBytes(), UnboundBytes())
	assert.NoError(t, err)
	CheckLsmIterResultByKey1(t, txnIterator4, []StringKeyValuePair{
		{"test1", "233"},
	})

	assert.NoError(t, storage.Shutdown())
}
