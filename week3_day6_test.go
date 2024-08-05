package mini_lsm

import (
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSerializable1(t *testing.T) {
	dir := t.TempDir()
	options := DefaultForWeek2Test(&CompactionOptions{
		CompactionType: NoCompaction,
	})
	options.Serializable = true
	storage := utils.Unwrap(Open(dir, options))
	assert.NoError(t, storage.Put(b("key1"), b("1")))
	assert.NoError(t, storage.Put(b("key2"), b("2")))
	txn1 := utils.Unwrap(storage.NewTxn())
	txn2 := utils.Unwrap(storage.NewTxn())
	txn1.Put(b("key1"), utils.Unwrap(txn1.Get(b("key2"))))
	txn2.Put(b("key2"), utils.Unwrap(txn2.Get(b("key1"))))
	assert.NoError(t, txn1.Commit())
	assert.True(t, txn2.Commit() != nil)
	txn2.End()
	get, err := storage.Get(b("key1"))
	assert.NoError(t, err)
	assert.Equal(t, get, b("2"))
	get = utils.Unwrap(storage.Get(b("key2")))
	assert.Equal(t, get, b("2"))
	assert.NoError(t, storage.Shutdown())
}

func TestSerializable2(t *testing.T) {
	dir := t.TempDir()
	options := DefaultForWeek2Test(&CompactionOptions{
		CompactionType: NoCompaction,
	})
	options.Serializable = true
	storage := utils.Unwrap(Open(dir, options))
	txn1 := utils.Unwrap(storage.NewTxn())
	txn2 := utils.Unwrap(storage.NewTxn())
	txn1.Put(b("key1"), b("1"))
	txn2.Put(b("key1"), b("2"))
	assert.NoError(t, txn1.Commit())
	assert.NoError(t, txn2.Commit())
	get, err := storage.Get(b("key1"))
	assert.NoError(t, err)
	assert.Equal(t, get, b("2"))
	assert.NoError(t, storage.Shutdown())
}

func TestSerializable3TsRange(t *testing.T) {
	dir := t.TempDir()
	options := DefaultForWeek2Test(&CompactionOptions{
		CompactionType: NoCompaction,
	})
	options.Serializable = true
	storage := utils.Unwrap(Open(dir, options))
	assert.NoError(t, storage.Put(b("key1"), b("1")))
	assert.NoError(t, storage.Put(b("key2"), b("2")))
	txn1 := utils.Unwrap(storage.NewTxn())
	txn1.Put(b("key1"), utils.Unwrap(txn1.Get(b("key2"))))
	assert.NoError(t, txn1.Commit())
	txn2 := utils.Unwrap(storage.NewTxn())
	txn2.Put(b("key2"), utils.Unwrap(txn2.Get(b("key1"))))
	assert.NoError(t, txn2.Commit())
	txn2.End()
	get, err := storage.Get(b("key1"))
	assert.NoError(t, err)
	assert.Equal(t, get, b("2"))
	get = utils.Unwrap(storage.Get(b("key2")))
	assert.Equal(t, get, b("2"))
	assert.NoError(t, storage.Shutdown())
}

func TestSerializable4Scan(t *testing.T) {
	dir := t.TempDir()
	options := DefaultForWeek2Test(&CompactionOptions{
		CompactionType: NoCompaction,
	})
	options.Serializable = true
	storage := utils.Unwrap(Open(dir, options))
	assert.NoError(t, storage.Put(b("key1"), b("1")))
	assert.NoError(t, storage.Put(b("key2"), b("2")))
	txn1 := utils.Unwrap(storage.NewTxn())
	txn2 := utils.Unwrap(storage.NewTxn())
	txn1.Put(b("key1"), utils.Unwrap(txn1.Get(b("key2"))))
	assert.NoError(t, txn1.Commit())
	iter := utils.Unwrap(txn2.Scan(UnboundBytes(), UnboundBytes()))
	for iter.IsValid() {
		assert.NoError(t, iter.Next())
	}
	txn2.Put(b("key2"), b("1"))
	err := txn2.Commit()
	assert.True(t, err != nil)
	txn2.End()
	get, err := storage.Get(b("key1"))
	assert.NoError(t, err)
	assert.Equal(t, get, b("2"))
	get = utils.Unwrap(storage.Get(b("key2")))
	assert.Equal(t, get, b("2"))
	assert.NoError(t, storage.Shutdown())
}

func TestSerializable5ReadOnly(t *testing.T) {
	dir := t.TempDir()
	options := DefaultForWeek2Test(&CompactionOptions{
		CompactionType: NoCompaction,
	})
	options.Serializable = true
	storage := utils.Unwrap(Open(dir, options))
	assert.NoError(t, storage.Put(b("key1"), b("1")))
	assert.NoError(t, storage.Put(b("key2"), b("2")))
	txn1 := utils.Unwrap(storage.NewTxn())
	txn1.Put(b("key1"), utils.Unwrap(txn1.Get(b("key2"))))
	assert.NoError(t, txn1.Commit())
	txn2 := utils.Unwrap(storage.NewTxn())
	_, err := txn2.Get(b("key1"))
	assert.NoError(t, err)
	iter := utils.Unwrap(txn2.Scan(UnboundBytes(), UnboundBytes()))
	for iter.IsValid() {
		assert.NoError(t, iter.Next())
	}
	assert.NoError(t, txn2.Commit())
	get, err := storage.Get(b("key1"))
	assert.NoError(t, err)
	assert.Equal(t, get, b("2"))
	get = utils.Unwrap(storage.Get(b("key2")))
	assert.Equal(t, get, b("2"))
	assert.NoError(t, storage.Shutdown())
}
