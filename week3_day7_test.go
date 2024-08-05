package mini_lsm

import (
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTask3MvccCompaction1(t *testing.T) {
	dir := t.TempDir()
	options := DefaultForWeek2Test(&CompactionOptions{
		CompactionType: NoCompaction,
	})
	storage := utils.Unwrap(Open(dir, options))
	err := storage.WriteBatch([]WriteBatchRecord{
		&PutOp{K: b("table1_a"), V: b("1")},
		&PutOp{K: b("table1_b"), V: b("1")},
		&PutOp{K: b("table1_c"), V: b("1")},
		&PutOp{K: b("table2_a"), V: b("1")},
		&PutOp{K: b("table2_b"), V: b("1")},
		&PutOp{K: b("table2_c"), V: b("1")},
	})
	assert.NoError(t, err)
	assert.NoError(t, storage.ForceFlush())
	snapshot0 := utils.Unwrap(storage.NewTxn())
	err = storage.WriteBatch([]WriteBatchRecord{
		&PutOp{K: b("table1_a"), V: b("2")},
		&DelOp{K: b("table1_b")},
		&PutOp{K: b("table1_c"), V: b("2")},
		&PutOp{K: b("table2_a"), V: b("2")},
		&DelOp{K: b("table2_b")},
		&PutOp{K: b("table2_c"), V: b("2")},
	})
	assert.NoError(t, err)
	assert.NoError(t, storage.ForceFlush())
	storage.AddCompactionFilter(&PrefixCompactionFilter{prefix: b("table2_")})
	assert.NoError(t, storage.ForceFullCompaction())

	iter := ConstructMergeIteratorOverStorage(storage.inner.ReadState())
	CheckIterResultByKey1(t, iter, []StringKeyValuePair{
		{"table1_a", "2"},
		{"table1_a", "1"},
		{"table1_b", ""},
		{"table1_b", "1"},
		{"table1_c", "2"},
		{"table1_c", "1"},
		{"table2_a", "2"},
		{"table2_b", ""},
		{"table2_c", "2"},
	})
	snapshot0.End()
	assert.NoError(t, storage.ForceFullCompaction())
	iter = ConstructMergeIteratorOverStorage(storage.inner.ReadState())
	CheckIterResultByKey1(t, iter, []StringKeyValuePair{
		{"table1_a", "2"},
		{"table1_c", "2"},
	})
	assert.NoError(t, storage.Shutdown())

}
