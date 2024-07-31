package mini_lsm

import (
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"github.com/stretchr/testify/assert"
	"path"
	"testing"
)

func TestTask2MemtableMvcc(t *testing.T) {
	dir := t.TempDir()
	options := DefaultForWeek2Test(&CompactionOptions{
		CompactionType: NoCompaction,
	})
	options.EnableWal = true
	storage := utils.Unwrap(Open(dir, options))
	assert.NoError(t, storage.Put(b("a"), b("1")))
	assert.NoError(t, storage.Put(b("b"), b("1")))
	snapshot1 := utils.Unwrap(storage.NewTxn())
	assert.NoError(t, storage.Put(b("a"), b("2")))
	snapshot2 := utils.Unwrap(storage.NewTxn())
	assert.NoError(t, storage.Delete(b("b")))
	assert.NoError(t, storage.Put(b("c"), b("1")))
	snapshot3 := utils.Unwrap(storage.NewTxn())
	assert.Equal(t, "1", string(utils.Unwrap(snapshot1.Get(b("a")))))
	assert.Equal(t, "1", string(utils.Unwrap(snapshot1.Get(b("b")))))
	assert.Equal(t, []byte(nil), utils.Unwrap(snapshot1.Get(b("c"))))
	CheckLsmIterResultByKey1(t, utils.Unwrap(snapshot1.Scan(UnboundBytes(), UnboundBytes())), []StringKeyValuePair{
		{"a", "1"},
		{"b", "1"},
	})

	assert.Equal(t, "2", string(utils.Unwrap(snapshot2.Get(b("a")))))
	assert.Equal(t, "1", string(utils.Unwrap(snapshot2.Get(b("b")))))
	assert.Equal(t, []byte(nil), utils.Unwrap(snapshot2.Get(b("c"))))
	CheckLsmIterResultByKey1(t, utils.Unwrap(snapshot2.Scan(UnboundBytes(), UnboundBytes())), []StringKeyValuePair{
		{"a", "2"},
		{"b", "1"},
	})

	assert.Equal(t, "2", string(utils.Unwrap(snapshot3.Get(b("a")))))
	assert.Equal(t, []byte(nil), utils.Unwrap(snapshot3.Get(b("b"))))
	assert.Equal(t, "1", string(utils.Unwrap(snapshot3.Get(b("c")))))
	CheckLsmIterResultByKey1(t, utils.Unwrap(snapshot3.Scan(UnboundBytes(), UnboundBytes())), []StringKeyValuePair{
		{"a", "2"},
		{"c", "1"},
	})
	storage.inner.stateLock.Lock()
	assert.NoError(t, storage.inner.ForceFreezeMemTable())
	storage.inner.stateLock.Unlock()
	assert.NoError(t, storage.Put(b("a"), b("3")))
	assert.NoError(t, storage.Put(b("b"), b("3")))
	snapshot4 := utils.Unwrap(storage.NewTxn())
	assert.NoError(t, storage.Put(b("a"), b("4")))
	snapshot5 := utils.Unwrap(storage.NewTxn())
	assert.NoError(t, storage.Delete(b("b")))
	assert.NoError(t, storage.Put(b("c"), b("5")))
	snapshot6 := utils.Unwrap(storage.NewTxn())

	assert.Equal(t, "1", string(utils.Unwrap(snapshot1.Get(b("a")))))
	assert.Equal(t, "1", string(utils.Unwrap(snapshot1.Get(b("b")))))
	assert.Equal(t, []byte(nil), utils.Unwrap(snapshot1.Get(b("c"))))
	CheckLsmIterResultByKey1(t, utils.Unwrap(snapshot1.Scan(UnboundBytes(), UnboundBytes())), []StringKeyValuePair{
		{"a", "1"},
		{"b", "1"},
	})

	assert.Equal(t, "2", string(utils.Unwrap(snapshot2.Get(b("a")))))
	assert.Equal(t, "1", string(utils.Unwrap(snapshot2.Get(b("b")))))
	assert.Equal(t, []byte(nil), utils.Unwrap(snapshot2.Get(b("c"))))
	CheckLsmIterResultByKey1(t, utils.Unwrap(snapshot2.Scan(UnboundBytes(), UnboundBytes())), []StringKeyValuePair{
		{"a", "2"},
		{"b", "1"},
	})

	assert.Equal(t, "2", string(utils.Unwrap(snapshot3.Get(b("a")))))
	assert.Equal(t, []byte(nil), utils.Unwrap(snapshot3.Get(b("b"))))
	assert.Equal(t, "1", string(utils.Unwrap(snapshot3.Get(b("c")))))
	CheckLsmIterResultByKey1(t, utils.Unwrap(snapshot3.Scan(UnboundBytes(), UnboundBytes())), []StringKeyValuePair{
		{"a", "2"},
		{"c", "1"},
	})

	assert.Equal(t, "3", string(utils.Unwrap(snapshot4.Get(b("a")))))
	assert.Equal(t, "3", string(utils.Unwrap(snapshot4.Get(b("b")))))
	assert.Equal(t, "1", string(utils.Unwrap(snapshot4.Get(b("c")))))
	CheckLsmIterResultByKey1(t, utils.Unwrap(snapshot4.Scan(UnboundBytes(), UnboundBytes())), []StringKeyValuePair{
		{"a", "3"},
		{"b", "3"},
		{"c", "1"},
	})

	assert.Equal(t, "4", string(utils.Unwrap(snapshot5.Get(b("a")))))
	assert.Equal(t, "3", string(utils.Unwrap(snapshot5.Get(b("b")))))
	assert.Equal(t, "1", string(utils.Unwrap(snapshot5.Get(b("c")))))
	CheckLsmIterResultByKey1(t, utils.Unwrap(snapshot5.Scan(UnboundBytes(), UnboundBytes())), []StringKeyValuePair{
		{"a", "4"},
		{"b", "3"},
		{"c", "1"},
	})

	assert.Equal(t, "4", string(utils.Unwrap(snapshot6.Get(b("a")))))
	assert.Equal(t, []byte(nil), utils.Unwrap(snapshot6.Get(b("b"))))
	assert.Equal(t, "5", string(utils.Unwrap(snapshot6.Get(b("c")))))
	CheckLsmIterResultByKey1(t, utils.Unwrap(snapshot6.Scan(UnboundBytes(), UnboundBytes())), []StringKeyValuePair{
		{"a", "4"},
		{"c", "5"},
	})

	assert.NoError(t, storage.Shutdown())

}

func TestTask2LsmIteratorMvcc(t *testing.T) {
	dir := t.TempDir()
	options := DefaultForWeek2Test(&CompactionOptions{
		CompactionType: NoCompaction,
	})
	options.EnableWal = true
	storage := utils.Unwrap(Open(dir, options))
	assert.NoError(t, storage.Put(b("a"), b("1")))
	assert.NoError(t, storage.Put(b("b"), b("1")))
	snapshot1 := utils.Unwrap(storage.NewTxn())
	assert.NoError(t, storage.Put(b("a"), b("2")))
	snapshot2 := utils.Unwrap(storage.NewTxn())
	assert.NoError(t, storage.Delete(b("b")))
	assert.NoError(t, storage.Put(b("c"), b("1")))
	snapshot3 := utils.Unwrap(storage.NewTxn())

	assert.NoError(t, storage.ForceFlush())

	assert.Equal(t, "1", string(utils.Unwrap(snapshot1.Get(b("a")))))
	assert.Equal(t, "1", string(utils.Unwrap(snapshot1.Get(b("b")))))
	assert.Equal(t, []byte(nil), utils.Unwrap(snapshot1.Get(b("c"))))
	CheckLsmIterResultByKey1(t, utils.Unwrap(snapshot1.Scan(UnboundBytes(), UnboundBytes())), []StringKeyValuePair{
		{"a", "1"},
		{"b", "1"},
	})

	assert.Equal(t, "2", string(utils.Unwrap(snapshot2.Get(b("a")))))
	assert.Equal(t, "1", string(utils.Unwrap(snapshot2.Get(b("b")))))
	assert.Equal(t, []byte(nil), utils.Unwrap(snapshot2.Get(b("c"))))
	CheckLsmIterResultByKey1(t, utils.Unwrap(snapshot2.Scan(UnboundBytes(), UnboundBytes())), []StringKeyValuePair{
		{"a", "2"},
		{"b", "1"},
	})

	assert.Equal(t, "2", string(utils.Unwrap(snapshot3.Get(b("a")))))
	assert.Equal(t, []byte(nil), utils.Unwrap(snapshot3.Get(b("b"))))
	assert.Equal(t, "1", string(utils.Unwrap(snapshot3.Get(b("c")))))
	CheckLsmIterResultByKey1(t, utils.Unwrap(snapshot3.Scan(UnboundBytes(), UnboundBytes())), []StringKeyValuePair{
		{"a", "2"},
		{"c", "1"},
	})
	assert.NoError(t, storage.Put(b("a"), b("3")))
	assert.NoError(t, storage.Put(b("b"), b("3")))
	snapshot4 := utils.Unwrap(storage.NewTxn())
	assert.NoError(t, storage.Put(b("a"), b("4")))
	snapshot5 := utils.Unwrap(storage.NewTxn())
	assert.NoError(t, storage.Delete(b("b")))
	assert.NoError(t, storage.Put(b("c"), b("5")))
	snapshot6 := utils.Unwrap(storage.NewTxn())

	assert.NoError(t, storage.ForceFlush())

	assert.Equal(t, "1", string(utils.Unwrap(snapshot1.Get(b("a")))))
	assert.Equal(t, "1", string(utils.Unwrap(snapshot1.Get(b("b")))))
	assert.Equal(t, []byte(nil), utils.Unwrap(snapshot1.Get(b("c"))))
	CheckLsmIterResultByKey1(t, utils.Unwrap(snapshot1.Scan(UnboundBytes(), UnboundBytes())), []StringKeyValuePair{
		{"a", "1"},
		{"b", "1"},
	})

	assert.Equal(t, "2", string(utils.Unwrap(snapshot2.Get(b("a")))))
	assert.Equal(t, "1", string(utils.Unwrap(snapshot2.Get(b("b")))))
	assert.Equal(t, []byte(nil), utils.Unwrap(snapshot2.Get(b("c"))))
	CheckLsmIterResultByKey1(t, utils.Unwrap(snapshot2.Scan(UnboundBytes(), UnboundBytes())), []StringKeyValuePair{
		{"a", "2"},
		{"b", "1"},
	})

	assert.Equal(t, "2", string(utils.Unwrap(snapshot3.Get(b("a")))))
	assert.Equal(t, []byte(nil), utils.Unwrap(snapshot3.Get(b("b"))))
	assert.Equal(t, "1", string(utils.Unwrap(snapshot3.Get(b("c")))))
	CheckLsmIterResultByKey1(t, utils.Unwrap(snapshot3.Scan(UnboundBytes(), UnboundBytes())), []StringKeyValuePair{
		{"a", "2"},
		{"c", "1"},
	})

	assert.Equal(t, "3", string(utils.Unwrap(snapshot4.Get(b("a")))))
	assert.Equal(t, "3", string(utils.Unwrap(snapshot4.Get(b("b")))))
	assert.Equal(t, "1", string(utils.Unwrap(snapshot4.Get(b("c")))))
	CheckLsmIterResultByKey1(t, utils.Unwrap(snapshot4.Scan(UnboundBytes(), UnboundBytes())), []StringKeyValuePair{
		{"a", "3"},
		{"b", "3"},
		{"c", "1"},
	})

	assert.Equal(t, "4", string(utils.Unwrap(snapshot5.Get(b("a")))))
	assert.Equal(t, "3", string(utils.Unwrap(snapshot5.Get(b("b")))))
	assert.Equal(t, "1", string(utils.Unwrap(snapshot5.Get(b("c")))))
	CheckLsmIterResultByKey1(t, utils.Unwrap(snapshot5.Scan(UnboundBytes(), UnboundBytes())), []StringKeyValuePair{
		{"a", "4"},
		{"b", "3"},
		{"c", "1"},
	})

	assert.Equal(t, "4", string(utils.Unwrap(snapshot6.Get(b("a")))))
	assert.Equal(t, []byte(nil), utils.Unwrap(snapshot6.Get(b("b"))))
	assert.Equal(t, "5", string(utils.Unwrap(snapshot6.Get(b("c")))))
	CheckLsmIterResultByKey1(t, utils.Unwrap(snapshot6.Scan(UnboundBytes(), UnboundBytes())), []StringKeyValuePair{
		{"a", "4"},
		{"c", "5"},
	})

	CheckLsmIterResultByKey1(t, utils.Unwrap(snapshot6.Scan(IncludeBytes(b("a")), IncludeBytes(b("a")))), []StringKeyValuePair{
		{"a", "4"},
	})

	iter := utils.Unwrap(snapshot6.Scan(ExcludeBytes(b("a")), ExcludeBytes(b("c"))))
	//PrintIter(iter)
	CheckLsmIterResultByKey1(t, iter, []StringKeyValuePair{})

	snapshot1.RemoveReader()
	snapshot2.RemoveReader()
	snapshot3.RemoveReader()
	snapshot4.RemoveReader()
	snapshot5.RemoveReader()
	snapshot6.RemoveReader()
	assert.NoError(t, storage.Shutdown())
}

func TestTask2LsmIteratorMvcc1(t *testing.T) {
	dir := t.TempDir()
	options := DefaultForWeek2Test(&CompactionOptions{
		CompactionType: NoCompaction,
	})
	options.EnableWal = true
	storage := utils.Unwrap(Open(dir, options))
	assert.NoError(t, storage.Put(b("a"), b("1")))
	assert.NoError(t, storage.Put(b("b"), b("1")))
	assert.NoError(t, storage.Put(b("a"), b("2")))
	assert.NoError(t, storage.Delete(b("b")))
	assert.NoError(t, storage.Put(b("c"), b("1")))
	assert.NoError(t, storage.ForceFlush())
	assert.NoError(t, storage.Put(b("a"), b("3")))
	assert.NoError(t, storage.Put(b("b"), b("3")))
	assert.NoError(t, storage.Put(b("a"), b("4")))
	assert.NoError(t, storage.Delete(b("b")))
	assert.NoError(t, storage.Put(b("c"), b("5")))
	snapshot6 := utils.Unwrap(storage.NewTxn())
	assert.NoError(t, storage.ForceFlush())

	//assert.Equal(t, "4", string(utils.Unwrap(snapshot6.Get(b("a")))))
	//assert.Equal(t, []byte(nil), utils.Unwrap(snapshot6.Get(b("b"))))
	//assert.Equal(t, "5", string(utils.Unwrap(snapshot6.Get(b("c")))))
	//CheckLsmIterResultByKey1(t, utils.Unwrap(snapshot6.Scan(UnboundBytes(), UnboundBytes())), []StringKeyValuePair{
	//	{"a", "4"},
	//	{"c", "5"},
	//})
	//
	//CheckLsmIterResultByKey1(t, utils.Unwrap(snapshot6.Scan(IncludeBytes(b("a")), IncludeBytes(b("a")))), []StringKeyValuePair{
	//	{"a", "4"},
	//})

	iter := utils.Unwrap(snapshot6.Scan(ExcludeBytes(b("a")), ExcludeBytes(b("c"))))
	// PrintIter(iter)
	CheckLsmIterResultByKey1(t, iter, []StringKeyValuePair{})

	assert.NoError(t, storage.Shutdown())
}

func TestMemtable1(t *testing.T) {
	table := CreateMemTable(0)
	table.Put(KeyFromBytesWithTs(b("a"), 1), b("1"))
	table.Put(KeyFromBytesWithTs(b("a"), 2), b("2"))
	table.Put(KeyFromBytesWithTs(b("a"), 3), b("3"))
	table.Put(KeyFromBytesWithTs(b("b"), 1), b("1"))
	table.Put(KeyFromBytesWithTs(b("b"), 2), b("2"))
	table.Put(KeyFromBytesWithTs(b("b"), 3), b("3"))
	table.Put(KeyFromBytesWithTs(b("c"), 1), b("1"))
	table.Put(KeyFromBytesWithTs(b("c"), 2), b("2"))
	table.Put(KeyFromBytesWithTs(b("c"), 3), b("3"))
	skipMap := table.skipMap
	skipMap.Find(KeyFromBytesWithTs(b("a"), TsRangeBegin))
}

func TestTask3SstTs(t *testing.T) {
	builder := NewSsTableBuilder(16)
	builder.Add(KeyFromBytesWithTs(b("11"), 1), b("11"))
	builder.Add(KeyFromBytesWithTs(b("22"), 2), b("22"))
	builder.Add(KeyFromBytesWithTs(b("33"), 3), b("11"))
	builder.Add(KeyFromBytesWithTs(b("44"), 4), b("22"))
	builder.Add(KeyFromBytesWithTs(b("55"), 5), b("11"))
	builder.Add(KeyFromBytesWithTs(b("66"), 6), b("22"))
	dir := t.TempDir()
	sst, err := builder.buildForTest(path.Join(dir, "1.sst"))
	assert.NoError(t, err)
	assert.Equal(t, uint64(6), sst.MaxTs())
	assert.NoError(t, sst.CloseSstFile())
}
