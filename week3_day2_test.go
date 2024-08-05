package mini_lsm

import (
	"fmt"
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestTask3CompactionIntegration(t *testing.T) {
	dir := t.TempDir()
	options := DefaultForWeek2Test(&CompactionOptions{
		CompactionType: NoCompaction,
	})
	options.EnableWal = true
	storage := utils.Unwrap(Open(dir, options))
	// TODO 事务提交
	_, _ = storage.NewTxn()
	for i := 0; i <= 20000; i++ {
		assert.NoError(t, storage.Put(b("0"), b(fmt.Sprintf("%02000d", i))))
	}
	time.Sleep(1 * time.Second)
	f := func() bool {
		storage.inner.rwLock.RLock()
		x := len(storage.inner.state.immMemTable) > 0
		storage.inner.rwLock.RUnlock()
		return x
	}

	for {
		if f() {
			assert.NoError(t, storage.inner.ForceFlushNextImmMemtable())
		} else {
			break
		}
	}
	assert.True(t, len(storage.inner.ReadState().l0SsTables) > 1)
	assert.NoError(t, storage.ForceFullCompaction())
	assert.NoError(t, storage.Sync())
	println(storage.inner.state.memTable.wal.cnt)
	println(storage.inner.state.memTable.wal.size)
	println(storage.inner.state.memTable.wal.file.Name())
	//println(atomic.LoadUint64(&x))
	//println(atomic.LoadInt32(&y))
	storage.DumpStructure()

	dumpFilesInDir(dir)
	assert.True(t, len(storage.inner.ReadState().l0SsTables) == 0)
	assert.True(t, len(storage.inner.ReadState().levels) == 1)
	assert.True(t, len(storage.inner.ReadState().levels[0].ssTables) == 1)
	for i := 0; i <= 100; i++ {
		assert.NoError(t, storage.Put(b("1"), b(fmt.Sprintf("%02000d", i))))
	}
	storage.inner.stateLock.Lock()
	assert.NoError(t, storage.inner.ForceFreezeMemTable())
	storage.inner.stateLock.Unlock()
	for {
		if f() {
			assert.NoError(t, storage.inner.ForceFlushNextImmMemtable())
		} else {
			break
		}
	}
	assert.NoError(t, storage.ForceFullCompaction())
	assert.NoError(t, storage.Sync())
	storage.DumpStructure()
	dumpFilesInDir(dir)
	assert.True(t, len(storage.inner.ReadState().l0SsTables) == 0)
	assert.True(t, len(storage.inner.ReadState().levels) == 1)
	assert.True(t, len(storage.inner.ReadState().levels[0].ssTables) == 2)
	assert.NoError(t, storage.Shutdown())
}
