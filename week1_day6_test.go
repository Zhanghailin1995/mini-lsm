package mini_lsm

import (
	"fmt"
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

func TestTask1StorageScan(t *testing.T) {
	dir := t.TempDir()
	storage := utils.Unwrap(OpenLsmStorageInner(dir, DefaultForWeek1Test()))
	assert.NoError(t, storage.Put(StringKey("0"), []byte("2333333")))
	assert.NoError(t, storage.Put(StringKey("00"), []byte("2333333")))
	assert.NoError(t, storage.Put(StringKey("4"), []byte("23")))
	Sync(storage)
	assert.NoError(t, storage.Delete(StringKey("4")))
	Sync(storage)

	assert.NoError(t, storage.Put(StringKey("1"), []byte("233")))
	assert.NoError(t, storage.Put(StringKey("2"), []byte("2333")))
	storage.stateLock.Lock()
	assert.NoError(t, storage.ForceFreezeMemTable())
	storage.stateLock.Unlock()

	assert.NoError(t, storage.Put(StringKey("00"), []byte("2333")))

	storage.stateLock.Lock()
	assert.NoError(t, storage.ForceFreezeMemTable())
	storage.stateLock.Unlock()

	assert.NoError(t, storage.Put(StringKey("3"), []byte("23333")))
	assert.NoError(t, storage.Delete(StringKey("1")))

	{
		storage.rwLock.RLock()
		assert.Equal(t, len(storage.state.l0SsTables), 2)
		assert.Equal(t, len(storage.state.immMemTable), 2)
		storage.rwLock.RUnlock()
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

	assert.NoError(t, storage.Close())

}

func TestTask1StorageGet(t *testing.T) {
	dir := t.TempDir()
	storage := utils.Unwrap(OpenLsmStorageInner(dir, DefaultForWeek1Test()))
	assert.NoError(t, storage.Put(StringKey("0"), []byte("2333333")))
	assert.NoError(t, storage.Put(StringKey("00"), []byte("2333333")))
	assert.NoError(t, storage.Put(StringKey("4"), []byte("23")))
	Sync(storage)
	assert.NoError(t, storage.Delete(StringKey("4")))
	Sync(storage)

	assert.NoError(t, storage.Put(StringKey("1"), []byte("233")))
	assert.NoError(t, storage.Put(StringKey("2"), []byte("2333")))
	storage.stateLock.Lock()
	assert.NoError(t, storage.ForceFreezeMemTable())
	storage.stateLock.Unlock()

	assert.NoError(t, storage.Put(StringKey("00"), []byte("2333")))

	storage.stateLock.Lock()
	assert.NoError(t, storage.ForceFreezeMemTable())
	storage.stateLock.Unlock()

	assert.NoError(t, storage.Put(StringKey("3"), []byte("23333")))
	assert.NoError(t, storage.Delete(StringKey("1")))

	{
		storage.rwLock.RLock()
		assert.Equal(t, len(storage.state.l0SsTables), 2)
		assert.Equal(t, len(storage.state.immMemTable), 2)
		storage.rwLock.RUnlock()
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

func TestTask2AutoFlush(t *testing.T) {
	dir := t.TempDir()
	storage := utils.Unwrap(Open(dir, DefaultForWeek1Day6Test()))

	val := utils.RepeatByte('1', 1024)

	for i := 0; i < 6000; i++ {
		assert.NoError(t, storage.Put([]byte(strconv.Itoa(i)), val))
	}

	time.Sleep(500 * time.Millisecond)

	storage.inner.rwLock.RLock()
	i := len(storage.inner.state.l0SsTables)
	storage.inner.rwLock.RUnlock()
	assert.True(t, i > 0)

	assert.NoError(t, storage.Shutdown())

}

func TestTask3SstFilter(t *testing.T) {
	dir := t.TempDir()
	storage := utils.Unwrap(OpenLsmStorageInner(dir, DefaultForWeek1Test()))

	for i := 1; i <= 10000; i++ {
		if i%1000 == 0 {
			Sync(storage)
		}
		assert.NoError(t, storage.Put(StringKey(fmt.Sprintf("%05d", i)), []byte("2333333")))
	}

	iter := utils.Unwrap(storage.Scan(Unbound(), Unbound()))
	assert.True(t, iter.NumActiveIterators() >= 10, "did you implement num_active_iterators? current active iterators = %d", iter.NumActiveIterators())

	maxNum := iter.NumActiveIterators()
	t.Log("maxNum:", maxNum)
	iter = utils.Unwrap(storage.Scan(Exclude(StringKey("10000")), Unbound()))
	assert.True(t, iter.NumActiveIterators() < maxNum)

	minNum := iter.NumActiveIterators()
	t.Log("minNum:", minNum)
	iter = utils.Unwrap(storage.Scan(Unbound(), Exclude(StringKey("00001"))))
	assert.Equal(t, iter.NumActiveIterators(), minNum)

	iter = utils.Unwrap(storage.Scan(Unbound(), Include(StringKey("00000"))))
	assert.Equal(t, iter.NumActiveIterators(), minNum)

	iter = utils.Unwrap(storage.Scan(Include(StringKey("10001")), Unbound()))
	assert.Equal(t, iter.NumActiveIterators(), minNum)

	iter = utils.Unwrap(storage.Scan(Include(StringKey("05000")), Exclude(StringKey("06000"))))
	assert.True(t, minNum <= iter.NumActiveIterators() && iter.NumActiveIterators() <= maxNum)

	assert.NoError(t, storage.Close())

}

//func TestSetBit(t *testing.T) {
//	b := make([]byte, 2)
//
//	SetBit(b, 10, true)
//	assert.Equal(t, b, []byte{0, 0b00000001 << 2})
//	SetBit(b, 11, true)
//	assert.Equal(t, b, []byte{0, 0b00000011 << 2})
//	SetBit(b, 10, false)
//	assert.Equal(t, b, []byte{0, 0b00000010 << 2})
//
//	x := utils.WrappingAddU32(200, math.MaxUint32)
//	println(x)
//}
