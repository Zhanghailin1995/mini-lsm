package mini_lsm

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"io/ioutil"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

var (
	FakeError = errors.New("fake error")
)

type MockIterator struct {
	Data []struct {
		K KeyBytes
		V []byte
	}
	ErrorWhen int32
	Index     uint32
}

func (m *MockIterator) Clone() *MockIterator {
	d := make([]struct {
		K KeyBytes
		V []byte
	}, len(m.Data))
	for i, v := range m.Data {
		d[i] = struct {
			K KeyBytes
			V []byte
		}{K: KeyOf(utils.Copy(v.K.Val)), V: utils.Copy(v.V)}
	}
	return &MockIterator{
		Data:      d,
		ErrorWhen: m.ErrorWhen,
		Index:     m.Index,
	}
}

func NewMockIterator(data []struct {
	K KeyBytes
	V []byte
}) *MockIterator {
	return &MockIterator{Data: data, ErrorWhen: -1, Index: 0}
}

func NewMockIteratorWithKVPair(data []KeyValuePair) *MockIterator {
	d := make([]struct {
		K KeyBytes
		V []byte
	}, len(data))
	for i, v := range data {
		d[i] = struct {
			K KeyBytes
			V []byte
		}{
			K: KeyOf(v[0]),
			V: utils.Copy(v[1]),
		}
	}
	return NewMockIterator(d)
}

func NewMockIteratorWithStringKVPair(data []StringKeyValuePair) *MockIterator {
	d := make([]struct {
		K KeyBytes
		V []byte
	}, len(data))
	for i, v := range data {
		d[i] = struct {
			K KeyBytes
			V []byte
		}{
			K: StringKey(v[0]),
			V: utils.Copy([]byte(v[1])),
		}
	}
	return NewMockIterator(d)
}

func NewMockIteratorWithError(data []struct {
	K KeyBytes
	V []byte
}, errorWhen int32) *MockIterator {
	return &MockIterator{Data: data, ErrorWhen: errorWhen, Index: 0}
}

func (m *MockIterator) Next() error {
	if m.Index < uint32(len(m.Data)) {
		m.Index += 1
	}
	if int32(m.Index) == m.ErrorWhen {
		return FakeError
	}
	return nil
}

func (m *MockIterator) Key() IteratorKey {
	if m.Index >= uint32(len(m.Data)) && m.ErrorWhen != -1 {
		panic("invalid access after next returns an error!")
	}
	return m.Data[m.Index].K
}

func (m *MockIterator) Value() []byte {
	if m.Index >= uint32(len(m.Data)) && m.ErrorWhen != -1 {
		panic("invalid access after next returns an error!")
	}
	return m.Data[m.Index].V
}

func (m *MockIterator) IsValid() bool {
	if m.Index >= uint32(m.ErrorWhen) && m.ErrorWhen != -1 {
		panic("invalid access after next returns an error!")
	}
	return m.Index < uint32(len(m.Data))
}

func (m *MockIterator) NumActiveIterators() int {
	return 1
}

func CheckIterResultByKey1(t *testing.T, iter StorageIterator, expected []StringKeyValuePair) {
	for i := range expected {
		if !iter.IsValid() {
			t.Errorf("expected valid iterator, got invalid")
		}
		if bytes.Compare([]byte(expected[i][0]), iter.Key().KeyRef()) != 0 {
			t.Errorf("expected key %s, got %s", expected[i][0], string(iter.Key().KeyRef()))
		}
		if expected[i][1] != string(iter.Value()) {
			t.Errorf("expected value %s, got %s", expected[i][1], string(iter.Value()))
		}
		if err := iter.Next(); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}
	if iter.IsValid() {
		t.Errorf("expected invalid iterator, got valid")
	}
}

func CheckIterResultByKey(t *testing.T, iter StorageIterator, expected []struct {
	K KeyBytes
	V []byte
}) {
	for i := range expected {
		if !iter.IsValid() {
			t.Errorf("expected valid iterator, got invalid")
		}
		if bytes.Compare(expected[i].K.Val, iter.Key().KeyRef()) != 0 {
			t.Errorf("expected key %s, got %s", string(expected[i].K.Val), string(iter.Key().KeyRef()))
		}
		if string(expected[i].V) != string(iter.Value()) {
			t.Errorf("expected value %s, got %s", string(expected[i].V), string(iter.Value()))
		}
		if err := iter.Next(); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}
	if iter.IsValid() {
		t.Errorf("expected invalid iterator, got valid")
	}
}

func CheckLsmIterResultByKey(t *testing.T, iter StorageIterator, expected []struct {
	K KeyBytes
	V []byte
}) {
	for i := range expected {
		if !iter.IsValid() {
			t.Errorf("expected valid iterator, got invalid")
		}
		if bytes.Compare(expected[i].K.Val, iter.Key().KeyRef()) != 0 {
			t.Errorf("expected key %s, got %s", string(expected[i].K.Val), string(iter.Key().KeyRef()))
		}
		if string(expected[i].V) != string(iter.Value()) {
			t.Errorf("expected value %s, got %s", string(expected[i].V), string(iter.Value()))
		}
		if err := iter.Next(); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}
	if iter.IsValid() {
		t.Errorf("expected invalid iterator, got valid")
	}
}

func CheckLsmIterResultByKey1(t *testing.T, iter StorageIterator, expected []StringKeyValuePair) {
	for i := range expected {
		if !iter.IsValid() {
			t.Errorf("expected valid iterator, got invalid")
		}
		if iter.Key().KeyRefCompare(StringKey(expected[i][0])) != 0 {
			t.Errorf("expected key %s, got %s", expected[i][0], string(iter.Key().KeyRef()))
		}
		if expected[i][1] != string(iter.Value()) {
			t.Errorf("expected key %s : value %s, got %s", expected[i][0], expected[i][1], string(iter.Value()))
		}
		//println(string(iter.Key().KeyRef()), string(iter.Value()))
		if err := iter.Next(); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}
	if iter.IsValid() {
		t.Errorf("expected invalid iterator, got valid")
	}
}

func GenerateSst(id uint32, path string, data []StringKeyValuePair, blockCache *BlockCache) *SsTable {
	builder := NewSsTableBuilder(128)
	for _, kv := range data {
		builder.Add(StringKey(kv[0]), []byte(kv[1]))
	}
	return utils.Unwrap(builder.Build(id, blockCache, path))
}

func Sync(storage *LsmStorageInner) {
	storage.stateLock.Lock()
	utils.UnwrapError(storage.ForceFreezeMemTable())
	storage.stateLock.Unlock()
	utils.UnwrapError(storage.ForceFlushNextImmMemtable())
}

func ConstructMergeIteratorOverStorage(state *LsmStorageState) *MergeIterator {
	iters := make([]StorageIterator, 0)
	for _, id := range state.l0SsTables {
		iters = append(iters, utils.Unwrap(CreateSsTableIteratorAndSeekToFirst(state.sstables[id])))
	}
	for _, level := range state.levels {
		for _, id := range level.ssTables {
			iters = append(iters, utils.Unwrap(CreateSsTableIteratorAndSeekToFirst(state.sstables[id])))
		}
	}
	return CreateMergeIterator(iters)
}

func CompactionBench(lsm *MiniLsm, t *testing.T) {
	keyMap := make(map[uint32]uint32)
	genKey := func(i uint32) string {
		return fmt.Sprintf("%10d", i)
	}
	genValue := func(i uint32) string {
		return fmt.Sprintf("%110d", i)
	}
	maxKey := uint32(0)
	overlaps := uint32(20000)
	if TsEnabled {
		overlaps = uint32(10000)
	}
	for iter := uint32(0); iter < 10; iter++ {
		rangeBegin := iter * 5000
		for i := rangeBegin; i < rangeBegin+overlaps; i++ {
			key := genKey(i)
			version, ok := keyMap[i]
			if !ok {
				version = 0
			}
			version += 1
			value := genValue(version)
			keyMap[i] = version
			utils.UnwrapError(lsm.Put([]byte(key), []byte(value)))
			if i > maxKey {
				maxKey = i
			}
		}
	}
	time.Sleep(time.Second)
	for ReadLsmStorageState(lsm.inner, func(state *LsmStorageState) bool {
		return len(state.immMemTable) > 0
	}) {
		utils.UnwrapError(lsm.inner.ForceFlushNextImmMemtable())
	}

	prevSnapshot := ReadLsmStorageState(lsm.inner, func(state *LsmStorageState) *LsmStorageState {
		return state
	})
	toCont := func() bool {
		time.Sleep(time.Second)
		snapshot := ReadLsmStorageState(lsm.inner, func(state *LsmStorageState) *LsmStorageState {
			return state
		})
		toCont := !slices.EqualFunc(prevSnapshot.levels, snapshot.levels, func(a, b *LevelSsTables) bool {
			return a.level == b.level && slices.Equal(a.ssTables, b.ssTables)
		}) || !slices.Equal(prevSnapshot.l0SsTables, snapshot.l0SsTables)
		prevSnapshot = snapshot
		return toCont
	}
	for toCont() {
		println("waiting for compaction to converge")
	}

	expectedKeyValuePairs := make([]StringKeyValuePair, 0)
	for i := uint32(0); i <= maxKey+40000; i++ {
		key := genKey(i)
		value := utils.Unwrap(lsm.Get([]byte(key)))
		u, ok := keyMap[i]
		if ok {
			expectedValue := genValue(u)
			if string(value) != expectedValue {
				panic(fmt.Sprintf("expected value %s, got %s", expectedValue, string(value)))
			}
			expectedKeyValuePairs = append(expectedKeyValuePairs, StringKeyValuePair{key, expectedValue})
		} else {
			utils.Assert(value == nil || len(value) == 0, "expected nil value")
		}

	}
	CheckIterResultByKey1(t, utils.Unwrap(lsm.Scan(UnboundBytes(), UnboundBytes())), expectedKeyValuePairs)
	println("This test case does not guarantee your compaction algorithm produces a LSM state as expected. It only does minimal checks on the size of the levels. Please use the compaction simulator to check if the compaction is correctly going on.")
}

func dumpFilesInDir(path string) {
	fmt.Println("--- DIR DUMP ---")

	files, err := ioutil.ReadDir(path)
	if err != nil {
		fmt.Println("Error reading directory:", err)
		return
	}

	for _, f := range files {
		fmt.Printf("%s, size=%.3fKB\n", filepath.Join(path, f.Name()), float64(f.Size())/1024.0)
	}
}

func b(s string) []byte {
	return []byte(s)
}

func GenerateSstWithTs(id uint32, p string, data []*Tuple[*Tuple[[]byte, uint64], []byte], blockCache *BlockCache) *SsTable {
	builder := NewSsTableBuilder(128)
	for _, kv := range data {
		builder.Add(KeyFromBytesWithTs(kv.First.First, kv.First.Second), kv.Second)
	}
	return utils.Unwrap(builder.Build(id, blockCache, p))
}

func CheckIterResultByKeyAndTs(t *testing.T, iter StorageIterator, expected []*Tuple[*Tuple[[]byte, uint64], []byte]) {
	for i := range expected {
		if !iter.IsValid() {
			t.Errorf("expected valid iterator, got invalid")
		}
		if bytes.Compare(expected[i].First.First, iter.Key().KeyRef()) != 0 {
			t.Errorf("expected key %s, got %s", string(expected[i].First.First), string(iter.Key().KeyRef()))
		}
		if expected[i].First.Second != iter.Key().(KeyBytes).Ts {
			t.Errorf("expected ts %d, got %d", expected[i].First.Second, iter.Key().(KeyBytes).Ts)
		}
		if string(expected[i].Second) != string(iter.Value()) {
			t.Errorf("expected value %s, got %s", string(expected[i].Second), string(iter.Value()))
		}
		if err := iter.Next(); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}
	if iter.IsValid() {
		t.Errorf("expected invalid iterator, got valid")
	}
}
