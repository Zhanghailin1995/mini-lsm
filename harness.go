package mini_lsm

import (
	"bytes"
	"errors"
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"testing"
)

var (
	FakeError = errors.New("fake error")
)

type MockIterator struct {
	Data []struct {
		K KeyType
		V []byte
	}
	ErrorWhen int32
	Index     uint32
}

func (m *MockIterator) Clone() *MockIterator {
	d := make([]struct {
		K KeyType
		V []byte
	}, len(m.Data))
	for i, v := range m.Data {
		d[i] = struct {
			K KeyType
			V []byte
		}{K: Key(utils.Copy(v.K.Val)), V: utils.Copy(v.V)}
	}
	return &MockIterator{
		Data:      d,
		ErrorWhen: m.ErrorWhen,
		Index:     m.Index,
	}
}

func NewMockIterator(data []struct {
	K KeyType
	V []byte
}) *MockIterator {
	return &MockIterator{Data: data, ErrorWhen: -1, Index: 0}
}

func NewMockIteratorWithKVPair(data []KeyValuePair) *MockIterator {
	d := make([]struct {
		K KeyType
		V []byte
	}, len(data))
	for i, v := range data {
		d[i] = struct {
			K KeyType
			V []byte
		}{
			K: Key(v[0]),
			V: utils.Copy(v[1]),
		}
	}
	return NewMockIterator(d)
}

func NewMockIteratorWithStringKVPair(data []StringKeyValuePair) *MockIterator {
	d := make([]struct {
		K KeyType
		V []byte
	}, len(data))
	for i, v := range data {
		d[i] = struct {
			K KeyType
			V []byte
		}{
			K: StringKey(v[0]),
			V: utils.Copy([]byte(v[1])),
		}
	}
	return NewMockIterator(d)
}

func NewMockIteratorWithError(data []struct {
	K KeyType
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

func (m *MockIterator) Key() KeyType {
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
		if bytes.Compare([]byte(expected[i][0]), iter.Key().Val) != 0 {
			t.Errorf("expected key %s, got %s", expected[i][0], string(iter.Key().Val))
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
	K KeyType
	V []byte
}) {
	for i := range expected {
		if !iter.IsValid() {
			t.Errorf("expected valid iterator, got invalid")
		}
		if bytes.Compare(expected[i].K.Val, iter.Key().Val) != 0 {
			t.Errorf("expected key %s, got %s", string(expected[i].K.Val), string(iter.Key().Val))
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
	K KeyType
	V []byte
}) {
	for i := range expected {
		if !iter.IsValid() {
			t.Errorf("expected valid iterator, got invalid")
		}
		if bytes.Compare(expected[i].K.Val, iter.Key().Val) != 0 {
			t.Errorf("expected key %s, got %s", string(expected[i].K.Val), string(iter.Key().Val))
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
		if iter.Key().Compare(StringKey(expected[i][0])) != 0 {
			t.Errorf("expected key %s, got %s", expected[i][0], string(iter.Key().Val))
		}
		if expected[i][1] != string(iter.Value()) {
			t.Errorf("expected key %s : value %s, got %s", expected[i][0], expected[i][1], string(iter.Value()))
		}
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
