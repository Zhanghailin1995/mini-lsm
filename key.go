package mini_lsm

import (
	"bytes"
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"math"
)

const TsEnabled = false

const (
	TsDefault    uint64 = 0
	TsMax        uint64 = math.MaxUint64
	TsMin        uint64 = 0
	TsRangeBegin uint64 = math.MaxUint64
	TsRangeEnd   uint64 = 0
)

type Tuple[T any, U any] struct {
	First  T
	Second U
}

type IteratorKey interface {
	Compare(IteratorKey) int
	KeyRefCompare(IteratorKey) int
	KeyRef() []byte
}

type KeyBytes struct {
	Val []byte
	Ts  uint64
}

type KeySlice []byte

func KeySliceOf(v []byte) KeySlice {
	return v
}

func (k KeyBytes) Compare(other IteratorKey) int {
	x := bytes.Compare(k.Val, other.(KeyBytes).Val)
	if x == 0 {
		x1 := int(k.Ts - other.(KeyBytes).Ts)
		if x1 > 0 {
			return 1
		} else if x1 < 0 {
			return -1
		} else {
			return 0
		}
	}
	return x
}

func (k KeyBytes) KeyRefCompare(other IteratorKey) int {
	return bytes.Compare(k.Val, other.KeyRef())
}

func (k KeyBytes) KeyRef() []byte {
	return k.Val
}

func (k KeySlice) Compare(other IteratorKey) int {
	return bytes.Compare(k, other.(KeySlice))
}

func (k KeySlice) KeyRefCompare(other IteratorKey) int {
	return bytes.Compare(k, other.KeyRef())
}

func (k KeySlice) KeyRef() []byte {
	return k
}

func KeyFromBytesWithTs(key []byte, ts uint64) KeyBytes {
	return KeyBytes{
		Val: key,
		Ts:  ts,
	}
}

type KeyValuePair [][]byte

type StringKeyValuePair []string

type Comparable interface {
	Compare(Comparable) int
}

func (k KeyBytes) RawLen() int {
	return len(k.Val) + 8 // + size of u64
}

func (k KeyBytes) KeyLen() int {
	return len(k.Val)
}

func (k KeyBytes) Clone() KeyBytes {
	return KeyBytes{
		Val: utils.Copy(k.Val),
		Ts:  k.Ts,
	}
}

func KeyOf(v []byte) KeyBytes {
	return KeyBytes{
		Val: v,
		Ts:  TsDefault,
	}
}

func StringKey(v string) KeyBytes {
	return KeyBytes{
		Val: []byte(v),
	}
}

type BoundType uint8

const (
	Unbounded BoundType = iota
	Included
	Excluded
)

type KeyBound struct {
	Val  KeyBytes
	Type BoundType
}

type BytesBound struct {
	Val  []byte
	Type BoundType
}

func BytesBounded(val []byte, boundType BoundType) BytesBound {
	return BytesBound{Val: val, Type: boundType}
}

func MapBound(bound BytesBound) KeyBound {
	return KeyBound{Val: KeyOf(bound.Val), Type: bound.Type}
}

func IncludeBytes(val []byte) BytesBound {
	return BytesBounded(val, Included)
}

func ExcludeBytes(val []byte) BytesBound {
	return BytesBounded(val, Excluded)
}

func UnboundBytes() BytesBound {
	return BytesBounded(nil, Unbounded)
}

func Bounded(val KeyBytes, boundType BoundType) KeyBound {
	return KeyBound{Val: val, Type: boundType}
}

func Include(val KeyBytes) KeyBound {
	return Bounded(val, Included)
}

func Exclude(val KeyBytes) KeyBound {
	return Bounded(val, Excluded)
}

func Unbound() KeyBound {
	return Bounded(KeyBytes{
		Val: nil,
	}, Unbounded)
}
