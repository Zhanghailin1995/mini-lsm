package mini_lsm

import (
	"bytes"
	"github.com/Zhanghailin1995/mini-lsm/utils"
)

type KeyType struct {
	Val []byte
}

type KeyValuePair [][]byte

type StringKeyValuePair []string

type Comparable interface {
	Compare(Comparable) int
}

func (k KeyType) Compare(other Comparable) int {
	return bytes.Compare(k.Val, other.(KeyType).Val)
}

func (k KeyType) Clone() KeyType {
	return KeyType{
		Val: utils.Copy(k.Val),
	}
}

func Key(v []byte) KeyType {
	return KeyType{
		Val: utils.Copy(v),
	}
}

func StringKey(v string) KeyType {
	return KeyType{
		Val: []byte(v),
	}
}

type StorageIterator interface {

	// Value Get the current value
	Value() []byte

	// Key Get the current key
	Key() KeyType

	// IsValid Check if the current iterator is valid
	IsValid() bool

	// Next Move the iterator to the next element
	Next() error

	// NumActiveIterators Number of underlying active iterators for this iterator
	NumActiveIterators() int
}

func PrintIter(iter StorageIterator) {
	println()
	println("================================")
	for iter.IsValid() {
		println(string(iter.Key().Val), string(iter.Value()))
		iter.Next()
	}
	println("================================")
	println()
}
