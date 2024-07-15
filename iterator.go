package mini_lsm

import "github.com/Zhanghailin1995/mini-lsm/utils"

type KeyType struct {
	Val []byte
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
