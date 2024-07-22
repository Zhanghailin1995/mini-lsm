package mini_lsm

import (
	"bytes"
	"github.com/Zhanghailin1995/mini-lsm/utils"
)

const TSEnabled = false

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
		Val: v,
	}
}

func StringKey(v string) KeyType {
	return KeyType{
		Val: []byte(v),
	}
}
