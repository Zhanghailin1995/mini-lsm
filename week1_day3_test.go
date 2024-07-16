package mini_lsm

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBlockBuildSingleKey(t *testing.T) {
	builder := NewBlockBuilder(16)
	assert.True(t, builder.Add(StringKey("233"), []byte("233333")))
	builder.Build()
}

func TestBlockBuildFull(t *testing.T) {
	builder := NewBlockBuilder(16)
	assert.True(t, builder.Add(StringKey("11"), []byte("11")))
	assert.False(t, builder.Add(StringKey("22"), []byte("22")))
	builder.Build()
}

func TestBlockBuildLarge1(t *testing.T) {
	builder := NewBlockBuilder(16)
	buf := make([]byte, 100)
	for i := 0; i < 100; i++ {
		buf[i] = []byte("1")[0]
	}
	assert.True(t, builder.Add(StringKey("11"), buf))

	builder.Build()
}

func TestBlockBuildLarge2(t *testing.T) {
	builder := NewBlockBuilder(16)
	assert.True(t, builder.Add(StringKey("11"), []byte("1")))
	buf := make([]byte, 100)
	for i := 0; i < 100; i++ {
		buf[i] = []byte("1")[0]
	}
	assert.False(t, builder.Add(StringKey("22"), buf))
	builder.Build()
	//sprintf := fmt.Sprintf("value_%010d", 1)
	//println(sprintf)
	//println(utils.RoundUpToPowerOfTwo(10000))
}

func keyOf(i int) KeyType {
	return StringKey(fmt.Sprintf("key_%03d", i*5))
}

func valueOf(i int) []byte {
	return []byte(fmt.Sprintf("value_%010d", i))
}

func numOfKeys() int {
	return 100
}

func generateBlock(t *testing.T) *Block {
	builder := NewBlockBuilder(10000)
	for i := 0; i < numOfKeys(); i++ {
		assert.True(t, builder.Add(keyOf(i), valueOf(i)))
	}
	return builder.Build()
}

func TestGenerateBlock(t *testing.T) {
	generateBlock(t)
}

func TestBlockEncode(t *testing.T) {
	block := generateBlock(t)
	encoded := block.Encode()
	decoded := DecodeBlock(encoded)
	assert.Equal(t, block, decoded)
}

func TestBlockDecode(t *testing.T) {
	block := generateBlock(t)
	encoded := block.Encode()
	decoded := DecodeBlock(encoded)
	assert.Equal(t, block, decoded)
}

func TestBlockIterator(t *testing.T) {
	block := generateBlock(t)
	iter := block.CreateIteratorAndSeekToFirst()
	for i := 0; i < 5; i++ {
		for j := 0; j < numOfKeys(); j++ {
			k := iter.Key()
			v := iter.Value()
			assert.Equal(t, k.Val, keyOf(j).Val)
			assert.Equal(t, v, valueOf(j))
			iter.Next()
		}
		iter.SeekToFirst()
	}
}

func TestBlockSeekKey(t *testing.T) {
	block := generateBlock(t)
	iter := block.CreateIteratorAndSeekToKey(keyOf(0))
	for i := 1; i <= 5; i++ {
		for j := 0; j < numOfKeys(); j++ {
			k := iter.Key()
			v := iter.Value()
			if !assert.Equal(t, k.Val, keyOf(j).Val) {
				//println("wrong key")
				//println(i, j, string(k.Val), string(keyOf(j).Val))
			}
			assert.Equal(t, v, valueOf(j))
			//println("seek to key: ", fmt.Sprintf("key_%03d", j*5+i))
			iter.SeekToKey(StringKey(fmt.Sprintf("key_%03d", j*5+i)))
		}
		iter.SeekToKey(StringKey("k"))
	}
}

//func TestSlice(t *testing.T) {
//	s := []int{1, 2, 3, 4, 5}
//	s1 := s[1:3]
//	s1 = append(s1, 6)
//	t.Log(s)
//}
