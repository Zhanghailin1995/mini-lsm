package mini_lsm

import (
	"fmt"
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"github.com/dgryski/go-farm"
	"github.com/stretchr/testify/assert"
	"path"
	"testing"
)

func KeyOfW1D7(idx uint32) []byte {
	return []byte(fmt.Sprintf("key_%010d", idx*5))
}

func ValueOfW1D7(idx uint32) []byte {
	return []byte(fmt.Sprintf("value_%010d", idx))
}

func NumOfKeysW1D7() uint32 {
	return 100
}

func TestTask1BloomFilter(t *testing.T) {
	keyHashes := make([]uint32, 0)
	for i := uint32(0); i < NumOfKeysW1D7(); i++ {
		key := KeyOfW1D7(i)
		keyHashes = append(keyHashes, farm.Fingerprint32(key))
	}
	bitsPerKey := BloomBitsPerKey(uint32(len(keyHashes)), 0.01)
	fmt.Printf("bits per key: %d\n", bitsPerKey)
	bloom := BuildFromKeyHashes(keyHashes, bitsPerKey)
	fmt.Printf("bloom filter size: %d, k=%d\n", len(bloom.filter), bloom.k)
	assert.True(t, bloom.k < 30)
	for i := uint32(0); i < NumOfKeysW1D7(); i++ {
		key := KeyOfW1D7(i)
		assert.True(t, bloom.MayContain(farm.Fingerprint32(key)))
	}
	x := 0
	cnt := 0
	for i := NumOfKeysW1D7(); i < NumOfKeysW1D7()*10; i++ {
		key := KeyOfW1D7(i)
		if bloom.MayContain(farm.Fingerprint32(key)) {
			x++
		}
		cnt++
	}

	assert.NotEqualf(t, x, cnt, "bloom filter not taking effect?")
	assert.NotEqualf(t, x, 0, "bloom filter not taking effect?")
}

func TestTask2SstDecode(t *testing.T) {
	builder := NewSsTableBuilder(128)
	for i := uint32(0); i < NumOfKeysW1D7(); i++ {
		builder.Add(KeyOf(KeyOfW1D7(i)), ValueOfW1D7(i))
	}
	dir := t.TempDir()
	p := path.Join(dir, "1.sst")
	sst := utils.Unwrap(builder.buildForTest(p))
	sst2 := utils.Unwrap(OpenSsTable(0, nil, utils.Unwrap(OpenFileObject(p))))
	bloom1 := sst.bloom
	bloom2 := sst2.bloom
	assert.Equal(t, bloom1.filter, bloom2.filter)
	assert.Equal(t, bloom1.k, bloom2.k)
	assert.NoError(t, sst.CloseSstFile())
	assert.NoError(t, sst2.CloseSstFile())
}

func TestTask3BlockKeyCompression(t *testing.T) {
	builder := NewSsTableBuilder(128)
	for i := uint32(0); i < NumOfKeysW1D7(); i++ {
		builder.Add(KeyOf(KeyOfW1D7(i)), ValueOfW1D7(i))
	}
	dir := t.TempDir()
	p := path.Join(dir, "1.sst")
	sst := utils.Unwrap(builder.buildForTest(p))
	//if TsEnabled {
	//	assert.Truef(t, len(sst.blockMeta) <= 34, "you have %d blocks, expect 34", len(sst.blockMeta))
	//} else {
	//	assert.Truef(t, len(sst.blockMeta) <= 25, "you have %d blocks, expect 25", len(sst.blockMeta))
	//}
	assert.Truef(t, len(sst.blockMeta) <= 34, "you have %d blocks, expect 34", len(sst.blockMeta))
	assert.NoError(t, sst.CloseSstFile())
}
