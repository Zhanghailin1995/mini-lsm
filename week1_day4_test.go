package mini_lsm

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"path"
	"slices"
	"testing"
)

func TestSstBuildSingleKey(t *testing.T) {
	builder := NewSsTableBuilder(16)
	builder.Add(StringKey("233"), []byte("233333"))
	dir := t.TempDir()
	println(dir)
	sst, err := builder.buildForTest(path.Join(dir, "1.sst"))
	assert.NoError(t, err)
	assert.NoError(t, sst.CloseSstFile())
}

func TestSstBuildTwoBlocks(t *testing.T) {
	builder := NewSsTableBuilder(16)
	builder.Add(StringKey("11"), []byte("11"))
	builder.Add(StringKey("22"), []byte("22"))
	builder.Add(StringKey("33"), []byte("11"))
	builder.Add(StringKey("44"), []byte("22"))
	builder.Add(StringKey("55"), []byte("11"))
	builder.Add(StringKey("66"), []byte("22"))
	assert.True(t, len(builder.meta) >= 2)
	dir := t.TempDir()
	println(dir)
	sst, err := builder.buildForTest(path.Join(dir, "1.sst"))
	assert.NoError(t, err)
	assert.NoError(t, sst.CloseSstFile())
}

func generateSstForW1D4(t *testing.T) (string, *SsTable) {
	builder := NewSsTableBuilder(128)
	for i := 0; i < numOfKeys(); i++ {
		builder.Add(keyOf(i), valueOf(i))
	}
	dir := t.TempDir()
	println(dir)
	sst, err := builder.buildForTest(path.Join(dir, "1.sst"))
	assert.NoError(t, err)
	return dir, sst
}

func TestSstBuildAll(t *testing.T) {
	_, sst := generateSstForW1D4(t)
	assert.Equal(t, sst.FirstKey().KeyRef(), keyOf(0).Val)
	assert.Equal(t, sst.LastKey().KeyRef(), keyOf(numOfKeys()-1).Val)
	assert.NoError(t, sst.CloseSstFile())
}

func TestSstDecode(t *testing.T) {
	_, sst := generateSstForW1D4(t)
	meta := sst.blockMeta
	newSst, err := OpenSsTableForTest(sst.file)
	assert.NoError(t, err)
	//assert.Equal(t, newSst.blockMeta, meta)
	for i, block := range meta {
		newMeta := newSst.blockMeta[i]
		assert.Equal(t, block.offset, newMeta.offset)
		assert.True(t, slices.Equal(block.firstKey.Val, newMeta.firstKey.Val))
	}
	assert.Equal(t, newSst.FirstKey().KeyRef(), keyOf(0).Val)
	assert.Equal(t, newSst.LastKey().KeyRef(), keyOf(numOfKeys()-1).Val)
	assert.NoError(t, newSst.CloseSstFile())
}

func TestSstIterator(t *testing.T) {
	_, sst := generateSstForW1D4(t)
	iter, err := CreateSsTableIteratorAndSeekToFirst(sst)
	assert.NoError(t, err)
	for x := 0; x < 5; x++ {
		for i := 0; i < numOfKeys(); i++ {
			k, v := iter.Key(), iter.Value()
			assert.Equal(t, k.KeyRef(), keyOf(i).Val)
			assert.Equal(t, v, valueOf(i))
			assert.NoError(t, iter.Next())
		}
		assert.NoError(t, iter.SeekToFirst())
	}
	assert.NoError(t, sst.CloseSstFile())
}

func TestSstSeekKey(t *testing.T) {
	_, sst := generateSstForW1D4(t)
	iter, err := CreateSsTableIteratorAndSeekToKey(sst, keyOf(0))
	assert.NoError(t, err)
	for offset := 1; offset <= 5; offset++ {
		for i := 0; i < numOfKeys(); i++ {
			k, v := iter.Key(), iter.Value()
			assert.Equal(t, keyOf(i).Val, k.KeyRef())
			assert.Equal(t, valueOf(i), v)
			assert.NoError(t, iter.SeekToKey(StringKey(fmt.Sprintf("key_%03d", i*5+offset))))
		}
		assert.NoError(t, iter.SeekToKey(StringKey("k")))
	}
	assert.NoError(t, sst.CloseSstFile())
}
