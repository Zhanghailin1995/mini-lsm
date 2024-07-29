package mini_lsm

import (
	"fmt"
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"github.com/stretchr/testify/assert"
	"path"
	"testing"
)

func TestSstBuildMultiVersionSimple(t *testing.T) {
	builder := NewSsTableBuilder(16)
	builder.Add(KeyFromBytesWithTs(b("233"), 233), b("233333"))
	builder.Add(KeyFromBytesWithTs(b("233"), 0), b("2333333"))
	dir := t.TempDir()
	sst := utils.Unwrap(builder.buildForTest(path.Join(dir, "1.sst")))
	assert.NoError(t, sst.CloseSstFile())
}

func generateTestDataW3D1() []*Tuple[*Tuple[[]byte, uint64], []byte] {
	tuples := make([]*Tuple[*Tuple[[]byte, uint64], []byte], 0)
	for i := 0; i < 100; i++ {
		t := &Tuple[*Tuple[[]byte, uint64], []byte]{
			First: &Tuple[[]byte, uint64]{
				First:  b(fmt.Sprintf("key%05d", i/5)),
				Second: uint64(5 - i%5),
			},
			Second: b(fmt.Sprintf("value%05d", i)),
		}
		tuples = append(tuples, t)
	}
	return tuples
}

func TestSstBuildMultiVersionHard(t *testing.T) {
	dir := t.TempDir()
	tuples := generateTestDataW3D1()
	sst := GenerateSstWithTs(1, path.Join(dir, "1.sst"), tuples, nil)
	assert.NoError(t, sst.CloseSstFile())
	sst1, err := OpenSsTable(1, nil, utils.Unwrap(OpenFileObject(path.Join(dir, "1.sst"))))
	assert.NoError(t, err)
	iter, err := CreateSsTableIteratorAndSeekToFirst(sst1)
	assert.NoError(t, err)
	CheckIterResultByKeyAndTs(t, iter, tuples)
	assert.NoError(t, sst1.CloseSstFile())
}
