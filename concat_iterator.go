package mini_lsm

import (
	"github.com/Zhanghailin1995/mini-lsm/utils"
)

type SstConcatIterator struct {
	current    *SsTableIterator
	nextSstIdx uint32
	sstables   []*SsTable
}

func checkSstValid(sstables []*SsTable) {
	for _, sst := range sstables {
		utils.Assert(sst.FirstKey().Compare(sst.LastKey()) <= 0, "sstable is invalid")
	}
	if len(sstables) > 0 {
		for i := 0; i < len(sstables)-1; i++ {
			utils.Assert(sstables[i].LastKey().Compare(sstables[i+1].FirstKey()) < 0, "sstable is invalid")
		}
	}
}

func CreateSstConcatIteratorAndSeekToFirst(sstables []*SsTable) (*SstConcatIterator, error) {
	checkSstValid(sstables)
	if len(sstables) == 0 {
		return &SstConcatIterator{
			current:    nil,
			nextSstIdx: 0,
			sstables:   sstables,
		}, nil
	}
	iter, err := CreateSsTableIteratorAndSeekToFirst(sstables[0])
	if err != nil {
		return nil, err
	}
	sstconcatIter := &SstConcatIterator{
		current:    iter,
		nextSstIdx: 1,
		sstables:   sstables,
	}
	err = sstconcatIter.moveUntilValid()
	if err != nil {
		return nil, err
	}
	return sstconcatIter, nil
}

func CreateSstConcatIteratorAndSeekToKey(sstables []*SsTable, key KeyBytes) (*SstConcatIterator, error) {
	checkSstValid(sstables)
	idx := utils.SaturatingSub(utils.PartitionPoint(sstables, func(sst *SsTable) bool {
		return sst.FirstKey().Compare(key) <= 0
	}), 1)

	if idx >= len(sstables) {
		return &SstConcatIterator{
			current:    nil,
			nextSstIdx: uint32(len(sstables)),
			sstables:   sstables,
		}, nil
	}

	iter, err := CreateSsTableIteratorAndSeekToKey(sstables[idx], key)
	if err != nil {
		return nil, err
	}
	sstconcatIter := &SstConcatIterator{
		current:    iter,
		nextSstIdx: uint32(idx) + 1,
		sstables:   sstables,
	}
	err = sstconcatIter.moveUntilValid()
	if err != nil {
		return nil, err
	}
	return sstconcatIter, nil
}

func (s *SstConcatIterator) moveUntilValid() error {
	for s.current != nil {
		if s.current.IsValid() {
			break
		}
		if s.nextSstIdx >= uint32(len(s.sstables)) {
			s.current = nil
		} else {
			sstIter, err := CreateSsTableIteratorAndSeekToFirst(s.sstables[s.nextSstIdx])
			if err != nil {
				return err
			}
			s.current = sstIter
			s.nextSstIdx++
		}
	}
	return nil
}

func (s *SstConcatIterator) Key() IteratorKey {
	return s.current.Key()
}

func (s *SstConcatIterator) Value() []byte {
	return s.current.Value()
}

func (s *SstConcatIterator) IsValid() bool {
	if s.current == nil {
		return false
	}
	utils.Assert(s.current.IsValid(), "current iterator is invalid")
	return true
}

func (s *SstConcatIterator) Next() error {
	err := s.current.Next()
	if err != nil {
		return err
	}
	err = s.moveUntilValid()
	if err != nil {
		return err

	}
	return nil
}

func (s *SstConcatIterator) NumActiveIterators() int {
	return 1
}
