package mini_lsm

type SsTableIterator struct {
	table   *SsTable
	blkIter *BlockIterator
	blkIdx  uint32
}

func seekSsTableToFirstInner(sst *SsTable) (uint32, *BlockIterator, error) {
	block, err := sst.ReadBlockCached(0)
	if err != nil {
		return 0, nil, err
	}
	blkIter := block.CreateIteratorAndSeekToFirst()
	return 0, blkIter, nil
}

func CreateSsTableIteratorAndSeekToFirst(sst *SsTable) (*SsTableIterator, error) {
	blkIdx, blkIter, err := seekSsTableToFirstInner(sst)
	if err != nil {
		return nil, err
	}
	return &SsTableIterator{
		table:   sst,
		blkIter: blkIter,
		blkIdx:  blkIdx,
	}, nil
}

func (s *SsTableIterator) SeekToFirst() error {
	blkIdx, blkIter, err := seekSsTableToFirstInner(s.table)
	if err != nil {
		return err
	}
	s.blkIdx = blkIdx
	s.blkIter = blkIter
	return nil
}

func seekSsTableToKeyInner(sst *SsTable, key KeyType) (uint32, *BlockIterator, error) {
	blockIdx := sst.FindBlockIdx(key)
	block, err := sst.ReadBlockCached(blockIdx)
	if err != nil {
		return 0, nil, err
	}
	blkIter := block.CreateIteratorAndSeekToKey(key)
	if !blkIter.IsValid() {
		blockIdx++
		if blockIdx < uint32(sst.NumOfBlocks()) {
			block, err = sst.ReadBlockCached(blockIdx)
			if err != nil {
				return 0, nil, err
			}
			blkIter = block.CreateIteratorAndSeekToFirst()
		}
	}
	return blockIdx, blkIter, nil
}

func CreateSsTableIteratorAndSeekToKey(sst *SsTable, key KeyType) (*SsTableIterator, error) {
	blkIdx, blkIter, err := seekSsTableToKeyInner(sst, key)
	if err != nil {
		return nil, err
	}
	return &SsTableIterator{
		table:   sst,
		blkIter: blkIter,
		blkIdx:  blkIdx,
	}, nil
}

func (s *SsTableIterator) SeekToKey(key KeyType) error {
	blkIdx, blkIter, err := seekSsTableToKeyInner(s.table, key)
	if err != nil {
		return err
	}
	s.blkIdx = blkIdx
	s.blkIter = blkIter
	return nil
}

func (s *SsTableIterator) Key() KeyType {
	return s.blkIter.Key()
}

func (s *SsTableIterator) Value() []byte {
	return s.blkIter.Value()
}

func (s *SsTableIterator) IsValid() bool {
	return s.blkIter.IsValid()
}

func (s *SsTableIterator) Next() error {
	s.blkIter.Next()
	if !s.blkIter.IsValid() {
		s.blkIdx++
		if s.blkIdx < uint32(s.table.NumOfBlocks()) {
			block, err := s.table.ReadBlockCached(s.blkIdx)
			if err != nil {
				return err
			}
			s.blkIter = block.CreateIteratorAndSeekToFirst()
		}
	}
	return nil
}

func (s *SsTableIterator) NumActiveIterators() int {
	return 1
}
