package mini_lsm

import (
	"bytes"
	"encoding/binary"
	"github.com/Zhanghailin1995/mini-lsm/utils"
)

type SsTableBuilder struct {
	blockBuilder *BlockBuilder
	firstKey     KeyType
	lastKey      KeyType
	data         []byte
	meta         []*BlockMeta
	blockSize    uint32
}

func NewSsTableBuilder(blockSize uint32) *SsTableBuilder {
	return &SsTableBuilder{
		blockBuilder: NewBlockBuilder(blockSize),
		firstKey:     Key([]byte{}),
		lastKey:      Key([]byte{}),
		data:         make([]byte, 0), // 应该给一个capacity
		meta:         make([]*BlockMeta, 0),
		blockSize:    blockSize,
	}
}

func (s *SsTableBuilder) Add(key KeyType, value []byte) {
	if len(s.firstKey.Val) == 0 {
		s.firstKey = key.Clone()
	}
	if s.blockBuilder.Add(key, value) {
		s.lastKey = key.Clone()
		return
	}
	//println("finish block at", string(key.Val), string(value))
	s.finishBlock()
	utils.Assert(s.blockBuilder.Add(key, value), "block add error")
	s.firstKey = key.Clone()
	s.lastKey = key.Clone()
}

func (s *SsTableBuilder) finishBlock() {
	builder := s.blockBuilder
	s.blockBuilder = NewBlockBuilder(s.blockSize)
	encodedBlock := builder.Build().Encode()
	s.meta = append(s.meta, &BlockMeta{
		offset:   uint32(len(s.data)),
		firstKey: s.firstKey.Clone(),
		lastKey:  s.lastKey.Clone(),
	})
	s.firstKey = Key([]byte{})
	s.lastKey = Key([]byte{})
	s.data = append(s.data, encodedBlock...)
}

func (s *SsTableBuilder) EstimatedSize() int {
	return len(s.data)
}

func (s *SsTableBuilder) Build(id uint32, blockCache *BlockCache, path string) (*SsTable, error) {
	s.finishBlock()
	buf := s.data
	metaOffset := len(buf)
	buf = EncodeBlockMeta(s.meta, buf)
	// put 32 into buf
	buffer := bytes.NewBuffer(buf)
	buffer.Grow(4) // u32
	utils.ErrorWrapper(binary.Write(buffer, binary.BigEndian, uint32(metaOffset)))
	buf = buffer.Bytes()
	file, err := CreateFileObject(path, buf)
	if err != nil {
		return nil, err
	}
	return &SsTable{
		id:              id,
		file:            file,
		blockCache:      blockCache,
		firstKey:        s.meta[0].firstKey.Clone(),
		lastKey:         s.meta[len(s.meta)-1].lastKey.Clone(),
		blockMeta:       s.meta,
		blockMetaOffset: uint32(metaOffset),
		bloom:           nil,
		maxTs:           0,
	}, nil
}

func (s *SsTableBuilder) buildForTest(path string) (*SsTable, error) {
	return s.Build(0, nil, path)
}
