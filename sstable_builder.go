package mini_lsm

import (
	"bytes"
	"encoding/binary"
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"github.com/dgryski/go-farm"
)

type SsTableBuilder struct {
	blockBuilder *BlockBuilder
	firstKey     KeyBytes
	lastKey      KeyBytes
	data         []byte
	meta         []*BlockMeta
	blockSize    uint32
	keyHashes    []uint32
	maxTs        uint64
}

func NewSsTableBuilder(blockSize uint32) *SsTableBuilder {
	return &SsTableBuilder{
		blockBuilder: NewBlockBuilder(blockSize),
		firstKey:     KeyOf([]byte{}),
		lastKey:      KeyOf([]byte{}),
		data:         make([]byte, 0), // 应该给一个capacity
		meta:         make([]*BlockMeta, 0),
		blockSize:    blockSize,
		keyHashes:    make([]uint32, 0),
		maxTs:        uint64(0),
	}
}

func (s *SsTableBuilder) Add(key KeyBytes, value []byte) {
	if len(s.firstKey.Val) == 0 {
		s.firstKey = key.Clone()
	}
	if key.Ts > s.maxTs {
		s.maxTs = key.Ts
	}
	s.keyHashes = append(s.keyHashes, farm.Fingerprint32(key.Val))
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
	s.firstKey = KeyOf([]byte{})
	s.lastKey = KeyOf([]byte{})
	checksum := utils.Crc32(encodedBlock)
	checkSumBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(checkSumBuf, checksum)
	s.data = append(s.data, encodedBlock...)
	s.data = append(s.data, checkSumBuf...)
}

func (s *SsTableBuilder) EstimatedSize() int {
	return len(s.data)
}

func (s *SsTableBuilder) Build(id uint32, blockCache *BlockCache, path string) (*SsTable, error) {
	s.finishBlock()
	buf := s.data
	metaOffset := len(buf)
	//println("metaOffset: ", metaOffset)
	//println("meta size: ", len(s.meta))
	buf = EncodeBlockMeta(s.meta, s.maxTs, buf)
	//println("buf size: ", len(buf))
	// put 32 into buf
	buffer := bytes.NewBuffer(buf)
	//buffer.Grow(4) // u32
	utils.UnwrapError(binary.Write(buffer, binary.BigEndian, uint32(metaOffset)))

	bloom := BuildFromKeyHashes(s.keyHashes, BloomBitsPerKey(uint32(len(s.keyHashes)), 0.01))
	//println("bloom size: ", len(bloom.filter))
	bloomOffset := buffer.Len()
	bloom.EncodeBuffer(buffer)
	utils.UnwrapError(binary.Write(buffer, binary.BigEndian, uint32(bloomOffset)))

	buf = buffer.Bytes()
	//println("buf size: ", len(buf))
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
		bloom:           bloom,
		maxTs:           s.maxTs,
	}, nil
}

func (s *SsTableBuilder) buildForTest(path string) (*SsTable, error) {
	return s.Build(0, nil, path)
}
