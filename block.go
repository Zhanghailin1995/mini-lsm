package mini_lsm

import (
	"bytes"
	"encoding/binary"
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"unsafe"
)

const (
	SizeOfU16 = uint16(unsafe.Sizeof(uint16(0)))
)

type Block struct {
	data    []byte
	offsets []uint16
}

func (b *Block) GetFirstKey() KeyBytes {
	buf := b.data
	// advance 2 bytes to skip the first key overlap
	buf = buf[SizeOfU16:]
	keyLen := binary.BigEndian.Uint16(buf)
	// advance 2 bytes to skip the key len
	buf = buf[SizeOfU16:]
	key := buf[:keyLen]
	ts := binary.BigEndian.Uint64(buf[keyLen:])
	return KeyFromBytesWithTs(utils.Copy(key), ts)
}

func (b *Block) Encode() []byte {
	estimatedSize := b.estimatedSize()
	rawBuffer := make([]byte, 0, utils.RoundUpToPowerOfTwo(uint64(estimatedSize)))
	buf := bytes.NewBuffer(rawBuffer)
	buf.Write(b.data)
	for _, offset := range b.offsets {
		err := binary.Write(buf, binary.BigEndian, offset)
		if err != nil {
			panic(err)
		}
	}
	err := binary.Write(buf, binary.BigEndian, uint16(len(b.offsets)))
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func DecodeBlock(data []byte) *Block {
	// 从buf的最后两个字节读取offsets的长度
	entryOffsetsLen := binary.BigEndian.Uint16(data[len(data)-int(SizeOfU16):])
	dataEnd := len(data) - int(SizeOfU16) - int(entryOffsetsLen)*int(SizeOfU16)
	offsetsRawBuf := data[dataEnd : len(data)-int(SizeOfU16)]
	offsets := make([]uint16, entryOffsetsLen)
	for i := 0; i < int(entryOffsetsLen); i++ {
		offsets[i] = binary.BigEndian.Uint16(offsetsRawBuf[i*int(SizeOfU16) : (i+1)*int(SizeOfU16)])
	}
	block := &Block{
		data:    utils.Copy(data[:dataEnd]),
		offsets: offsets,
	}
	return block
}

func (b *Block) estimatedSize() int {
	return len(b.data) + len(b.offsets)*int(SizeOfU16) + int(SizeOfU16)
}

type BlockBuilder struct {
	offsets   []uint16
	data      []byte
	blockSize uint32
	firstKey  KeyBytes
}

func computeOverlap(firstKey, key []byte) uint32 {
	i := 0
	for {
		if i >= len(firstKey) || i >= len(key) {
			break
		}
		if firstKey[i] != key[i] {
			break
		}
		i++
	}
	return uint32(i)
}

func NewBlockBuilder(blockSize uint32) *BlockBuilder {
	return &BlockBuilder{
		offsets:   make([]uint16, 0, 256),
		data:      make([]byte, 0, utils.RoundUpToPowerOfTwo(uint64(blockSize))), // power of 2
		blockSize: blockSize,
		firstKey:  KeyOf([]byte{}),
	}
}

func (b *BlockBuilder) estimatedSize() uint32 {
	// 2 表示 block有多少个entry
	return uint32(SizeOfU16) + uint32(len(b.data)) + uint32(len(b.offsets))*uint32(SizeOfU16)
}

func (b *BlockBuilder) Add(key KeyBytes, value []byte) bool {
	utils.Assert(len(key.Val) != 0, "key must not be empty")
	// 为什么要加self.is_empty()判断
	// 如果一个大key-value对加入到一个空的block中，会导致block的大小超过block_size
	// 那么应该允许其加入，否则会导致该key-value一直无法写入，block size是软限制，并非一定不可以超过
	// 2 * 3 一个key len 一个 value len 一个offset
	// key_overlap_len (u16) | rest_key_len (u16) | key (rest_key_len)
	if b.estimatedSize()+uint32(key.RawLen()+len(value)+int(SizeOfU16)*3) > b.blockSize && !b.isEmpty() {
		return false
	}

	b.offsets = append(b.offsets, uint16(len(b.data)))

	overlap := computeOverlap(b.firstKey.Val, key.Val)
	idx := len(b.data)
	// put key overlap
	b.data = append(b.data, make([]byte, SizeOfU16)...)
	binary.BigEndian.PutUint16(b.data[idx:idx+int(SizeOfU16)], uint16(overlap))
	idx += int(SizeOfU16)
	// put key len = key.len - overlap

	b.data = append(b.data, make([]byte, SizeOfU16)...)
	binary.BigEndian.PutUint16(b.data[idx:idx+int(SizeOfU16)], uint16(key.KeyLen()-int(overlap)))
	idx += int(SizeOfU16)

	// put key
	b.data = append(b.data, key.Val[overlap:]...)
	idx += len(key.Val) - int(overlap)

	b.data = append(b.data, make([]byte, 8)...)
	binary.BigEndian.PutUint64(b.data[idx:idx+8], key.Ts)
	idx += 8

	// put value len
	b.data = append(b.data, make([]byte, SizeOfU16)...)
	binary.BigEndian.PutUint16(b.data[idx:idx+int(SizeOfU16)], uint16(len(value)))
	idx += int(SizeOfU16)

	// put value
	b.data = append(b.data, value...)
	if b.firstKey.Val == nil || len(b.firstKey.Val) == 0 {
		b.firstKey = key.Clone()
	}
	return true
}

func (b *BlockBuilder) isEmpty() bool {
	return len(b.offsets) == 0
}

func (b *BlockBuilder) Build() *Block {
	if b.isEmpty() {
		panic("block should not be empty")
	}
	block := &Block{
		data:    b.data,
		offsets: b.offsets,
	}
	b.offsets = nil
	b.data = nil
	b.blockSize = 0
	b.firstKey = KeyOf(nil)
	return block
}

type BlockIterator struct {
	block      *Block
	key        KeyBytes
	valueStart int
	valueEnd   int
	idx        int
	firstKey   KeyBytes
}

func NewBlockIterator(block *Block) *BlockIterator {
	return &BlockIterator{
		block:      block,
		key:        KeyOf([]byte{}),
		valueStart: 0,
		valueEnd:   0,
		idx:        0,
		firstKey:   block.GetFirstKey(),
	}
}

func (b *Block) CreateIteratorAndSeekToFirst() *BlockIterator {
	bi := NewBlockIterator(b)
	bi.SeekToFirst()
	return bi
}

func (b *Block) CreateIteratorAndSeekToKey(key KeyBytes) *BlockIterator {
	bi := NewBlockIterator(b)
	bi.SeekToKey(key)
	return bi
}

func (b *BlockIterator) Key() KeyBytes {
	return b.key
}

func (b *BlockIterator) Value() []byte {
	return b.block.data[b.valueStart:b.valueEnd]

}

func (b *BlockIterator) SeekToFirst() {
	b.seekTo(0)
}

func (b *BlockIterator) seekToOffset(offset int) {
	entry := b.block.data[offset:]
	overlapLen := int(binary.BigEndian.Uint16(entry))
	entry = entry[SizeOfU16:]

	keyLen := int(binary.BigEndian.Uint16(entry))
	entry = entry[SizeOfU16:]

	key := entry[:keyLen]
	entry = entry[keyLen:]
	rawKey := make([]byte, overlapLen+len(key))
	copy(rawKey, b.firstKey.Val[:overlapLen])
	copy(rawKey[overlapLen:], key)
	//println("rawKey: ", string(rawKey))
	ts := binary.BigEndian.Uint64(entry)
	entry = entry[8:]
	b.key = KeyFromBytesWithTs(rawKey, ts)

	valueLen := int(binary.BigEndian.Uint16(entry))
	// offset + overlaplen's len + keylen's len + keylen + valuelen's len
	b.valueStart = offset + int(SizeOfU16) + int(SizeOfU16) + keyLen + 8 + int(SizeOfU16)
	b.valueEnd = b.valueStart + valueLen
}

func (b *BlockIterator) seekTo(idx int) {
	if idx >= len(b.block.offsets) {
		b.key = KeyOf([]byte{})
		b.valueStart = 0
		b.valueEnd = 0
		return
	}
	offset := int(b.block.offsets[idx])
	b.seekToOffset(offset)
	b.idx = idx
}

func (b *BlockIterator) IsValid() bool {
	return b.key.Val != nil && len(b.key.Val) != 0
}

func (b *BlockIterator) Next() {
	b.idx++
	b.seekTo(b.idx)
}

func (b *BlockIterator) SeekToKey(key KeyBytes) {
	low := 0
	high := len(b.block.offsets)
	for low < high {
		mid := low + (high-low)/2
		b.seekTo(mid)
		utils.Assert(b.IsValid(), "block iterator is invalid")
		cmp := b.key.Compare(key)
		if cmp < 0 {
			low = mid + 1
		} else if cmp > 0 {
			high = mid
		} else {
			return
		}
	}
	b.seekTo(low)
}
