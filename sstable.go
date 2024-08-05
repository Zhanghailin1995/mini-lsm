package mini_lsm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"os"
	"sync"
	"unsafe"
)

type BlockMeta struct {
	offset   uint32
	firstKey KeyBytes
	lastKey  KeyBytes
}

func EncodeBlockMeta(blockMeta []*BlockMeta, maxTs uint64, buf []byte) []byte {
	// the size of the block meta
	estimatedSize := 4
	for _, bm := range blockMeta {
		// the size of the offset
		estimatedSize += int(unsafe.Sizeof(uint32(0)))
		// the size of the first key len
		estimatedSize += int(unsafe.Sizeof(uint16(0)))
		// the size of the first key
		estimatedSize += bm.firstKey.RawLen()
		// the size of the last key len
		estimatedSize += int(unsafe.Sizeof(uint16(0)))
		// the size of the last key
		estimatedSize += bm.lastKey.RawLen()
	}
	estimatedSize += 8 // maxTs
	estimatedSize += 4 // crc32
	//println("estimated size", estimatedSize)
	// ensure the buf has enough space
	originalLen := len(buf)
	buffer := bytes.NewBuffer(buf)
	buffer.Grow(estimatedSize)
	utils.ErrorWrapper(binary.Write(buffer, binary.BigEndian, uint32(len(blockMeta)))) // block meta num
	for _, bm := range blockMeta {
		utils.ErrorWrapper(binary.Write(buffer, binary.BigEndian, bm.offset))                    // offset
		utils.ErrorWrapper(binary.Write(buffer, binary.BigEndian, uint16(len(bm.firstKey.Val)))) // first key len
		buffer.Write(bm.firstKey.Val)                                                            // first key
		utils.ErrorWrapper(binary.Write(buffer, binary.BigEndian, bm.firstKey.Ts))               // first key ts
		utils.ErrorWrapper(binary.Write(buffer, binary.BigEndian, uint16(len(bm.lastKey.Val))))  // last key len
		buffer.Write(bm.lastKey.Val)                                                             // last key
		utils.ErrorWrapper(binary.Write(buffer, binary.BigEndian, bm.lastKey.Ts))                // last key ts
		//println(bm.offset, len(bm.firstKey.Val), string(bm.firstKey.Val), bm.firstKey.Ts, len(bm.lastKey.Val), string(bm.lastKey.Val), bm.lastKey.Ts)
	}
	utils.ErrorWrapper(binary.Write(buffer, binary.BigEndian, maxTs)) // maxTs
	tmp := buffer.Bytes()
	checksum := utils.Crc32(tmp[originalLen+4:])
	utils.ErrorWrapper(binary.Write(buffer, binary.BigEndian, checksum))
	utils.Assert(len(buffer.Bytes())-originalLen == estimatedSize, "encode block meta error")
	return buffer.Bytes()
}

func DecodeBlockMeta(data []byte) ([]*BlockMeta, uint64) {
	var blockMeta []*BlockMeta
	num := binary.BigEndian.Uint32(data)
	data = data[4:]
	checksum := utils.Crc32(data[:len(data)-4])
	for i := 0; i < int(num); i++ {

		offset := binary.BigEndian.Uint32(data)
		data = data[4:]

		firstKeyLen := binary.BigEndian.Uint16(data)
		data = data[2:]

		firstKey := utils.Copy(data[:firstKeyLen])
		data = data[firstKeyLen:]

		firstKeyTs := binary.BigEndian.Uint64(data)
		data = data[8:]

		lastKeyLen := binary.BigEndian.Uint16(data)
		data = data[2:]

		lastKey := utils.Copy(data[:lastKeyLen])
		data = data[lastKeyLen:]

		lastKeyTs := binary.BigEndian.Uint64(data)
		data = data[8:]
		//println(i, offset, firstKeyLen, string(firstKey), firstKeyTs, lastKeyLen, string(lastKey), lastKeyTs)
		blockMeta = append(blockMeta, &BlockMeta{
			offset:   offset,
			firstKey: KeyFromBytesWithTs(firstKey, firstKeyTs),
			lastKey:  KeyFromBytesWithTs(lastKey, lastKeyTs),
		})
	}
	maxTs := binary.BigEndian.Uint64(data)
	data = data[8:]
	if checksum != binary.BigEndian.Uint32(data) {
		panic("decode block meta checksum error")
	}
	return blockMeta, maxTs
}

type FileObject struct {
	file *os.File
	size int64
}

func createFileObjectSizeOnly(size int64) *FileObject {
	return &FileObject{
		file: nil,
		size: size,
	}
}

func (f *FileObject) Close() error {
	//println("close file", f.file.Name())
	return f.file.Close()
}

func OpenFileObject(path string) (*FileObject, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fi, err := file.Stat()
	if err != nil {
		return nil, err
	}
	return &FileObject{file: file, size: fi.Size()}, nil
}

func CreateFileObject(path string, data []byte) (*FileObject, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	n, err := file.Write(data)
	if err != nil {
		return nil, err
	}
	if n != len(data) {
		return nil, errors.New("write data not enough")
	}
	if err = file.Sync(); err != nil {
		return nil, err
	}
	if err = file.Close(); err != nil {
		return nil, err
	}
	file, err = os.Open(path)
	if err != nil {
		return nil, err
	}
	return &FileObject{file: file, size: int64(len(data))}, nil
}

func (f *FileObject) Read(offset int64, size int) ([]byte, error) {
	data := make([]byte, size)
	buf := data[:]
	for len(buf) > 0 {
		n, err := f.file.ReadAt(buf, offset)
		if n == 0 {
			break
		} else if n > 0 {
			tmp := buf
			buf = tmp[n:]
			offset += int64(n)
		} else if err != nil {
			// 这个实现正确吗？// FIXME
			if utils.IsEINTR(err) {
				continue
			}
			// TODO if err is interrupt, continue read
			return nil, err
		}

	}
	if len(buf) > 0 {
		return nil, errors.New("read data not enough")
	}
	return data, nil
}

func (f *FileObject) Size() int64 {
	return f.size
}

type BlockCache struct {
	rwLock sync.RWMutex
	cache  map[BlockKey]*Block
}

type BlockKey struct {
	sstId    uint32
	blockIdx uint32
}

func BlockCacheKey(sstId, blockIdx uint32) BlockKey {
	return BlockKey{
		sstId:    sstId,
		blockIdx: blockIdx,
	}
}

func NewBlockCache() *BlockCache {
	return &BlockCache{
		rwLock: sync.RWMutex{},
		cache:  make(map[BlockKey]*Block),
	}
}

type GetBlockFunc func(*SsTable, uint32) (*Block, error)

func (b *BlockCache) TryGetWith(sstId, blockIdx uint32, sst *SsTable, getBlockFunc GetBlockFunc) (*Block, error) {
	b.rwLock.RLock()
	block, ok := b.cache[BlockCacheKey(sstId, blockIdx)]
	if !ok {
		b.rwLock.RUnlock()
		b.rwLock.Lock()
		// double check
		if block, ok := b.cache[BlockCacheKey(sstId, blockIdx)]; ok {
			b.rwLock.Unlock()
			return block, nil
		}
		block, err := getBlockFunc(sst, blockIdx)
		if err != nil {
			return nil, err
		}
		b.cache[BlockCacheKey(sstId, blockIdx)] = block
		b.rwLock.Unlock()
		return block, nil
	} else {
		b.rwLock.RUnlock()
		return block, nil
	}
}

type SsTable struct {
	file            *FileObject
	blockMeta       []*BlockMeta
	blockMetaOffset uint32
	id              uint32
	blockCache      *BlockCache
	firstKey        KeyBytes
	lastKey         KeyBytes
	bloom           *Bloom
	maxTs           uint64
}

func OpenSsTableForTest(file *FileObject) (*SsTable, error) {
	return OpenSsTable(0, nil, file)
}

func OpenSsTable(id uint32, blockCache *BlockCache, file *FileObject) (*SsTable, error) {
	fileSize := file.Size()

	// read bloom
	rawBloomOffset, err := file.Read(fileSize-4, 4)
	if err != nil {
		return nil, err
	}
	bloomOffset := binary.BigEndian.Uint32(rawBloomOffset)
	rawBloom, err := file.Read(int64(bloomOffset), int(fileSize)-4-int(bloomOffset))
	if err != nil {
		return nil, err
	}
	bloom := DecodeBloom(rawBloom)

	rawMetaOffset, err := file.Read(int64(bloomOffset-4), 4)
	if err != nil {
		return nil, err
	}
	blockMetaOffset := binary.BigEndian.Uint32(rawMetaOffset)
	rawMeta, err := file.Read(int64(blockMetaOffset), int(bloomOffset)-4-int(blockMetaOffset))
	if err != nil {
		return nil, err
	}
	blockMeta, maxTs := DecodeBlockMeta(rawMeta)

	firstKey := blockMeta[0].firstKey.Clone()
	lastKey := blockMeta[len(blockMeta)-1].lastKey.Clone()
	return &SsTable{
		file:            file,
		firstKey:        firstKey,
		lastKey:         lastKey,
		blockMeta:       blockMeta,
		id:              id,
		blockMetaOffset: blockMetaOffset,
		blockCache:      blockCache,
		bloom:           bloom,
		maxTs:           maxTs,
	}, nil
}

func createSsTableMetaOnly(id uint32, fileSize int64, firstKey KeyBytes, lastKey KeyBytes) *SsTable {
	return &SsTable{
		file:            createFileObjectSizeOnly(fileSize),
		firstKey:        firstKey,
		lastKey:         lastKey,
		blockMeta:       make([]*BlockMeta, 0),
		id:              id,
		blockMetaOffset: uint32(fileSize),
		blockCache:      nil,
		bloom:           nil,
		maxTs:           0,
	}
}

func (s *SsTable) NumOfBlocks() int {
	return len(s.blockMeta)
}

func (s *SsTable) readBlock(blockIdx uint32) (*Block, error) {
	offset := s.blockMeta[blockIdx].offset
	var offsetEnd uint32
	if blockIdx+1 >= uint32(len(s.blockMeta)) {
		offsetEnd = s.blockMetaOffset
	} else {
		offsetEnd = s.blockMeta[blockIdx+1].offset
	}
	blockLen := offsetEnd - offset - 4
	rawBlockWithChksum, err := s.file.Read(int64(offset), int(blockLen)+4)
	if err != nil {
		return nil, err
	}
	rawBlock := rawBlockWithChksum[:blockLen]
	checksum := binary.BigEndian.Uint32(rawBlockWithChksum[blockLen:])
	if checksum != utils.Crc32(rawBlock) {
		panic("block checksum error")
	}
	return DecodeBlock(rawBlock), nil
}

func (s *SsTable) ReadBlockCached(blockIdx uint32) (*Block, error) {
	if s.blockCache != nil {
		return s.blockCache.TryGetWith(s.id, blockIdx, s, (*SsTable).readBlock)
	}
	return s.readBlock(blockIdx)
}

func (s *SsTable) FindBlockIdx(key KeyBytes) uint32 {
	// 这个方法的实现抄自rust的标准库 partition_point
	pred := func(meta *BlockMeta) bool {
		return meta.firstKey.Compare(key) <= 0
	}

	size := len(s.blockMeta)
	left, right := 0, size
	for left < right {
		mid := left + size/2
		less := pred(s.blockMeta[mid])
		if less {
			left = mid + 1
		} else {
			right = mid
		}
		// we ignore equal
		size = right - left
	}
	utils.Assert(left <= len(s.blockMeta), "find block idx error")
	if left-1 < 0 {
		return 0
	}
	return uint32(left - 1)
}

func (s *SsTable) CloseSstFile() error {
	return s.file.Close()
}

func (s *SsTable) FirstKey() KeyBytes {
	return s.firstKey
}

func (s *SsTable) LastKey() KeyBytes {
	return s.lastKey
}

func (s *SsTable) TableSize() int64 {
	return s.file.Size()
}

func (s *SsTable) Id() uint32 {
	return s.id
}

func (s *SsTable) MaxTs() uint64 {
	return s.maxTs
}
