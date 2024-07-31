package mini_lsm

import (
	"bytes"
	"encoding/binary"
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"math"
)

type Bloom struct {
	filter []byte
	k      uint8
}

func GetBit(b []byte, idx uint32) bool {
	pos := idx / 8
	offset := idx % 8
	return b[pos]&(1<<offset) != 0
}

func SetBit(b []byte, idx uint32, val bool) {
	pos := idx / 8
	offset := idx % 8
	if val {
		b[pos] |= 1 << offset
	} else {
		b[pos] &= ^(1 << offset)
	}
}

func BitLen(b []byte) uint32 {
	return uint32(len(b) * 8)
}

func DecodeBloom(buf []byte) *Bloom {
	checksum := binary.BigEndian.Uint32(buf[len(buf)-4:])
	if utils.Crc32(buf[:len(buf)-4]) != checksum {
		panic("bloom decode checksum error")
	}
	filter := buf[:len(buf)-5]
	k := buf[len(buf)-5]
	return &Bloom{
		filter: utils.Copy(filter),
		k:      k,
	}
}

func (b *Bloom) Encode(buf []byte) []byte {
	buf = append(buf, b.filter...)
	buf = append(buf, b.k)
	return buf
}

func (b *Bloom) EncodeBuffer(buf *bytes.Buffer) {
	offset := buf.Len()
	buf.Write(b.filter)
	buf.WriteByte(b.k)
	content := buf.Bytes()[offset:]
	checksum := utils.Crc32(content)
	err := binary.Write(buf, binary.BigEndian, checksum)
	if err != nil {
		panic(err)
	}
}

func BloomBitsPerKey(entries uint32, falsePositiveRate float64) uint32 {
	size := -1.0 * float64(entries) * math.Log(falsePositiveRate) / math.Pow(math.Ln2, 2)
	locs := math.Ceil(size / float64(entries))
	return uint32(locs)
}

func BuildFromKeyHashes(keys []uint32, bitsPerKey uint32) *Bloom {
	k := uint32(math.Min(math.Max(float64(bitsPerKey)*0.69, 1), 30))
	nbits := uint32(math.Max(float64(len(keys))*float64(bitsPerKey), 64))
	nbytes := (nbits + 7) / 8
	nbits = nbytes * 8
	filter := make([]byte, nbytes)
	for _, h := range keys {
		delta := (h >> 17) | (h << 15)
		for i := uint32(0); i < k; i++ {
			bitPos := h % nbits
			SetBit(filter, bitPos, true)
			h = utils.WrappingAddU32(h, delta)
		}
	}
	return &Bloom{
		filter: filter,
		k:      uint8(k),
	}
}

func (b *Bloom) MayContain(h uint32) bool {
	if b.k > 30 {
		return true
	} else {
		nbits := BitLen(b.filter)
		delta := (h >> 17) | (h << 15)
		for i := uint8(0); i < b.k; i++ {
			bitPos := h % nbits
			if !GetBit(b.filter, bitPos) {
				return false
			}
			h = utils.WrappingAddU32(h, delta)
		}
		return true
	}
}
