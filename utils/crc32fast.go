package utils

import "hash/crc32"

var crc32Table = crc32.MakeTable(crc32.Castagnoli)

func Crc32(data []byte) uint32 {
	return crc32.Checksum(data, crc32Table)
}
