package utils

import "math"

// RoundUpToPowerOfTwo 返回不小于n的最小2的次方数
func RoundUpToPowerOfTwo(n uint64) uint64 {
	if n == 0 {
		return 1
	}
	// 使用位运算找到大于或等于n的最小2的幂
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	return n + 1
}

func WrappingAddU32(a, b uint32) uint32 {
	x := uint64(a) + uint64(b)
	if x > math.MaxUint32 {
		return uint32(x - math.MaxUint32 - 1)
	} else {
		return uint32(x)
	}
}

func MaxU32(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}
