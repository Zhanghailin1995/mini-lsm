package utils

import (
	"errors"
	"os"
	"reflect"
	"syscall"
)

func IsEINTR(err error) bool {
	var syscallErr syscall.Errno
	if errors.As(err, &syscallErr) {
		return syscallErr == syscall.EINTR
	}
	return false
}

func ErrorWrapper(err error) {
	if err != nil {
		panic(err)
	}
}

func Unwrap[T any](t T, err error) T {
	if err != nil {
		panic(err)
	}
	return t
}

func UnwrapError(err error) {
	if err != nil {
		panic(err)
	}
}

func CreateDirIfNotExist(path string) error {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return os.MkdirAll(path, os.ModePerm)
	} else {
		return err
	}
}

func RepeatByte(b byte, n int) []byte {
	bytes := make([]byte, n)
	for i := 0; i < n; i++ {
		bytes[i] = b
	}
	return bytes
}

func PartitionPoint[T any](arr []T, pred func(T) bool) int {
	size := len(arr)
	left := 0
	right := size
	for left < right {
		mid := left + size/2
		less := pred(arr[mid])
		if less {
			left = mid + 1
		} else {
			right = mid
		}
		// we ignore equal
		size = right - left
	}
	Assert(left <= len(arr), "find partition point idx error")
	return left
}

func SaturatingSub(a, b int) int {
	if a-b < 0 {
		return 0
	}
	return a - b
}

func IsNil(x interface{}) bool {
	if x == nil {
		return true
	}
	rv := reflect.ValueOf(x)
	return rv.Kind() == reflect.Ptr && rv.IsNil()
}
