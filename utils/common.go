package utils

import (
	"errors"
	"os"
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
