package utils

import (
	"errors"
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
