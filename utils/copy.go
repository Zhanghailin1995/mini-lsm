package utils

func Copy(in []byte) (out []byte) {
	out = make([]byte, len(in))
	copy(out, in)
	return out
}
