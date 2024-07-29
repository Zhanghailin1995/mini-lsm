package mini_lsm

type StorageIterator interface {

	// Value Get the current value
	Value() []byte

	// Key Get the current key
	Key() IteratorKey

	// IsValid Check if the current iterator is valid
	IsValid() bool

	// Next Move the iterator to the next element
	Next() error

	// NumActiveIterators Number of underlying active iterators for this iterator
	NumActiveIterators() int
}

func PrintIter(iter StorageIterator) {
	println()
	println("================================")
	for iter.IsValid() {
		println(string(iter.Key().KeyRef()), string(iter.Value()))
		iter.Next()
	}
	println("================================")
	println()
}
