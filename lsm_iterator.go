package mini_lsm

import (
	"bytes"
	"errors"
)

// a: TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>
// b: SstConcatIterator,
type LsmIteratorInner = TwoMergeIterator

type LsmIterator struct {
	inner    *LsmIteratorInner
	endBound BytesBound
	isValid  bool
	readTs   uint64
	prevKey  []byte
}

func CreateLsmIterator(iter *LsmIteratorInner, endBound BytesBound, readTs uint64) (*LsmIterator, error) {
	l := &LsmIterator{
		inner:    iter,
		isValid:  iter.IsValid(),
		endBound: endBound,
		readTs:   readTs,
		prevKey:  make([]byte, 0),
	}
	err := l.moveToKey()
	if err != nil {
		return nil, err
	}
	return l, nil
}

//func (l *LsmIterator) moveToNonDelete() error {
//	for {
//		//println("l.inner.KeyOf: ", string(l.inner.Key().KeyRef()), " l.inner.Value: ", string(l.inner.Value()))
//		if l.IsValid() && len(l.inner.Value()) == 0 {
//			err := l.nextInner()
//			if err != nil {
//				return err
//			}
//		} else {
//			break
//		}
//
//	}
//	return nil
//}

func (l *LsmIterator) moveToKey() error {
	// 这个函数的作用是迭代掉所有相同的key(ts 不同)
	for {
		for l.inner.IsValid() && bytes.Compare(l.inner.Key().KeyRef(), l.prevKey) == 0 {
			err := l.nextInner()
			if err != nil {
				return err
			}
		}
		if !l.inner.IsValid() {
			break
		}
		l.prevKey = append(l.prevKey[:0], l.inner.Key().KeyRef()...)
		for l.inner.IsValid() &&
			bytes.Compare(l.inner.Key().KeyRef(), l.prevKey) == 0 &&
			l.inner.Key().(KeyBytes).Ts > l.readTs {
			err := l.nextInner()
			if err != nil {
				return err
			}
		}
		if !l.inner.IsValid() {
			break
		}
		if bytes.Compare(l.inner.Key().KeyRef(), l.prevKey) != 0 {
			continue
		}
		if len(l.inner.Value()) != 0 {
			break
		}
	}
	return nil
}

func (l *LsmIterator) nextInner() error {
	if err := l.inner.Next(); err != nil {
		return err
	}
	if !l.inner.IsValid() {
		l.isValid = false
		return nil
	}
	if l.endBound.Type == Included {
		// println(string(l.inner.Key().KeyRef()), string(l.endBound.Val.KeyRef()))
		// 这里为什么只比较key，而不比较ts，因为lsmIterator已经是比较外层的接口了，他的endBound只是一个[]byte
		// 而根据我们的定义，("a", 233) < ("a", 0) < ("b", 235)
		// 如果是include,那么我们其实找到的第一个key就是最新的key,而exclude则要完全跳过这个key，不管ts是多少
		//l.isValid = l.inner.Key().KeyRefCompare(l.endBound.Val) <= 0
		l.isValid = bytes.Compare(l.inner.Key().KeyRef(), l.endBound.Val) <= 0
	} else if l.endBound.Type == Excluded {
		// l.isValid = l.inner.Key().KeyRefCompare(l.endBound.Val) < 0
		// println("========>>>>", string(l.inner.Key().KeyRef()), string(l.endBound.Val))
		l.isValid = bytes.Compare(l.inner.Key().KeyRef(), l.endBound.Val) < 0
	}
	return nil
}

func (l *LsmIterator) NumActiveIterators() int {
	return l.inner.NumActiveIterators()
}

func (l *LsmIterator) Key() IteratorKey {
	return KeySlice(l.inner.Key().KeyRef())
}

func (l *LsmIterator) Value() []byte {
	return l.inner.Value()
}

func (l *LsmIterator) IsValid() bool {
	return l.isValid
}

func (l *LsmIterator) Next() error {
	if err := l.nextInner(); err != nil {
		return err
	}
	return l.moveToKey()
}

type FusedIterator struct {
	iter     StorageIterator
	hasError bool
}

func CreateFusedIterator(iter StorageIterator) *FusedIterator {
	return &FusedIterator{
		iter:     iter,
		hasError: false,
	}
}

func (f *FusedIterator) Key() IteratorKey {
	if f.hasError || !f.iter.IsValid() {
		panic("called key on an invalid iterator")
	}
	return f.iter.Key()
}

func (f *FusedIterator) Value() []byte {
	if f.hasError || !f.iter.IsValid() {
		panic("called value on an invalid iterator")
	}
	return f.iter.Value()
}

func (f *FusedIterator) IsValid() bool {
	return !f.hasError && f.iter.IsValid()
}

func (f *FusedIterator) Next() error {
	if f.hasError {
		return errors.New("called next on an invalid iterator")
	}
	if f.iter.IsValid() {
		if err := f.iter.Next(); err != nil {
			f.hasError = true
			return err
		}
	}
	return nil
}

func (f *FusedIterator) NumActiveIterators() int {
	return f.iter.NumActiveIterators()
}
