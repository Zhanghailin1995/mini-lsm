package mini_lsm

import "errors"

type LsmIteratorInner = MergeIterator

type LsmIterator struct {
	inner *LsmIteratorInner
}

func CreateLsmIterator(iter *LsmIteratorInner) (*LsmIterator, error) {
	l := &LsmIterator{
		inner: iter,
	}
	err := l.moveToNonDelete()
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (l *LsmIterator) moveToNonDelete() error {
	for {
		//println("l.inner.Key: ", string(l.inner.Key().Val), " l.inner.Value: ", string(l.inner.Value()))
		if l.IsValid() && len(l.inner.Value()) == 0 {
			err := l.Next()
			if err != nil {
				return err
			}
		} else {
			break
		}

	}
	return nil
}

func (l *LsmIterator) NumActiveIterators() int {
	return l.inner.NumActiveIterators()
}

func (l *LsmIterator) Key() KeyType {
	return l.inner.Key()
}

func (l *LsmIterator) Value() []byte {
	return l.inner.Value()
}

func (l *LsmIterator) IsValid() bool {
	return l.inner.IsValid()
}

func (l *LsmIterator) Next() error {
	err := l.inner.Next()
	if err != nil {
		return err
	}
	return l.moveToNonDelete()
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

func (f *FusedIterator) Key() KeyType {
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
