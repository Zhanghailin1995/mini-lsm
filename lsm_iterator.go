package mini_lsm

import "errors"

// a: TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>
// b: SstConcatIterator,
type LsmIteratorInner = TwoMergeIterator

type LsmIterator struct {
	inner    *LsmIteratorInner
	endBound KeyBound
	isValid  bool
}

func CreateLsmIterator(iter *LsmIteratorInner, endBound KeyBound) (*LsmIterator, error) {
	l := &LsmIterator{
		inner:    iter,
		isValid:  iter.IsValid(),
		endBound: endBound,
	}
	err := l.moveToNonDelete()
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (l *LsmIterator) moveToNonDelete() error {
	for {
		//println("l.inner.KeyOf: ", string(l.inner.Key().KeyRef()), " l.inner.Value: ", string(l.inner.Value()))
		if l.IsValid() && len(l.inner.Value()) == 0 {
			err := l.nextInner()
			if err != nil {
				return err
			}
		} else {
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
		l.isValid = l.inner.Key().KeyRefCompare(l.endBound.Val) <= 0
	} else if l.endBound.Type == Excluded {
		l.isValid = l.inner.Key().KeyRefCompare(l.endBound.Val) < 0
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
