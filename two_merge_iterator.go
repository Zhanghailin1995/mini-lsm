package mini_lsm

type TwoMergeIterator struct {
	a       StorageIterator
	b       StorageIterator
	chooseA bool
}

func (t *TwoMergeIterator) chooseA0() bool {
	if !t.a.IsValid() {
		return false
	}
	if !t.b.IsValid() {
		return true
	}
	return t.a.Key().Compare(t.b.Key()) < 0
}

func (t *TwoMergeIterator) skipB() error {
	if t.a.IsValid() {
		if t.b.IsValid() && t.a.Key().Compare(t.b.Key()) == 0 {
			if err := t.b.Next(); err != nil {
				return err
			}
		}
	}
	return nil
}

func CreateTwoMergeIterator(a, b StorageIterator) (*TwoMergeIterator, error) {
	iter := &TwoMergeIterator{a: a, b: b, chooseA: false}
	if err := iter.skipB(); err != nil {
		return nil, err
	}
	iter.chooseA = iter.chooseA0()
	return iter, nil
}

func (t *TwoMergeIterator) Key() KeyType {
	if t.chooseA {
		return t.a.Key()
	}
	return t.b.Key()
}

func (t *TwoMergeIterator) Value() []byte {
	if t.chooseA {
		return t.a.Value()
	}
	return t.b.Value()
}

func (t *TwoMergeIterator) IsValid() bool {
	if t.chooseA {
		return t.a.IsValid()
	}
	return t.b.IsValid()
}

func (t *TwoMergeIterator) Next() error {
	if t.chooseA {
		if err := t.a.Next(); err != nil {
			return err
		}
	} else {
		if err := t.b.Next(); err != nil {
			return err
		}
	}
	if err := t.skipB(); err != nil {
		return err
	}
	t.chooseA = t.chooseA0()
	return nil
}

func (t *TwoMergeIterator) NumActiveIterators() int {
	return 1
}
