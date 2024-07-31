package mini_lsm

import "github.com/tidwall/btree"

type Watermark struct {
	readers *btree.Map[uint64, uint32]
}

func NewWaterMark() *Watermark {
	return &Watermark{
		readers: btree.NewMap[uint64, uint32](32),
	}
}

func (w *Watermark) AddReader(readTs uint64) {
	e, ok := w.readers.Get(readTs)
	if ok {
		w.readers.Set(readTs, e+1)
	} else {
		w.readers.Set(readTs, 1)
	}
}

func (w *Watermark) NumRetainedSnapshot() uint32 {
	return uint32(w.readers.Len())
}

func (w *Watermark) RemoveReader(readTs uint64) {
	e, ok := w.readers.Get(readTs)
	if ok {
		if e == 1 {
			w.readers.Delete(readTs)
		} else {
			w.readers.Set(readTs, e-1)
		}
	}
}

func (w *Watermark) Watermark() (uint64, bool) {
	if w.readers.Len() == 0 {
		return 0, false
	}
	k, _, ok := w.readers.Min()
	if !ok {
		return 0, false
	}
	return k, true
}
