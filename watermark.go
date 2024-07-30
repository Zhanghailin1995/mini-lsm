package mini_lsm

type Watermark struct {
	readers map[uint64]uint32
}

func NewWaterMark() *Watermark {
	return &Watermark{
		readers: make(map[uint64]uint32),
	}
}

func (w *Watermark) AddReader(readTs uint64) {

}

func (w *Watermark) RemoveReader(readTs uint64) {

}

func (w *Watermark) Watermark() (uint64, bool) {
	return 0, true
}
