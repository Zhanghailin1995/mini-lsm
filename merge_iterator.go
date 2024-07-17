package mini_lsm

import (
	"container/heap"
)

type HeapWrapper struct {
	idx  uint32
	iter StorageIterator
}

func (h *HeapWrapper) Compare(other *HeapWrapper) int {
	//res := bytes.Compare(h.iter.Key().Val, other.iter.Key().Val)
	res := h.iter.Key().Compare(other.iter.Key())
	if res == 0 {
		if h.idx < other.idx {
			return -1
		} else if h.idx > other.idx {
			return 1
		} else {
			return 0
		}

	}
	return res
}

type IterBinaryHeap []*HeapWrapper

func (h IterBinaryHeap) Len() int { return len(h) }

func (h IterBinaryHeap) Less(i, j int) bool {
	//res := bytes.Compare(h[i].iter.Key().Val, h[j].iter.Key().Val)
	res := h[i].iter.Key().Compare(h[j].iter.Key())
	if res == 0 {
		return h[i].idx < h[j].idx
	}
	return res < 0
}

func (h IterBinaryHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *IterBinaryHeap) Push(x interface{}) {
	*h = append(*h, x.(*HeapWrapper))
}

func (h *IterBinaryHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type MergeIterator struct {
	iters   IterBinaryHeap
	current *HeapWrapper
}

func CreateMergeIterator(iters []StorageIterator) *MergeIterator {
	if len(iters) == 0 {
		return &MergeIterator{
			iters:   make(IterBinaryHeap, 0),
			current: nil,
		}
	}

	h := make(IterBinaryHeap, 0, len(iters))
	heap.Init(&h)

	allInvalid := true
	for i, iter := range iters {
		if iter.IsValid() {
			allInvalid = false
			heap.Push(&h, &HeapWrapper{
				iter: iter,
				idx:  uint32(i),
			})
		}
	}

	if allInvalid {
		return &MergeIterator{
			iters: h,
			current: &HeapWrapper{
				iter: iters[0],
				idx:  0,
			},
		}
	}

	current := heap.Pop(&h).(*HeapWrapper)

	return &MergeIterator{
		iters:   h,
		current: current,
	}
}

func (m *MergeIterator) Key() KeyType {
	return m.current.iter.Key()
}

func (m *MergeIterator) Value() []byte {
	return m.current.iter.Value()
}

func (m *MergeIterator) IsValid() bool {
	return m.current != nil && m.current.iter.IsValid()
}

func (m *MergeIterator) NumActiveIterators() int {
	total := 0
	for _, iter := range m.iters {
		total += iter.iter.NumActiveIterators()
	}
	if m.current != nil {
		total += m.current.iter.NumActiveIterators()
	}
	return total
}

func (m *MergeIterator) Next() error {
	for m.iters.Len() > 0 {
		innerIter := heap.Pop(&m.iters).(*HeapWrapper)
		//println("1inner:", string(innerIter.iter.Key().Val), innerIter.idx)
		//println("1curr:", string(m.current.iter.Key().Val), m.current.idx)
		if innerIter.iter.Key().Compare(m.current.iter.Key()) < 0 {
			panic("invalid key order")
		}
		if innerIter.iter.Key().Compare(m.current.iter.Key()) == 0 {
			//if bytes.Compare(innerIter.iter.Key().Val, m.current.iter.Key().Val) == 0 {
			if err := innerIter.iter.Next(); err != nil {
				return err
			}
			if !innerIter.iter.IsValid() {
				continue
			}

		} else {
			heap.Push(&m.iters, innerIter)
			break
		}
		heap.Push(&m.iters, innerIter)
	}

	if err := m.current.iter.Next(); err != nil {
		return err
	}

	if !m.current.iter.IsValid() {
		if m.iters.Len() > 0 {
			m.current = heap.Pop(&m.iters).(*HeapWrapper)
		}
		return nil
	}

	if m.iters.Len() > 0 {
		innerIterator := heap.Pop(&m.iters).(*HeapWrapper)
		if innerIterator.Compare(m.current) < 0 {
			//println("2inner:", string(innerIterator.iter.Key().Val), innerIterator.idx)
			//println("2curr:", string(m.current.iter.Key().Val), m.current.idx)
			m.current, innerIterator = innerIterator, m.current
		}
		heap.Push(&m.iters, innerIterator)
	}

	return nil
}
