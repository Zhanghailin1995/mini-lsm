package mini_lsm

type CompactionType int

const (
	NoCompaction CompactionType = iota
	Leveled
	Tiered
	Simple
)

type CompactionOptions struct {
	compactionType CompactionType
	opt            interface{}
}
