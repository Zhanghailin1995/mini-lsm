package mini_lsm

import "fmt"

func (lsm *LsmStorageInner) DumpStructure() {
	snapshot := ReadLsmStorageState(lsm, func(state *LsmStorageState) *LsmStorageState {
		return state
	})

	if len(snapshot.l0SsTables) > 0 {
		fmt.Printf("L0: (%d): %v\n", len(snapshot.l0SsTables), snapshot.l0SsTables)
	}
	for _, level := range snapshot.levels {
		fmt.Printf("L%d: (%d): %v\n", level.level, len(level.ssTables), level.ssTables)
	}
}

func (lsm *MiniLsm) DumpStructure() {
	lsm.inner.DumpStructure()
}
