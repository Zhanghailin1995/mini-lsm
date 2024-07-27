package main

import (
	"bufio"
	"flag"
	"fmt"
	mini_lsm "github.com/Zhanghailin1995/mini-lsm"
	"github.com/Zhanghailin1995/mini-lsm/utils"
	"os"
	"strconv"
	"strings"
)

func main() {

	dbPath := flag.String("path", "lsm.db", "lsm db path")
	compaction := flag.String("compaction", "leveled", "lsm compaction strategy")
	enableWal := flag.Bool("enable_wal", false, "enable write ahead log")
	serializable := flag.Bool("serializable", false, "serializable transaction")
	flag.Parse()
	println(*dbPath, *compaction, *enableWal, *serializable)

	var compactionOptions *mini_lsm.CompactionOptions
	if *compaction == "none" {
		compactionOptions = &mini_lsm.CompactionOptions{
			CompactionType: mini_lsm.NoCompaction,
			Opt:            nil,
		}
	} else if *compaction == "leveled" {
		compactionOptions = &mini_lsm.CompactionOptions{
			CompactionType: mini_lsm.Leveled,
			Opt: &mini_lsm.LeveledCompactionOptions{
				Level0FileNumCompactionTrigger: 4,
				MaxLevels:                      4,
				BaseLevelSizeMb:                128,
				LevelSizeMultiplier:            2,
			},
		}
	} else if *compaction == "tiered" {
		compactionOptions = &mini_lsm.CompactionOptions{
			CompactionType: mini_lsm.Tiered,
			Opt: &mini_lsm.TieredCompactionOptions{
				NumTiers:                    3,
				MaxSizeAmplificationPercent: 200,
				SizeRatio:                   1,
				MinMergeWidth:               2,
			},
		}
	} else if *compaction == "simple" {
		compactionOptions = &mini_lsm.CompactionOptions{
			CompactionType: mini_lsm.Simple,
			Opt: &mini_lsm.SimpleLeveledCompactionOptions{
				Level0FileNumCompactionTrigger: 2,
				MaxLevels:                      4,
				SizeRatioPercent:               200,
			},
		}
	} else {
		panic("invalid compaction strategy")
	}

	lsm := utils.Unwrap(mini_lsm.Open(*dbPath, &mini_lsm.LsmStorageOptions{
		BlockSize:           4096,
		TargetSstSize:       2 << 20,
		NumberMemTableLimit: 3,
		CompactionOptions:   compactionOptions,
		EnableWal:           *enableWal,
		Serializable:        *serializable,
	}))

	epoch := 0

	for {
		reader := bufio.NewReader(os.Stdin)
		line := utils.Unwrap(reader.ReadString('\n'))
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "fill ") {
			options := strings.SplitN(line, " ", 2)
			if options == nil {
				println("invalid fill command")
				continue
			}
			beginAndEnd := strings.SplitN(options[1], " ", 2)
			if beginAndEnd == nil {
				println("invalid fill command")
				continue
			}
			begin := utils.Unwrap(strconv.Atoi(beginAndEnd[0]))
			end := utils.Unwrap(strconv.Atoi(beginAndEnd[1]))

			for i := begin; i <= end; i++ {
				utils.UnwrapError(lsm.Put([]byte(strconv.Itoa(i)), []byte(fmt.Sprintf("value%d@%d", i, epoch))))
			}
			fmt.Printf("%d values filled with epoch %d\r\n", end-begin, epoch)
		} else if strings.HasPrefix(line, "quit") {
			break
		} else if strings.HasPrefix(line, "get ") {
			options := strings.SplitN(line, " ", 2)
			if options == nil {
				println("invalid get command")
				continue
			}
			key := []byte(options[1])
			value := utils.Unwrap(lsm.Get(key))
			if value == nil {
				fmt.Printf("%s not found\r\n", key)
			} else {
				fmt.Printf("%s=%s\r\n", key, string(value))
			}
		} else if strings.HasPrefix(line, "flush") {
			utils.UnwrapError(lsm.ForceFlush())
			println("flush success")
		} else if strings.HasPrefix(line, "scan ") {
			rest := strings.SplitN(line, " ", 2)
			if rest == nil {
				println("invalid scan command")
				continue
			}
			beginAndEnd := strings.SplitN(rest[1], " ", 2)
			if beginAndEnd == nil {
				println("invalid fill command")
				continue
			}
			begin := beginAndEnd[0]
			end := beginAndEnd[1]
			iter, err := lsm.Scan(mini_lsm.IncludeBytes([]byte(begin)), mini_lsm.IncludeBytes([]byte(end)))
			if err != nil {
				println("scan error")
				continue
			}
			cnt := 0
			for iter.IsValid() {
				cnt++
				fmt.Printf("%s=%s\r\n", string(iter.Key().Val), string(iter.Value()))
				utils.UnwrapError(iter.Next())
			}
			fmt.Printf("%d keys scanned\r\n", cnt)
		} else if strings.HasPrefix(line, "full_compaction") {
			utils.UnwrapError(lsm.ForceFullCompaction())
			println("full compaction success")
		}
		epoch++
	}
	err := lsm.Shutdown()
	if err != nil {
		panic(err)
	}

}
