package pkg

type Task struct {
	keyRange  KeyRange
	shardFrom Shard
	shardTo   Shard

	startKeyRange KeyRange
	splitBy       []string

	oldKeyRangesOnShardFrom map[KeyRange]Stats
	oldKeyRangesOnShardTo   map[KeyRange]Stats
	newKeyRangesOnShardFrom map[KeyRange]Stats
	newKeyRangesOnShardTo   map[KeyRange]Stats

	profit float64
}

type TasksByProfitIncrease []Task

func (a TasksByProfitIncrease) Len() int           { return len(a) }
func (a TasksByProfitIncrease) Less(i, j int) bool { return a[i].profit < a[j].profit }
func (a TasksByProfitIncrease) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
