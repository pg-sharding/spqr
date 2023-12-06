package shard

func ShardIDs(shards []Shard) []string {
	ret := []string{}
	for _, shard := range shards {
		ret = append(ret, shard.ID())
	}
	return ret
}