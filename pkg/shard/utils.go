package shard

// TODO : unit tests
func ShardIDs(shards []Shard) []uint {
	ret := []uint{}
	for _, shard := range shards {
		ret = append(ret, shard.ID())
	}
	return ret
}
