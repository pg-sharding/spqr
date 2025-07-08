package shard

// ShardIDs returns a slice of shard IDs extracted from the given list of shards.
// It takes a list of shards as input and returns a slice of shard IDs.
//
// Parameters:
//   - shards: A list of shards to extract the IDs from.
//
// Returns:
//   - []uint: A slice of shard IDs.
func ShardIDs(shards []ShardHostInstance) []uint {
	ret := []uint{}
	for _, shard := range shards {
		ret = append(ret, shard.ID())
	}
	return ret
}
