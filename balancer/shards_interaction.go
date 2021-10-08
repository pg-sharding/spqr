package balancer

type Database struct {}

func (Database) getShardStats(shard Shard, keyRanges []KeyRange) (map[string]map[string]Stats, error) {
	return nil, nil
}

func (Database) startTransfer(task Task) error {
	return nil
}
