package main

import "math/big"

type InstallationInterface interface {
	getShardStats(shard Shard, keyRanges []KeyRange) (map[string]map[string]Stats, error)
	startTransfer(task Action) error
	removeRange(keyRange KeyRange, shard Shard) error
	getKeyDistanceByRanges(shard Shard, keyRanges []KeyRange) (map[string]*big.Int, error)
}

type Installation struct{}

func (Installation) getShardStats(shard Shard, keyRanges []KeyRange) (map[string]map[string]Stats, error) {
	return nil, nil
}

func (Installation) startTransfer(task Action) error {
	return nil
}

func (Installation) removeRange(keyRange KeyRange, shard Shard) error {
	return nil
}

func (Installation) getKeyDistanceByRanges(shard Shard, keyRanges []KeyRange) (map[string]*big.Int, error) {
	return nil, nil
}
