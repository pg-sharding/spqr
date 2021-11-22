package main

import "math/big"

type Spqr struct {}

func (Spqr) lockKeyRange(rng KeyRange) error {
	return nil
}

func (Spqr) unlockKeyRange(rng KeyRange) error {
	return nil
}

func (Spqr) initKeyRanges() (map[Shard][]KeyRange, map[KeyRange]big.Int, error) {
	return nil, nil, nil
}

func (Spqr) splitKeyRange(rng KeyRange, border string) error {
	return nil
}

func (Spqr) mergeKeyRanges(leftRng, rightRng KeyRange) error {
	return nil
}

func (Spqr) shouldReloadRanges() (bool, error) {
	return false, nil
}