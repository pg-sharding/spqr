package balancer

type Spqr struct {}

func (Spqr) lockKeyRange(rng KeyRange) error {
	return nil
}

func (Spqr) unlockKeyRange(rng KeyRange) error {
	return nil
}

func (Spqr) initKeyRanges() error {
	return nil
}

func (Spqr) splitKeyRange(rng KeyRange, border string) error {
	return nil
}

func (Spqr) mergeKeyRanges(leftRng, rightRng KeyRange) error {
	return nil
}
