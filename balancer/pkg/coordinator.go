package pkg

type CoordinatorInterface interface {
	initKeyRanges() (map[Shard][]KeyRange, error)
	isReloadRequired() (bool, error)

	lockKeyRange(rng KeyRange) error
	unlockKeyRange(rng KeyRange) error

	splitKeyRange(border *string) error
	mergeKeyRanges(border *string) error
	moveKeyRange(rng KeyRange, shardFrom, shardTo Shard) error
}

type Coordinator struct {

}

func (c Coordinator) initKeyRanges() (map[Shard][]KeyRange, error) {
	panic("implement me")
}

func (c Coordinator) isReloadRequired() (bool, error) {
	panic("implement me")
}

func (c Coordinator) lockKeyRange(rng KeyRange) error {
	panic("implement me")
}

func (c Coordinator) unlockKeyRange(rng KeyRange) error {
	panic("implement me")
}

func (c Coordinator) splitKeyRange(border *string) error {
	panic("implement me")
}

func (c Coordinator) mergeKeyRanges(border *string) error {
	panic("implement me")
}

func (c Coordinator) moveKeyRange(rng KeyRange, shardFrom, shardTo Shard) error {
	panic("implement me")
}
