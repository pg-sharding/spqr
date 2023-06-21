package pkg

type ActionStage int64

const (
	actionStagePlan     ActionStage = iota
	actionStageSplit
	actionStageLock
	actionStageTransfer
	actionStageMove
	actionStageUnlock
	actionStageRemove
	actionStageMerge
	actionStageDone
)

type Action struct {
	id          uint64
	actionStage ActionStage
	isRunning   bool

	keyRange  KeyRange
	fromShard Shard
	toShard   Shard
}
