package pkg

type ActionStage int64

const (
	actionStagePlan     ActionStage = 0
	actionStageSplit                = 1
	actionStageLock                 = 2
	actionStageTransfer             = 3
	actionStageMove                 = 4
	actionStageUnlock               = 5
	actionStageRemove               = 6
	actionStageMerge                = 7
	actionStageDone                 = 7
)

type Action struct {
	id          uint64
	actionStage ActionStage
	isRunning   bool

	keyRange  KeyRange
	fromShard Shard
	toShard   Shard
}
