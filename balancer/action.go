package main

type ActionStage int64

const (
	plan     ActionStage = 0
	split                = 1
	lock                 = 2
	transfer             = 3
	move                 = 4
	unlock               = 5
	remove               = 6
	merge                = 7
	done                 = 7
)

type Action struct {
	id          uint64
	actionStage ActionStage
	isRunning   bool

	keyRange  KeyRange
	fromShard Shard
	toShard   Shard
}
