package provider

type Task struct {
	shardFromId string
	shardToId   string
	krIdFrom    string
	krIdTo      string
	bound       []byte
	tempKRId    string
	state       taskState
}

type taskState int

const (
	taskPlanned = iota
	taskSplit
	taskMoved
)

type unificationType int

const (
	unificationNone = iota
	unificationLeft
	unificationRight
)

type TaskGroup struct {
	tasks       []*Task
	unification unificationType
}
