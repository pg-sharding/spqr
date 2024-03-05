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

type joinType int

const (
	joinNone = iota
	joinLeft
	joinRight
)

type TaskGroup struct {
	tasks    []*Task
	joinType joinType
}
