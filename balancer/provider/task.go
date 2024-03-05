package provider

type Task struct {
	ShardFromId string
	ShardToId   string
	KrIdFrom    string
	KrIdTo      string
	Bound       []byte
	KrIdTemp    string
	State       TaskState
}

type TaskState int

const (
	taskPlanned = iota
	taskSplit
	taskMoved
)

type JoinType int

const (
	joinNone = iota
	joinLeft
	joinRight
)

type TaskGroup struct {
	Tasks    []*Task
	JoinType JoinType
}
