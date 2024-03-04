package provider

type Task struct {
	shardFromId string
	shardToId   string
	krIdFrom    string
	krIdTo      string
	bound       []byte
}

const (
	unificationLeft = iota
	unificationRight
	unificationNone
)

type unificationType int

type TaskGroup struct {
	tasks []*Task
	uType unificationType
}
