package tasks

import (
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
)

type Task struct {
	shardFromId string
	shardToId   string
	krIdFrom    string
	krIdTo      string
	bound       []byte
	tempKRId    string
	state       TaskState
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
	tasks    []*Task
	joinType JoinType
}

func TaskGroupToProto(group *TaskGroup) *protos.TaskGroup {
	return &protos.TaskGroup{
		Tasks: func() []*protos.Task {
			res := make([]*protos.Task, len(group.tasks))
			for i, t := range group.tasks {
				res[i] = TaskToProto(t)
			}
			return res
		}(),
		JoinType: JoinTypeToProto(group.joinType),
	}
}

func TaskToProto(task *Task) *protos.Task {
	return &protos.Task{
		ShardIdFrom:    task.shardFromId,
		ShardIdTo:      task.shardToId,
		KeyRangeIdFrom: task.krIdFrom,
		KeyRangeIdTo:   task.krIdTo,
		KeyRangeIdTemp: task.tempKRId,
		Bound:          task.bound,
		Status:         TaskStateToProto(task.state),
	}
}

func TaskStateToProto(state TaskState) protos.TaskStatus {
	switch state {
	case taskPlanned:
		return protos.TaskStatus_Planned
	case taskSplit:
		return protos.TaskStatus_Split
	case taskMoved:
		return protos.TaskStatus_Moved
	default:
		panic("incorrect task state")
	}
}

func JoinTypeToProto(t JoinType) protos.JoinType {
	switch t {
	case joinNone:
		return protos.JoinType_JoinNone
	case joinLeft:
		return protos.JoinType_JoinLeft
	case joinRight:
		return protos.JoinType_JoinRight
	default:
		panic("incorrect join type")
	}
}

func TaskGroupFromProto(group *protos.TaskGroup) *TaskGroup {
	return &TaskGroup{
		tasks: func() []*Task {
			res := make([]*Task, len(group.Tasks))
			for i, t := range group.Tasks {
				res[i] = TaskFromProto(t)
			}
			return res
		}(),
		joinType: JoinTypeFromProto(group.JoinType),
	}
}

func TaskFromProto(task *protos.Task) *Task {
	return &Task{
		shardFromId: task.ShardIdFrom,
		shardToId:   task.ShardIdTo,
		krIdFrom:    task.KeyRangeIdFrom,
		krIdTo:      task.KeyRangeIdTo,
		tempKRId:    task.KeyRangeIdTemp,
		bound:       task.Bound,
		state:       TaskStateFromProto(task.Status),
	}
}

func TaskStateFromProto(state protos.TaskStatus) TaskState {
	switch state {
	case protos.TaskStatus_Planned:
		return taskPlanned
	case protos.TaskStatus_Split:
		return taskSplit
	case protos.TaskStatus_Moved:
		return taskMoved
	default:
		panic("incorrect task state")
	}
}

func JoinTypeFromProto(t protos.JoinType) JoinType {
	switch t {
	case protos.JoinType_JoinNone:
		return joinNone
	case protos.JoinType_JoinLeft:
		return joinLeft
	case protos.JoinType_JoinRight:
		return joinRight
	default:
		panic("incorrect join type")
	}
}

func TaskGroupToDb(group *TaskGroup) *qdb.TaskGroup {
	return &qdb.TaskGroup{
		Tasks: func() []*qdb.Task {
			res := make([]*qdb.Task, len(group.tasks))
			for i, task := range group.tasks {
				res[i] = TaskToDb(task)
			}
			return res
		}(),
		JoinType: int(group.joinType),
	}
}

func TaskToDb(task *Task) *qdb.Task {
	return &qdb.Task{
		ShardFromId: task.shardFromId,
		ShardToId:   task.shardToId,
		KrIdFrom:    task.krIdFrom,
		KrIdTo:      task.krIdTo,
		KrIdTemp:    task.tempKRId,
		Bound:       task.bound,
		State:       int(task.state),
	}
}

func TaskGroupFromDb(group *qdb.TaskGroup) *TaskGroup {
	return &TaskGroup{
		tasks: func() []*Task {
			res := make([]*Task, len(group.Tasks))
			for i, task := range group.Tasks {
				res[i] = TaskFromDb(task)
			}
			return res
		}(),
		joinType: JoinType(group.JoinType),
	}
}

func TaskFromDb(task *qdb.Task) *Task {
	return &Task{
		shardFromId: task.ShardFromId,
		shardToId:   task.ShardToId,
		krIdFrom:    task.KrIdFrom,
		krIdTo:      task.KrIdTo,
		tempKRId:    task.KrIdTemp,
		bound:       task.Bound,
		state:       TaskState(task.State),
	}
}
