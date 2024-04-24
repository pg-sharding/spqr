package tasks

import (
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
)

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
	TaskPlanned = iota
	TaskSplit
	TaskMoved
)

type JoinType int

const (
	JoinNone = iota
	JoinLeft
	JoinRight
)

type TaskGroup struct {
	Tasks    []*Task
	JoinType JoinType
}

func TaskGroupToProto(group *TaskGroup) *protos.TaskGroup {
	return &protos.TaskGroup{
		Tasks: func() []*protos.Task {
			res := make([]*protos.Task, len(group.Tasks))
			for i, t := range group.Tasks {
				res[i] = TaskToProto(t)
			}
			return res
		}(),
		JoinType: JoinTypeToProto(group.JoinType),
	}
}

func TaskToProto(task *Task) *protos.Task {
	return &protos.Task{
		ShardIdFrom:    task.ShardFromId,
		ShardIdTo:      task.ShardToId,
		KeyRangeIdFrom: task.KrIdFrom,
		KeyRangeIdTo:   task.KrIdTo,
		KeyRangeIdTemp: task.KrIdTemp,
		Bound:          task.Bound,
		Status:         TaskStateToProto(task.State),
	}
}

func TaskStateToProto(state TaskState) protos.TaskStatus {
	switch state {
	case TaskPlanned:
		return protos.TaskStatus_Planned
	case TaskSplit:
		return protos.TaskStatus_Split
	case TaskMoved:
		return protos.TaskStatus_Moved
	default:
		panic("incorrect task state")
	}
}

func TaskStateToStr(state TaskState) string {
	switch state {
	case TaskPlanned:
		return "PLANNED"
	case TaskSplit:
		return "SPLIT"
	case TaskMoved:
		return "MOVED"
	default:
		panic("incorrect task state")
	}
}

func JoinTypeToProto(t JoinType) protos.JoinType {
	switch t {
	case JoinNone:
		return protos.JoinType_JoinNone
	case JoinLeft:
		return protos.JoinType_JoinLeft
	case JoinRight:
		return protos.JoinType_JoinRight
	default:
		panic("incorrect join type")
	}
}

func TaskGroupFromProto(group *protos.TaskGroup) *TaskGroup {
	return &TaskGroup{
		Tasks: func() []*Task {
			res := make([]*Task, len(group.Tasks))
			for i, t := range group.Tasks {
				res[i] = TaskFromProto(t)
			}
			return res
		}(),
		JoinType: JoinTypeFromProto(group.JoinType),
	}
}

func TaskFromProto(task *protos.Task) *Task {
	return &Task{
		ShardFromId: task.ShardIdFrom,
		ShardToId:   task.ShardIdTo,
		KrIdFrom:    task.KeyRangeIdFrom,
		KrIdTo:      task.KeyRangeIdTo,
		KrIdTemp:    task.KeyRangeIdTemp,
		Bound:       task.Bound,
		State:       TaskStateFromProto(task.Status),
	}
}

func TaskStateFromProto(state protos.TaskStatus) TaskState {
	switch state {
	case protos.TaskStatus_Planned:
		return TaskPlanned
	case protos.TaskStatus_Split:
		return TaskSplit
	case protos.TaskStatus_Moved:
		return TaskMoved
	default:
		panic("incorrect task state")
	}
}

func JoinTypeFromProto(t protos.JoinType) JoinType {
	switch t {
	case protos.JoinType_JoinNone:
		return JoinNone
	case protos.JoinType_JoinLeft:
		return JoinLeft
	case protos.JoinType_JoinRight:
		return JoinRight
	default:
		panic("incorrect join type")
	}
}

func TaskGroupToDb(group *TaskGroup) *qdb.TaskGroup {
	return &qdb.TaskGroup{
		Tasks: func() []*qdb.Task {
			res := make([]*qdb.Task, len(group.Tasks))
			for i, task := range group.Tasks {
				res[i] = TaskToDb(task)
			}
			return res
		}(),
		JoinType: int(group.JoinType),
	}
}

func TaskToDb(task *Task) *qdb.Task {
	return &qdb.Task{
		ShardFromId: task.ShardFromId,
		ShardToId:   task.ShardToId,
		KrIdFrom:    task.KrIdFrom,
		KrIdTo:      task.KrIdTo,
		KrIdTemp:    task.KrIdTemp,
		Bound:       task.Bound,
		State:       int(task.State),
	}
}

func TaskGroupFromDb(group *qdb.TaskGroup) *TaskGroup {
	return &TaskGroup{
		Tasks: func() []*Task {
			res := make([]*Task, len(group.Tasks))
			for i, task := range group.Tasks {
				res[i] = TaskFromDb(task)
			}
			return res
		}(),
		JoinType: JoinType(group.JoinType),
	}
}

func TaskFromDb(task *qdb.Task) *Task {
	return &Task{
		ShardFromId: task.ShardFromId,
		ShardToId:   task.ShardToId,
		KrIdFrom:    task.KrIdFrom,
		KrIdTo:      task.KrIdTo,
		KrIdTemp:    task.KrIdTemp,
		Bound:       task.Bound,
		State:       TaskState(task.State),
	}
}
