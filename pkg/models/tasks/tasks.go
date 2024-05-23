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

// TaskGroupToProto converts a TaskGroup object to its corresponding protobuf representation.
// It creates a new protos.TaskGroup object and copies the values from the input object to the output object.
//
// Parameters:
//   - group: The TaskGroup object to convert.
//
// Returns:
//   - *protos.TaskGroup: The converted protos.TaskGroup object.
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

// TaskToProto converts a Task struct to a protos.Task struct.
// It creates a new protos.Task object and copies the values from the input object to the output object.
//
// Parameters:
//   - task: The Task object to convert.
//
// Returns:
//   - *protos.Task: The converted protos.Task object.
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

// TaskStateToProto converts a TaskState value to its corresponding protos.TaskStatus value.
// It takes a TaskState as input and returns the corresponding protos.TaskStatus value.
// If the input TaskState is not recognized, it panics with an "incorrect task state" error.
//
// Parameters:
//   - state: The TaskState value to convert.
//
// Returns:
//   - protos.TaskStatus: The converted protos.TaskStatus value.
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

// TaskStateToStr converts a TaskState value to its corresponding string representation.
// It takes a TaskState as input and returns the corresponding string representation.
// If the input TaskState is not recognized, it panics with an "incorrect task state" error.
//
// Parameters:
//   - state: The TaskState value to convert.
//
// Returns:
//   - string: The string representation of the TaskState value.
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

// JoinTypeToProto converts a JoinType to its corresponding protos.JoinType.
// It takes a JoinType as input and returns the corresponding protos.JoinType.
// If the input JoinType is not recognized, it panics with an "incorrect join type" error.
//
// Parameters:
//   - t: The JoinType value to convert.
//
// Returns:
//   - protos.JoinType: The converted protos.JoinType value.
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

// TaskGroupFromProto converts a protos.TaskGroup object to a TaskGroup object.
// It creates a new TaskGroup object and copies the values from the input object to the output object.
//
// Parameters:
//   - group: The protos.TaskGroup object to convert.
//
// Returns:
//   - *TaskGroup: The converted TaskGroup object.
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

// TaskFromProto converts a protos.Task object to a Task object.
// It creates a new Task object and copies the values from the input object to the output object.
//
// Parameters:
//   - task: The protos.Task object to convert.
//
// Returns:
//   - *Task: The converted Task object.
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

// TaskStateFromProto converts a protos.TaskStatus value to a TaskState value.
// It maps the given protos.TaskStatus to the corresponding TaskState value.
// If the given protos.TaskStatus is not recognized, it panics with an "incorrect task state" error.
//
// Parameters:
//   - state: The protos.TaskStatus value to convert.
//
// Returns:
//   - TaskState: The converted TaskState value.
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

// JoinTypeFromProto converts a protos.JoinType to a JoinType.
// It maps the given protos.JoinType to the corresponding JoinType value.
// If the given protos.JoinType is not recognized, it panics with an "incorrect join type" error.
//
// Parameters:
//   - t: The protos.JoinType value to convert.
//
// Returns:
//   - JoinType: The converted JoinType value.
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

// TaskGroupToDb converts a TaskGroup object to a qdb.TaskGroup object.
// It creates a new qdb.TaskGroup object and populates its fields based on the provided TaskGroup object.
// The Tasks field is populated by converting each Task object in the group.Tasks slice to a qdb.Task object using the TaskToDb function.
// The JoinType field is set to the integer value of the group.JoinType.
// The converted qdb.TaskGroup object is returned.
//
// Parameters:
//   - group: The TaskGroup object to convert.
//
// Returns:
//   - *qdb.TaskGroup: The converted qdb.TaskGroup object.
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

// TaskToDb converts a Task struct to a qdb.Task struct.
// It takes a pointer to a Task struct as input and returns a pointer to a qdb.Task struct.
// The function creates a new qdb.Task object and copies the values from the input Task object to the output qdb.Task object.
//
// Parameters:
//   - task: The Task object to convert.
//
// Returns:
//   - *qdb.Task: The converted qdb.Task object.
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

// TaskGroupFromDb converts a database task group to a TaskGroup struct.
// It takes a pointer to a qdb.TaskGroup as input and returns a pointer to a TaskGroup.
// The function creates a new TaskGroup object and populates its fields based on the provided qdb.TaskGroup object.
// The Tasks field is populated by converting each qdb.Task object in the group.Tasks slice to a Task object using the TaskFromDb function.
// The JoinType field is set to the JoinType value of the qdb.TaskGroup object.
//
// Parameters:
//   - group: The qdb.TaskGroup object to convert.
//
// Returns:
//   - *TaskGroup: The converted TaskGroup object.
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

// TaskFromDb converts a qdb.Task object to a Task object.
// It takes a pointer to a qdb.Task as input and returns a pointer to a Task.
// The function creates a new Task object and copies the values from the input qdb.Task object to the output Task object.
//
// Parameters:
//   - task: The qdb.Task object to convert.
//
// Returns:
//   - *Task: The converted Task object.
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
