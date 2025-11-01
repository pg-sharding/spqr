package tasks

import (
	"fmt"

	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
)

type MoveTask struct {
	ID          string    `json:"id"`
	Bound       [][]byte  `json:"bound"`
	KrIdTemp    string    `json:"kr_id_temp"`
	State       TaskState `json:"state"`
	TaskGroupID string    `json:"task_group_id"`
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

type SplitType int

const (
	SplitLeft = iota
	SplitRight
)

type MoveTaskGroup struct {
	ID          string    `json:"id"`
	ShardToId   string    `json:"shard_to_id"`
	KrIdFrom    string    `json:"kr_id_from"`
	KrIdTo      string    `json:"kr_id_to"`
	Type        SplitType `json:"type"`
	BoundRel    string    `json:"bound_rel"`
	Coeff       float64   `json:"coeff"`
	BatchSize   int64     `json:"batch_size"`
	Limit       int64     `json:"limit"`
	TotalKeys   int64     `json:"total_keys"`
	CurrentTask *MoveTask `json:"task"`
}

type RedistributeTaskState int

const (
	RedistributeTaskPlanned = iota
	RedistributeTaskMoved
)

type RedistributeTask struct {
	KrId      string
	ShardId   string
	BatchSize int
	TempKrId  string
	State     RedistributeTaskState
}

// TaskGroupToProto converts a MoveTaskGroup object to its corresponding protobuf representation.
// It creates a new protos.MoveTaskGroup object and copies the values from the input object to the output object.
//
// Parameters:
//   - group: The MoveTaskGroup object to convert.
//
// Returns:
//   - *protos.MoveTaskGroup: The converted protos.MoveTaskGroup object.
func TaskGroupToProto(group *MoveTaskGroup) *protos.MoveTaskGroup {
	if group == nil {
		return nil
	}
	return &protos.MoveTaskGroup{
		ID:             group.ID,
		CurrentTask:    MoveTaskToProto(group.CurrentTask),
		Type:           SplitTypeToProto(group.Type),
		ShardIdTo:      group.ShardToId,
		KeyRangeIdFrom: group.KrIdFrom,
		KeyRangeIdTo:   group.KrIdTo,
		Coeff:          group.Coeff,
		Limit:          group.Limit,
		TotalKeys:      group.TotalKeys,
		BatchSize:      group.BatchSize,
		BoundRel:       group.BoundRel,
	}
}

// MoveTaskToProto converts a MoveTask struct to a protos.MoveTask struct.
// It creates a new protos.MoveTask object and copies the values from the input object to the output object.
//
// Parameters:
//   - task: The MoveTask object to convert.
//
// Returns:
//   - *protos.MoveTask: The converted protos.MoveTask object.
func MoveTaskToProto(task *MoveTask) *protos.MoveTask {
	if task == nil {
		return nil
	}
	return &protos.MoveTask{
		ID:             task.ID,
		KeyRangeIdTemp: task.KrIdTemp,
		Bound:          task.Bound,
		Status:         TaskStateToProto(task.State),
		TaskGroupID:    task.TaskGroupID,
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

// SplitTypeToProto converts a SplitType to its corresponding protos.SplitType.
// It takes a SplitType as input and returns the corresponding protos.SplitType.
// If the input SplitType is not recognized, it panics with an "incorrect split type" error.
//
// Parameters:
//   - t: The SplitType value to convert.
//
// Returns:
//   - protos.SplitType: The converted protos.SplitType value.
func SplitTypeToProto(t SplitType) protos.SplitType {
	switch t {
	case SplitLeft:
		return protos.SplitType_SplitLeft
	case SplitRight:
		return protos.SplitType_SplitRight
	default:
		panic("incorrect split type")
	}
}

// TaskGroupFromProto converts a protos.MoveTaskGroup object to a MoveTaskGroup object.
// It creates a new MoveTaskGroup object and copies the values from the input object to the output object.
//
// Parameters:
//   - group: The protos.MoveTaskGroup object to convert.
//
// Returns:
//   - *MoveTaskGroup: The converted MoveTaskGroup object.
func TaskGroupFromProto(group *protos.MoveTaskGroup) *MoveTaskGroup {
	if group == nil {
		return nil
	}
	return &MoveTaskGroup{
		ID:          group.ID,
		CurrentTask: MoveTaskFromProto(group.CurrentTask),
		Type:        SplitTypeFromProto(group.Type),
		ShardToId:   group.ShardIdTo,
		KrIdFrom:    group.KeyRangeIdFrom,
		KrIdTo:      group.KeyRangeIdTo,
		Coeff:       group.Coeff,
		Limit:       group.Limit,
		TotalKeys:   group.TotalKeys,
		BatchSize:   group.BatchSize,
		BoundRel:    group.BoundRel,
	}
}

// MoveTaskFromProto converts a protos.MoveTask object to a MoveTask object.
// It creates a new MoveTask object and copies the values from the input object to the output object.
//
// Parameters:
//   - task: The protos.MoveTask object to convert.
//
// Returns:
//   - *MoveTask: The converted MoveTask object.
func MoveTaskFromProto(task *protos.MoveTask) *MoveTask {
	if task == nil {
		return nil
	}
	return &MoveTask{
		ID:          task.ID,
		KrIdTemp:    task.KeyRangeIdTemp,
		Bound:       task.Bound,
		State:       TaskStateFromProto(task.Status),
		TaskGroupID: task.TaskGroupID,
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

// SplitTypeFromProto converts a protos.JoinType to a SplitType.
// It maps the given protos.SplitType to the corresponding SplitType value.
// If the given protos.SplitType is not recognized, it panics with an "incorrect split type" error.
//
// Parameters:
//   - t: The protos.SplitType value to convert.
//
// Returns:
//   - SplitType: The converted SplitType value.
func SplitTypeFromProto(t protos.SplitType) SplitType {
	switch t {
	case protos.SplitType_SplitLeft:
		return SplitLeft
	case protos.SplitType_SplitRight:
		return SplitRight
	default:
		panic("incorrect split type")
	}
}

// TaskGroupToDb converts a MoveTaskGroup object to a qdb.MoveTaskGroup object.
// It creates a new qdb.MoveTaskGroup object and populates its fields based on the provided MoveTaskGroup object.
// The Tasks field is populated by converting each MoveTask object in the group.Tasks slice to a qdb.MoveTask object using the TaskToDb function.
// The JoinType field is set to the integer value of the group.JoinType.
// The converted qdb.MoveTaskGroup object is returned.
//
// Parameters:
//   - group: The MoveTaskGroup object to convert.
//
// Returns:
//   - *qdb.MoveTaskGroup: The converted qdb.MoveTaskGroup object.
func TaskGroupToDb(group *MoveTaskGroup) *qdb.MoveTaskGroup {
	if group == nil {
		return nil
	}
	return &qdb.MoveTaskGroup{
		Type:      int(group.Type),
		ShardToId: group.ShardToId,
		KrIdFrom:  group.KrIdFrom,
		KrIdTo:    group.KrIdTo,
		BoundRel:  group.BoundRel,
		Coeff:     group.Coeff,
		BatchSize: group.BatchSize,
		Limit:     group.Limit,
	}
}

// MoveTaskToDb converts a MoveTask struct to a qdb.MoveTask struct.
// It takes a pointer to a MoveTask struct as input and returns a pointer to a qdb.MoveTask struct.
// The function creates a new qdb.MoveTask object and copies the values from the input MoveTask object to the output qdb.MoveTask object.
//
// Parameters:
//   - task: The MoveTask object to convert.
//
// Returns:
//   - *qdb.MoveTask: The converted qdb.MoveTask object.
func MoveTaskToDb(task *MoveTask) *qdb.MoveTask {
	if task == nil {
		return nil
	}
	return &qdb.MoveTask{
		ID:          task.ID,
		KrIdTemp:    task.KrIdTemp,
		Bound:       task.Bound,
		State:       int(task.State),
		TaskGroupID: task.TaskGroupID,
	}
}

// TaskGroupFromDb converts a database task group to a MoveTaskGroup struct.
// It takes a pointer to a qdb.MoveTaskGroup as input and returns a pointer to a MoveTaskGroup.
// The function creates a new MoveTaskGroup object and populates its fields based on the provided qdb.MoveTaskGroup object.
// The Tasks field is populated by converting each qdb.MoveTask object in the group.Tasks slice to a MoveTask object using the TaskFromDb function.
// The JoinType field is set to the JoinType value of the qdb.MoveTaskGroup object.
//
// Parameters:
//   - group: The qdb.MoveTaskGroup object to convert.
//
// Returns:
//   - *MoveTaskGroup: The converted MoveTaskGroup object.
func TaskGroupFromDb(group *qdb.MoveTaskGroup, moveTask *qdb.MoveTask, totalKeys int64) *MoveTaskGroup {
	if group == nil {
		return nil
	}
	return &MoveTaskGroup{
		Type:        SplitType(group.Type),
		ShardToId:   group.ShardToId,
		KrIdFrom:    group.KrIdFrom,
		KrIdTo:      group.KrIdTo,
		BoundRel:    group.BoundRel,
		Coeff:       group.Coeff,
		BatchSize:   group.BatchSize,
		Limit:       group.Limit,
		CurrentTask: TaskFromDb(moveTask),
		TotalKeys:   totalKeys,
	}
}

// TaskFromDb converts a qdb.MoveTask object to a MoveTask object.
// It takes a pointer to a qdb.MoveTask as input and returns a pointer to a MoveTask.
// The function creates a new MoveTask object and copies the values from the input qdb.MoveTask object to the output MoveTask object.
//
// Parameters:
//   - task: The qdb.MoveTask object to convert.
//
// Returns:
//   - *MoveTask: The converted MoveTask object.
func TaskFromDb(task *qdb.MoveTask) *MoveTask {
	if task == nil {
		return nil
	}
	return &MoveTask{
		ID:          task.ID,
		KrIdTemp:    task.KrIdTemp,
		Bound:       task.Bound,
		State:       TaskState(task.State),
		TaskGroupID: task.TaskGroupID,
	}
}

type BalancerTaskState int

const (
	BalancerTaskPlanned = iota
	BalancerTaskMoved
)

type BalancerTask struct {
	Type      JoinType
	KrIdFrom  string
	KrIdTo    string
	KrIdTemp  string
	ShardIdTo string
	KeyCount  int64
	State     BalancerTaskState
}

func BalancerTaskFromProto(task *protos.BalancerTask) *BalancerTask {
	if task == nil {
		return nil
	}
	return &BalancerTask{
		State:     BalancerTaskStateFromProto(task.State),
		KrIdFrom:  task.KeyRangeIdFrom,
		KrIdTo:    task.KeyRangeIdTo,
		KrIdTemp:  task.KeyRangeIdTemp,
		ShardIdTo: task.ShardIdTo,
		KeyCount:  task.KeyCount,
		Type:      JoinTypeFromProto(task.Type),
	}
}

func BalancerTaskToProto(task *BalancerTask) *protos.BalancerTask {
	if task == nil {
		return nil
	}
	return &protos.BalancerTask{
		State:          BalancerTaskStateToProto(task.State),
		KeyRangeIdFrom: task.KrIdFrom,
		KeyRangeIdTo:   task.KrIdTo,
		KeyRangeIdTemp: task.KrIdTemp,
		ShardIdTo:      task.ShardIdTo,
		KeyCount:       task.KeyCount,
		Type:           JoinTypeToProto(task.Type),
	}
}

func BalancerTaskStateFromProto(state protos.BalancerTaskStatus) BalancerTaskState {
	switch state {
	case protos.BalancerTaskStatus_BalancerTaskPlanned:
		return BalancerTaskPlanned
	case protos.BalancerTaskStatus_BalancerTaskMoved:
		return BalancerTaskMoved
	default:
		panic("unknown balancer task status")
	}
}

func BalancerTaskStateToProto(state BalancerTaskState) protos.BalancerTaskStatus {
	switch state {
	case BalancerTaskPlanned:
		return protos.BalancerTaskStatus_BalancerTaskPlanned
	case BalancerTaskMoved:
		return protos.BalancerTaskStatus_BalancerTaskMoved
	default:
		panic("unknown balancer task status")
	}
}

func BalancerTaskToDb(task *BalancerTask) *qdb.BalancerTask {
	if task == nil {
		return nil
	}
	return &qdb.BalancerTask{
		Type:      int(task.Type),
		KrIdFrom:  task.KrIdFrom,
		KrIdTo:    task.KrIdTo,
		KrIdTemp:  task.KrIdTemp,
		ShardIdTo: task.ShardIdTo,
		KeyCount:  task.KeyCount,
		State:     int(task.State),
	}
}

func BalancerTaskFromDb(task *qdb.BalancerTask) *BalancerTask {
	if task == nil {
		return nil
	}
	return &BalancerTask{
		Type: func() JoinType {
			switch task.Type {
			case JoinLeft:
				return JoinLeft
			case JoinRight:
				return JoinRight
			case JoinNone:
				return JoinNone
			default:
				panic(fmt.Sprintf("incorrect join type: \"%d\"", task.Type))
			}
		}(),
		KrIdFrom:  task.KrIdFrom,
		KrIdTo:    task.KrIdTo,
		KrIdTemp:  task.KrIdTemp,
		ShardIdTo: task.ShardIdTo,
		KeyCount:  task.KeyCount,
		State: func() BalancerTaskState {
			switch task.State {
			case BalancerTaskPlanned:
				return BalancerTaskPlanned
			case BalancerTaskMoved:
				return BalancerTaskMoved
			default:
				panic(fmt.Sprintf("incorrect balancer task state: \"%d\"", task.State))
			}
		}(),
	}
}

func RedistributeTaskToProto(task *RedistributeTask) *protos.RedistributeTask {
	return &protos.RedistributeTask{
		KeyRangeId: task.KrId,
		ShardId:    task.ShardId,
		BatchSize:  int64(task.BatchSize),
		State:      RedistributeTaskStateToProto(task.State),
	}
}

func RedistributeTaskFromProto(task *protos.RedistributeTask) *RedistributeTask {
	return &RedistributeTask{
		KrId:      task.KeyRangeId,
		ShardId:   task.ShardId,
		BatchSize: int(task.BatchSize),
		State:     RedistributeTaskStateFromProto(task.State),
	}
}

func RedistributeTaskToDB(task *RedistributeTask) *qdb.RedistributeTask {
	return &qdb.RedistributeTask{
		KrId:      task.KrId,
		ShardId:   task.ShardId,
		BatchSize: task.BatchSize,
		State:     int(task.State),
	}
}

func RedistributeTaskFromDB(task *qdb.RedistributeTask) *RedistributeTask {
	return &RedistributeTask{
		KrId:      task.KrId,
		ShardId:   task.ShardId,
		BatchSize: task.BatchSize,
		State: func() RedistributeTaskState {
			switch task.State {
			case RedistributeTaskPlanned:
				return RedistributeTaskPlanned
			case RedistributeTaskMoved:
				return RedistributeTaskMoved
			default:
				panic("unknown redistribute task type")
			}
		}(),
	}
}

func RedistributeTaskStateToProto(state RedistributeTaskState) protos.RedistributeTaskState {
	switch state {
	case RedistributeTaskPlanned:
		return protos.RedistributeTaskState_RedistributeTaskPlanned
	case RedistributeTaskMoved:
		return protos.RedistributeTaskState_RedistributeTaskMoved
	default:
		panic("unknown redistribute task state")
	}
}

func RedistributeTaskStateFromProto(state protos.RedistributeTaskState) RedistributeTaskState {
	switch state {
	case protos.RedistributeTaskState_RedistributeTaskPlanned:
		return RedistributeTaskPlanned
	case protos.RedistributeTaskState_RedistributeTaskMoved:
		return RedistributeTaskMoved
	default:
		panic("unknown redistribute task state")
	}
}
