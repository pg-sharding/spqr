package spqrparser

import (
	"time"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/router/rfqn"
)

type ColumnRef struct {
	TableAlias string
	ColName    string
}
type OptAscDesc any

type SortByDefault struct {
	OptAscDesc
}
type SortByAsc struct {
	OptAscDesc
}
type SortByDesc struct {
	OptAscDesc
}
type OrderClause any

type Order struct {
	OrderClause
	OptAscDesc OptAscDesc
	Col        ColumnRef
}

type GroupByClause any

type GroupByClauseEmpty struct {
	GroupByClause
}

type GroupBy struct {
	GroupByClause
	Col []ColumnRef
}
type Show struct {
	Cmd     string
	Columns []string
	Where   lyx.Node
	Order   OrderClause
	GroupBy GroupByClause
}

type Help struct {
	CommandName string
}

func (*Help) iStatement() {}

type Set struct {
	Element Statement
}

type Create struct {
	Element Statement
}

func (*Create) iStatement() {}

type Drop struct {
	Element       Statement
	CascadeDelete bool
}

func (*Drop) iStatement() {}

type CreateStmt interface {
	iCreate()
}

type TraceStmt struct {
	Client uint
	All    bool
}

func (*TraceStmt) iStatement() {}

type StopTraceStmt struct {
}

func (*StopTraceStmt) iStatement() {}

type DropStmt interface {
	iDrop()
}

type DistributionDefinition struct {
	ID                   string
	ColTypes             []string
	Replicated           bool
	AutoIncrementEntries []AutoIncrementEntry
	DefaultShard         string
}

type ReferenceRelationDefinition struct {
	TableName            *rfqn.RelationFQN
	AutoIncrementEntries []*AutoIncrementEntry
	ShardIds             []string
}

type UniqueIndexDefinition struct {
	ID        string
	TableName *rfqn.RelationFQN
	Columns   []TypedColRef
}

type AutoIncrementEntry struct {
	Column string
	Start  uint64
}

type KeyRangeBound struct {
	Pivots [][]byte
}

type KeyRangeDefinition struct {
	LowerBound   *KeyRangeBound
	ShardID      string
	KeyRangeID   string
	Distribution *DistributionSelector
}

type ShardDefinition struct {
	Id    string
	Hosts []string
}

func (*KeyRangeDefinition) iCreate()          {}
func (*ShardDefinition) iCreate()             {}
func (*DistributionDefinition) iCreate()      {}
func (*ReferenceRelationDefinition) iCreate() {}
func (*UniqueIndexDefinition) iCreate()       {}

type SplitKeyRange struct {
	Border         *KeyRangeBound
	KeyRangeFromID string
	KeyRangeID     string
}

type UniteKeyRange struct {
	KeyRangeIDL string
	KeyRangeIDR string
}

type MoveKeyRange struct {
	DestShardID string
	KeyRangeID  string
}

type RedistributeKeyRange struct {
	DestShardID string
	KeyRangeID  string
	BatchSize   int
	Id          string
	Check       bool
	Apply       bool
	NoWait      bool
}

type KeyRangeSelector struct {
	KeyRangeID string
}

type DistributionSelector struct {
	ID string
}

type ReferenceRelationSelector struct {
	ID string
}

type UniqueIndexSelector struct {
	ID string
}

type ShardSelector struct {
	ID string
}

type TaskGroupSelector struct {
	ID string
}

type MoveTaskSelector struct {
	ID string
}

type RedistributeTaskSelector struct {
	ID string
}

func (*KeyRangeSelector) iDrop()          {}
func (*DistributionSelector) iDrop()      {}
func (*ReferenceRelationSelector) iDrop() {}
func (*UniqueIndexSelector) iDrop()       {}
func (*ShardSelector) iDrop()             {}
func (*TaskGroupSelector) iDrop()         {}
func (*MoveTaskSelector) iDrop()          {}
func (*RedistributeTaskSelector) iDrop()  {}

type Lock struct {
	KeyRangeID string
}

type Unlock struct {
	KeyRangeID string
}

type Listen struct {
	addr string
}
type Shutdown struct{}

type Kill struct {
	Cmd    string
	Target uint
}

type InvalidateCacheTarget string

const (
	StaleClientsInvalidateTarget       = InvalidateCacheTarget("STALE CLIENTS")
	BackendConnectionsInvalidateTarget = InvalidateCacheTarget("BACKENDS")
	SchemaCacheInvalidateTarget        = InvalidateCacheTarget("SCHEMA CACHE")
)

type Invalidate struct {
	Target InvalidateCacheTarget
}

type SyncReferenceTables struct {
	ShardID          string
	RelationSelector string
}

// coordinator

type RegisterRouter struct {
	Addr string
	ID   string
}

type UnregisterRouter struct {
	ID string
}

type AlterStmt interface {
	iAlter()
}

type Alter struct {
	Element Statement
}

func (*Alter) iStatement() {}

type DistributionAlterStatement interface {
	AlterStmt
	iAlterDistribution()
}

type AlterDistribution struct {
	Distribution *DistributionSelector
	Element      Statement
}

func (*AlterDistribution) iStatement()         {}
func (*AlterDistribution) iAlter()             {}
func (*AlterDistribution) iAlterDistribution() {}

type TypedColRef struct {
	Column string
	Type   string
}

type DistributionKeyEntry struct {
	Column       string
	HashFunction string
	Expr         []TypedColRef
}

type DistributedRelation struct {
	Relation             *rfqn.RelationFQN
	DistributionKey      []DistributionKeyEntry
	ReplicatedRelation   bool
	AutoIncrementEntries []*AutoIncrementEntry
}

type AttachRelation struct {
	Relations []*DistributedRelation
}

func (*AttachRelation) iStatement()         {}
func (*AttachRelation) iAlter()             {}
func (*AttachRelation) iAlterDistribution() {}

type AlterRelation struct {
	Relation *DistributedRelation
}

func (*AlterRelation) iStatement()         {}
func (*AlterRelation) iAlter()             {}
func (*AlterRelation) iAlterDistribution() {}

type AlterRelationV2 struct {
	RelationName *rfqn.RelationFQN
	Element      RelationAlterStmt
}

func (*AlterRelationV2) iStatement()         {}
func (*AlterRelationV2) iAlter()             {}
func (*AlterRelationV2) iAlterDistribution() {}

type RelationAlterStmt interface {
	iAlterRelation()
	iStatement()
}

type AlterRelationSchema struct {
	SchemaName string
}

func (*AlterRelationSchema) iStatement()     {}
func (*AlterRelationSchema) iAlterRelation() {}

type AlterRelationDistributionKey struct {
	DistributionKey []DistributionKeyEntry
}

func (*AlterRelationDistributionKey) iStatement()     {}
func (*AlterRelationDistributionKey) iAlterRelation() {}

type DetachRelation struct {
	RelationName *rfqn.RelationFQN
}

func (*DetachRelation) iStatement()         {}
func (*DetachRelation) iAlter()             {}
func (*DetachRelation) iAlterDistribution() {}

type AlterDefaultShard struct {
	Shard string
}

func (*AlterDefaultShard) iStatement()         {}
func (*AlterDefaultShard) iAlter()             {}
func (*AlterDefaultShard) iAlterDistribution() {}

type DropDefaultShard struct {
}

func (*DropDefaultShard) iStatement()         {}
func (*DropDefaultShard) iAlter()             {}
func (*DropDefaultShard) iAlterDistribution() {}

type SequenceSelector struct {
	Name string
}

func (*SequenceSelector) iDrop() {}

type RetryMoveTaskGroup struct {
	ID string
}

func (*RetryMoveTaskGroup) iStatement() {}

type StopMoveTaskGroup struct {
	ID string
}

func (*StopMoveTaskGroup) iStatement() {}

type ICPointAction struct {
	Act     string
	Timeout time.Duration /* for act = 'sleep' */
}

type InstanceControlPoint struct {
	Name   string
	Enable bool
	A      *ICPointAction
}

func (*InstanceControlPoint) iStatement() {}

// The following constants represent SHOW statements.
const (
	DatabasesStr          = "databases"
	DistributionsStr      = "distributions"
	CoordinatorAddrStr    = "coordinator_address"
	RoutersStr            = "routers"
	ShardsStr             = "shards"
	HostsStr              = "hosts"
	ShardingRules         = "sharding_rules"
	KeyRangesStr          = "key_ranges"
	KeyRangesExtendedStr  = "key_ranges_extended"
	ClientsStr            = "clients"
	PoolsStr              = "pools"
	InstanceStr           = "instance"
	BackendConnectionsStr = "backend_connections"
	StatusStr             = "status"
	VersionStr            = "version"
	RelationsStr          = "relations"
	TaskGroupStr          = "task_group"
	TaskGroupsStr         = "task_groups"
	PreparedStatementsStr = "prepared_statements"
	ReferenceRelationsStr = "reference_relations"
	UnsupportedStr        = "unsupported"
	QuantilesStr          = "time_quantiles"
	SequencesStr          = "sequences"
	IsReadOnlyStr         = "is_read_only"
	MoveStatsStr          = "move_stats"
	TsaCacheStr           = "tsa_cache"
	Users                 = "users"
	MoveTaskStr           = "move_task"
	MoveTasksStr          = "move_tasks"
	UniqueIndexesStr      = "unique_indexes"
	TaskGroupExtendedStr  = "task_group_ext"
	TaskGroupsExtendedStr = "task_groups_ext"
	RedistributeTasksStr  = "redistribute_tasks"
	ErrorStr              = "errors"
)

// not SHOW target
const (
	ClientStr  = "client"
	BackendStr = "backend"
)

const (
	//key range for default shard
	DEFAULT_KEY_RANGE_SUFFIX = "DEFAULT"
)

// Statement represents a statement.
type Statement interface {
	iStatement()
}

func (*Show) iStatement()                        {}
func (*Set) iStatement()                         {}
func (*KeyRangeSelector) iStatement()            {}
func (*DistributionSelector) iStatement()        {}
func (*ReferenceRelationSelector) iStatement()   {}
func (*UniqueIndexSelector) iStatement()         {}
func (*ShardSelector) iStatement()               {}
func (*TaskGroupSelector) iStatement()           {}
func (*MoveTaskSelector) iStatement()            {}
func (*RedistributeTaskSelector) iStatement()    {}
func (*SequenceSelector) iStatement()            {}
func (*Lock) iStatement()                        {}
func (*Unlock) iStatement()                      {}
func (*Shutdown) iStatement()                    {}
func (*Listen) iStatement()                      {}
func (*MoveKeyRange) iStatement()                {}
func (*RedistributeKeyRange) iStatement()        {}
func (*SplitKeyRange) iStatement()               {}
func (*UniteKeyRange) iStatement()               {}
func (*DistributionDefinition) iStatement()      {}
func (*ReferenceRelationDefinition) iStatement() {}
func (*UniqueIndexDefinition) iStatement()       {}
func (*KeyRangeDefinition) iStatement()          {}
func (*ShardDefinition) iStatement()             {}
func (*Kill) iStatement()                        {}
func (*Invalidate) iStatement()                  {}
func (*SyncReferenceTables) iStatement()         {}

func (*RegisterRouter) iStatement()   {}
func (*UnregisterRouter) iStatement() {}
