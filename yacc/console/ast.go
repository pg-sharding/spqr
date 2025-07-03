package spqrparser

type ColumnRef struct {
	TableAlias string
	ColName    string
}
type OptAscDesc interface{}

type SortByDefault struct {
	OptAscDesc
}
type SortByAsc struct {
	OptAscDesc
}
type SortByDesc struct {
	OptAscDesc
}
type OrderClause interface{}

type Order struct {
	OrderClause
	OptAscDesc OptAscDesc
	Col        ColumnRef
}

type GroupByClause interface{}

type GroupByClauseEmpty struct {
	GroupByClause
}

type GroupBy struct {
	GroupByClause
	Col ColumnRef
}

type WhereClauseNode interface {
}

type WhereClauseEmpty struct {
	WhereClauseNode
}

type WhereClauseLeaf struct {
	WhereClauseNode

	Op     string
	ColRef ColumnRef
	Value  string
}

type WhereClauseOp struct {
	WhereClauseNode

	Op    string
	Left  WhereClauseNode
	Right WhereClauseNode
}

type Show struct {
	Cmd     string
	Where   WhereClauseNode
	Order   OrderClause
	GroupBy GroupByClause
}

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

type QualifiedName struct {
	Name       string
	SchemaName string
}

func (n QualifiedName) String() string {
	if len(n.SchemaName) < 1 {
		return n.Name
	}
	return n.SchemaName + "." + n.Name
}

type DistributionDefinition struct {
	ID                   string
	ColTypes             []string
	Replicated           bool
	AutoIncrementEntries []AutoIncrementEntry
	DefaultShard         string
}

type ShardingRuleDefinition struct {
	ID           string
	TableName    string
	Entries      []ShardingRuleEntry
	Distribution string
}

type ShardingRuleEntry struct {
	Column       string
	HashFunction string
}

type ReferenceRelationDefinition struct {
	TableName            string
	AutoIncrementEntries []*AutoIncrementEntry
	ShardIds             []string
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
	Distribution string
}

type ShardDefinition struct {
	Id    string
	Hosts []string
}

func (*KeyRangeDefinition) iCreate()          {}
func (*ShardDefinition) iCreate()             {}
func (*DistributionDefinition) iCreate()      {}
func (*ShardingRuleDefinition) iCreate()      {}
func (*ReferenceRelationDefinition) iCreate() {}

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
	Check       bool
	Apply       bool
}

type KeyRangeSelector struct {
	KeyRangeID string
}

type ShardingRuleSelector struct {
	ID string
}

type DistributionSelector struct {
	ID string
}

type ReferenceRelationSelector struct {
	ID string
}

type ShardSelector struct {
	ID string
}

type TaskGroupSelector struct{}

func (*KeyRangeSelector) iDrop()          {}
func (*ShardingRuleSelector) iDrop()      {}
func (*DistributionSelector) iDrop()      {}
func (*ReferenceRelationSelector) iDrop() {}
func (*ShardSelector) iDrop()             {}
func (*TaskGroupSelector) iDrop()         {}

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

type InvalidateCache struct{}

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
	Element Statement
}

func (*AlterDistribution) iStatement()         {}
func (*AlterDistribution) iAlter()             {}
func (*AlterDistribution) iAlterDistribution() {}

type DistributionKeyEntry struct {
	Column       string
	HashFunction string
}

type DistributedRelation struct {
	Name                 string
	SchemaName           string
	DistributionKey      []DistributionKeyEntry
	ReplicatedRelation   bool
	AutoIncrementEntries []*AutoIncrementEntry
}

type AttachRelation struct {
	Distribution *DistributionSelector
	Relations    []*DistributedRelation
}

func (*AttachRelation) iStatement()         {}
func (*AttachRelation) iAlter()             {}
func (*AttachRelation) iAlterDistribution() {}

type AlterRelation struct {
	Distribution *DistributionSelector
	Relation     *DistributedRelation
}

func (*AlterRelation) iStatement()         {}
func (*AlterRelation) iAlter()             {}
func (*AlterRelation) iAlterDistribution() {}

type DetachRelation struct {
	Distribution *DistributionSelector
	RelationName *QualifiedName
}

func (*DetachRelation) iStatement()         {}
func (*DetachRelation) iAlter()             {}
func (*DetachRelation) iAlterDistribution() {}

type AlterDefaultShard struct {
	Distribution *DistributionSelector
	Shard        string
}

func (*AlterDefaultShard) iStatement()         {}
func (*AlterDefaultShard) iAlter()             {}
func (*AlterDefaultShard) iAlterDistribution() {}

type DropDefaultShard struct {
	Distribution *DistributionSelector
}

func (*DropDefaultShard) iStatement()         {}
func (*DropDefaultShard) iAlter()             {}
func (*DropDefaultShard) iAlterDistribution() {}

type SequenceSelector struct {
	Name string
}

func (*SequenceSelector) iDrop() {}

type RetryMoveTaskGroup struct{}

func (*RetryMoveTaskGroup) iStatement() {}

// The following constants represent SHOW statements.
const (
	DatabasesStr          = "databases"
	DistributionsStr      = "distributions"
	CoordinatorAddrStr    = "coordinator_address"
	RoutersStr            = "routers"
	ShardsStr             = "shards"
	ShardingRules         = "sharding_rules"
	KeyRangesStr          = "key_ranges"
	ClientsStr            = "clients"
	PoolsStr              = "pools"
	InstanceStr           = "instance"
	BackendConnectionsStr = "backend_connections"
	StatusStr             = "status"
	VersionStr            = "version"
	RelationsStr          = "relations"
	TaskGroupStr          = "task_group"
	PreparedStatementsStr = "prepared_statements"
	ReferenceRelationsStr = "reference_relations"
	UnsupportedStr        = "unsupported"
	QuantilesStr          = "time_quantiles"
	SequencesStr          = "sequences"
	IsReadOnlyStr         = "is_read_only"
	MoveStatsStr          = "move_stats"
)

const (
	ClientStr = "client"
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
func (*ShardingRuleSelector) iStatement()        {}
func (*DistributionSelector) iStatement()        {}
func (*ReferenceRelationSelector) iStatement()   {}
func (*ShardSelector) iStatement()               {}
func (*TaskGroupSelector) iStatement()           {}
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
func (*ShardingRuleDefinition) iStatement()      {}
func (*KeyRangeDefinition) iStatement()          {}
func (*ShardDefinition) iStatement()             {}
func (*Kill) iStatement()                        {}
func (*WhereClauseLeaf) iStatement()             {}
func (*WhereClauseEmpty) iStatement()            {}
func (*WhereClauseOp) iStatement()               {}
func (*InvalidateCache) iStatement()             {}
func (*SyncReferenceTables) iStatement()         {}

func (*RegisterRouter) iStatement()   {}
func (*UnregisterRouter) iStatement() {}
