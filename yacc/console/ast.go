package spqrparser

type ColumnRef struct {
	TableAlias string
	ColName    string
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
	Cmd   string
	Where WhereClauseNode
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

type DistributionDefinition struct {
	ID       string
	ColTypes []string
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

type KeyRangeDefinition struct {
	LowerBound   []byte
	ShardID      string
	KeyRangeID   string
	Distribution string
}

type ShardDefinition struct {
	Id    string
	Hosts []string
}

func (*KeyRangeDefinition) iCreate()     {}
func (*ShardDefinition) iCreate()        {}
func (*DistributionDefinition) iCreate() {}
func (*ShardingRuleDefinition) iCreate() {}

type SplitKeyRange struct {
	Border         []byte
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

type KeyRangeSelector struct {
	KeyRangeID string
}

type ShardingRuleSelector struct {
	ID string
}

type DistributionSelector struct {
	ID string
}

type ShardSelector struct {
	ID string
}

type DropRoutersAll struct{}

func (*DropRoutersAll) iStatement() {}

func (*KeyRangeSelector) iDrop()     {}
func (*ShardingRuleSelector) iDrop() {}
func (*DistributionSelector) iDrop() {}
func (*ShardSelector) iDrop()        {}

const (
	EntityRouters      = "ROUTERS"
	EntityKeyRanges    = "KEY_RANGES"
	EntityShardingRule = "SHARDING_RULE"
)

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
	Name            string
	DistributionKey []DistributionKeyEntry
}

type AttachRelation struct {
	Distribution *DistributionSelector
	Relations    []*DistributedRelation
}

func (*AttachRelation) iStatement()         {}
func (*AttachRelation) iAlter()             {}
func (*AttachRelation) iAlterDistribution() {}

type DetachRelation struct {
	Distribution *DistributionSelector
	RelationName string
}

func (*DetachRelation) iStatement()         {}
func (*DetachRelation) iAlter()             {}
func (*DetachRelation) iAlterDistribution() {}

// The frollowing constants represent SHOW statements.
const (
	DatabasesStr          = "databases"
	DistributionsStr      = "distributions"
	RoutersStr            = "routers"
	ShardsStr             = "shards"
	ShardingRules         = "sharding_rules"
	KeyRangesStr          = "key_ranges"
	ClientsStr            = "clients"
	PoolsStr              = "pools"
	BackendConnectionsStr = "backend_connections"
	StatusStr             = "status"
	VersionStr            = "version"
	RelationsStr          = "relations"
	UnsupportedStr        = "unsupported"
)

const (
	ClientStr = "client"
)

// Statement represents a statement.
type Statement interface {
	iStatement()
}

func (*Show) iStatement()                   {}
func (*Set) iStatement()                    {}
func (*KeyRangeSelector) iStatement()       {}
func (*ShardingRuleSelector) iStatement()   {}
func (*DistributionSelector) iStatement()   {}
func (*ShardSelector) iStatement()          {}
func (*Lock) iStatement()                   {}
func (*Unlock) iStatement()                 {}
func (*Shutdown) iStatement()               {}
func (*Listen) iStatement()                 {}
func (*MoveKeyRange) iStatement()           {}
func (*SplitKeyRange) iStatement()          {}
func (*UniteKeyRange) iStatement()          {}
func (*DistributionDefinition) iStatement() {}
func (*ShardingRuleDefinition) iStatement() {}
func (*KeyRangeDefinition) iStatement()     {}
func (*ShardDefinition) iStatement()        {}
func (*Kill) iStatement()                   {}
func (*WhereClauseLeaf) iStatement()        {}
func (*WhereClauseEmpty) iStatement()       {}
func (*WhereClauseOp) iStatement()          {}

func (*RegisterRouter) iStatement()   {}
func (*UnregisterRouter) iStatement() {}
