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

type Create struct {
	Element Statement
}

func (*Create) iStatement() {}

type Drop struct {
	Element Statement
}

func (*Drop) iStatement() {}

type CreateStmt interface {
	iCreate()
}

type TraceStmt struct {
	ClientID string
	All      bool
	Stop     bool
}

func (*TraceStmt) iStatement() {}

type StopTraceStmt struct {
}

func (*StopTraceStmt) iStatement() {}

type DropStmt interface {
	iDrop()
}

type DataspaceDefinition struct {
	ID string
}

type ShardingRuleDefinition struct {
	ID        string
	TableName string
	Entries   []ShardingRuleEntry
}

type ShardingRuleEntry struct {
	Column       string
	HashFunction string
}

type KeyRangeDefinition struct {
	LowerBound []byte
	UpperBound []byte
	ShardID    string
	KeyRangeID string
}

type ShardDefinition struct {
	Id    string
	Hosts []string
}

func (*KeyRangeDefinition) iCreate()     {}
func (*ShardDefinition) iCreate()        {}
func (*DataspaceDefinition) iCreate()    {}
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

type DropRoutersAll struct{}

func (*DropRoutersAll) iStatement() {}

func (*KeyRangeSelector) iDrop()     {}
func (*ShardingRuleSelector) iDrop() {}

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
	Target string
}

// coordinator

type RegisterRouter struct {
	Addr string
	ID   string
}

type UnregisterRouter struct {
	ID string
}

// The frollowing constants represent SHOW statements.
const (
	DatabasesStr          = "databases"
	RoutersStr            = "routers"
	ShardsStr             = "shards"
	ShardingRules         = "sharding_rules"
	KeyRangesStr          = "key_ranges"
	ClientsStr            = "clients"
	PoolsStr              = "pools"
	BackendConnectionsStr = "backend_connections"
	StatusStr             = "status"
	VersionStr            = "version"
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
func (*KeyRangeSelector) iStatement()       {}
func (*ShardingRuleSelector) iStatement()   {}
func (*Lock) iStatement()                   {}
func (*Unlock) iStatement()                 {}
func (*Shutdown) iStatement()               {}
func (*Listen) iStatement()                 {}
func (*MoveKeyRange) iStatement()           {}
func (*SplitKeyRange) iStatement()          {}
func (*UniteKeyRange) iStatement()          {}
func (*DataspaceDefinition) iStatement()    {}
func (*ShardingRuleDefinition) iStatement() {}
func (*KeyRangeDefinition) iStatement()     {}
func (*ShardDefinition) iStatement()        {}
func (*Kill) iStatement()                   {}
func (*WhereClauseLeaf) iStatement()        {}
func (*WhereClauseEmpty) iStatement()       {}
func (*WhereClauseOp) iStatement()          {}

func (*RegisterRouter) iStatement()   {}
func (*UnregisterRouter) iStatement() {}
