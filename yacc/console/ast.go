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

type ShardedRelaion struct {
	Name    string
	Columns []string
}

type DataspaceDefinition struct {
	ID        string
	ColTypes  []string
	Relations []*ShardedRelaion
}

type KeyRangeDefinition struct {
	LowerBound []byte
	UpperBound []byte
	ShardID    string
	KeyRangeID string
	Dataspace  string
}

type ShardDefinition struct {
	Id    string
	Hosts []string
}

func (*KeyRangeDefinition) iCreate()  {}
func (*ShardDefinition) iCreate()     {}
func (*DataspaceDefinition) iCreate() {}

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

type DataspaceSelector struct {
	ID string
}

type DropRoutersAll struct{}

func (*DropRoutersAll) iStatement() {}

func (*KeyRangeSelector) iDrop()  {}
func (*DataspaceSelector) iDrop() {}

const (
	EntityRouters   = "ROUTERS"
	EntityKeyRanges = "KEY_RANGES"
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

type AttachTable struct {
	Relation  *ShardedRelaion
	Dataspace *DataspaceSelector
}

// The frollowing constants represent SHOW statements.
const (
	DatabasesStr          = "databases"
	DataspacesStr         = "dataspaces"
	RoutersStr            = "routers"
	ShardsStr             = "shards"
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

func (*Show) iStatement()                {}
func (*Set) iStatement()                 {}
func (*KeyRangeSelector) iStatement()    {}
func (*DataspaceSelector) iStatement()   {}
func (*Lock) iStatement()                {}
func (*Unlock) iStatement()              {}
func (*Shutdown) iStatement()            {}
func (*Listen) iStatement()              {}
func (*MoveKeyRange) iStatement()        {}
func (*SplitKeyRange) iStatement()       {}
func (*UniteKeyRange) iStatement()       {}
func (*DataspaceDefinition) iStatement() {}
func (*KeyRangeDefinition) iStatement()  {}
func (*ShardDefinition) iStatement()     {}
func (*Kill) iStatement()                {}
func (*WhereClauseLeaf) iStatement()     {}
func (*WhereClauseEmpty) iStatement()    {}
func (*WhereClauseOp) iStatement()       {}

func (*RegisterRouter) iStatement()   {}
func (*UnregisterRouter) iStatement() {}

func (*AttachTable) iStatement() {}
