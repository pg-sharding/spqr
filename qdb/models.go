package qdb

type ShardKey struct {
	Name string
	RW   bool
}

type KeyRange struct {
	LowerBound  []byte `json:"from"`
	UpperBound  []byte `json:"to"`
	ShardID     string `json:"shard_id"`
	KeyRangeID  string `json:"key_range_id"`
	DataspaceId string `json:"dataspace_id"`
}
type MoveKeyRangeStatus string

const (
	MoveKeyRangePlanned  = MoveKeyRangeStatus("PLANNED")
	MoveKeyRangeStarted  = MoveKeyRangeStatus("STARTED")
	MoveKeyRangeComplete = MoveKeyRangeStatus("COMPLETE")
)

type MoveKeyRange struct {
	MoveId     string             `json:"move_id"`
	ShardId    string             `json:"shard_id"`
	KeyRangeID string             `json:"key_range_id"`
	Status     MoveKeyRangeStatus `json:"status"`
}

type KeyRangeStatus string

const KRLocked = KeyRangeStatus("LOCKED")
const KRUnLocked = KeyRangeStatus("UNLOCKED")

type RouterState string

const (
	CLOSED = RouterState("CLOSED")
	OPENED = RouterState("OPENED")
)

type Router struct {
	Address string      `json:"address"`
	ID      string      `json:"id"`
	State   RouterState `json:"state,omitempty"`
}

func NewRouter(addr, id string, rst RouterState) *Router {
	return &Router{
		Address: addr,
		ID:      id,
		State:   rst,
	}
}

func (r Router) Addr() string {
	return r.Address
}

type ShardingRuleEntry struct {
	Column       string `json:"column"`
	HashFunction string `json:"hash"`
}

type ShardingRule struct {
	ID          string              `json:"id"`
	TableName   string              `json:"table"`
	Entries     []ShardingRuleEntry `json:"columns"`
	DataspaceId string              `json:"dataspace_id"`
}

type Shard struct {
	ID    string   `json:"id"`
	Hosts []string `json:"hosts"`
}

func NewShard(ID string, hosts []string) *Shard {
	return &Shard{
		ID:    ID,
		Hosts: hosts,
	}
}

type ShardingColumnType string

var (
	ColumnTypeVarchar = ShardingColumnType("varchar")
	ColumnTypeInteger = ShardingColumnType("integer")
)

type ShardedRelation struct {
	Name     string   `json:"name"`
	ColNames []string `json:"column_names"`
}

type Dataspace struct {
	ID        string                     `json:"id"`
	ColTypes  []ShardingColumnType       `json:"col_types,omitempty"`
	Relations map[string]ShardedRelation `json:"relations"`
}
