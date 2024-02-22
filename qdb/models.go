package qdb

type ShardKey struct {
	Name string
	RW   bool
}

type KeyRange struct {
	LowerBound     []byte `json:"from"`
	UpperBound     []byte `json:"to"`
	ShardID        string `json:"shard_id"`
	KeyRangeID     string `json:"key_range_id"`
	DistributionId string `json:"distribution_id"`
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

var (
	ColumnTypeVarchar  = "varchar"
	ColumnTypeInteger  = "integer"
	ColumnTypeUinteger = "uinteger"
)

type DistributionKeyEntry struct {
	Column       string `json:"column"`
	HashFunction string `json:"hash"`
}

type DistributedRelation struct {
	Name            string                 `json:"name"`
	DistributionKey []DistributionKeyEntry `json:"column_names"`
}

type Distribution struct {
	ID       string   `json:"id"`
	ColTypes []string `json:"col_types,omitempty"`

	Relations map[string]*DistributedRelation `json:"relations"`
}

func NewDistribution(id string, coltypes []string) *Distribution {
	distr := &Distribution{
		ID:        id,
		ColTypes:  coltypes,
		Relations: map[string]*DistributedRelation{},
	}

	return distr
}
