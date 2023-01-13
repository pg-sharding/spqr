package qdb

type KeyRange struct {
	LowerBound []byte `json:"from"`
	UpperBound []byte `json:"to"`
	ShardID    string `json:"shard_id"`
	KeyRangeID string `json:"key_range_id"`
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
	Id      string      `json:"id"`
	State   RouterState `json:"state,omitempty"`
}

func NewRouter(addr, id string, rst RouterState) *Router {
	return &Router{
		Address: addr,
		Id:      id,
		State:   rst,
	}
}

func (r Router) Addr() string {
	return r.Address
}

func (r Router) ID() string {
	return r.Id
}

type ShardingRule struct {
	Colnames []string `json:"columns"`
	Id       string   `json:"id"`
}

type Shard struct {
	ID string `json:"id"`
	Hosts []string `json:"hosts"`
}

func NewShard(ID string, hosts []string) *Shard {
	return &Shard{
		ID:    ID,
		Hosts: hosts,
	}
}
