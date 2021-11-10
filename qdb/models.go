package qdb

type ShardKey struct {
	Name string
	RW   bool
}

type KeyRange struct {
	LowerBound []byte `json:"from"`
	UpperBound []byte `json:"to"`
	ShardID    string `json:"shard_id"`
	KeyRangeID string `json:"key_range_id"`
}
type KeyRangeStatus string

const KRLocked = KeyRangeStatus("LOCKED")
const KRUnLocked = KeyRangeStatus("UNLOCKED")

type Router struct {
	addr string
	id   string
}

func NewRouter(addr, id string) *Router {
	return &Router{
		addr: addr,
		id:   id,
	}
}

func (r Router) Addr() string {
	return r.addr
}

func (r Router) ID() string {
	return r.id
}
