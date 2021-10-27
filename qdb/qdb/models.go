package qdb

type ShardKey struct {
	Name string
	RW   bool
}

type KeyRange struct {
	From       []byte
	To         []byte
	ShardId    string
	KeyRangeID string
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
