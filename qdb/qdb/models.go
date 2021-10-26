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
}
