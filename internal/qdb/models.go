package qdb

type ShardKey struct {
	Name string
	RW   bool
}

type KeyRange struct {
	From       int
	To         int
	ShardID    string
	KeyRangeID string
}
type KeyRangeStatus string

const KRLocked = KeyRangeStatus("LOCKED")
const KRUnLocked = KeyRangeStatus("UNLOCKED")
