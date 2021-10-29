package qdb

type QrouterDB interface {
	Lock(keyRangeID string) (*KeyRange, error)
	UnLock(keyRangeID string) error

	Add(keyRange *KeyRange) error
	Update(keyRange *KeyRange) error

	Begin() error
	Commit() error

	AddRouter(r *Router) error
	Check(kr *KeyRange) bool

	Watch(krid string, status *KeyRangeStatus, notifyio chan<- interface{}) error
	ListRouters() ([]*Router, error)
	DropKeyRange(krl *KeyRange) error
}
