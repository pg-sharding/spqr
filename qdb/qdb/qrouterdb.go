package qdb

type QrouterDB interface {
	Lock(keyRange *KeyRange) error
	UnLock(keyRange *KeyRange) error

	Add(keyRange *KeyRange) error
	Update(keyRange *KeyRange) error

	Begin() error
	Commit() error

	Check(kr *KeyRange) bool

	Watch(krid string, status *KeyRangeStatus, notifyio chan<- interface{}) error
}
