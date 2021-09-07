package qrouterdb

import "github.com/pg-sharding/spqr/yacc/spqrparser"

type QrouterDB interface {
	Lock(keyRange *spqrparser.KeyRange) error
	UnLock(keyRange *spqrparser.KeyRange) error

	Add(keyRange *spqrparser.KeyRange) error
	Update(keyRange *spqrparser.KeyRange) error

	Begin() error
	Commit() error

	Check(key int) bool
}
