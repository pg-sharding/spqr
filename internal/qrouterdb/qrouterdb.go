package qrouterdb

import "github.com/pg-sharding/spqr/yacc/spqrparser"

type ShardKey struct {
	Name string
	RW   bool
}

type QrouterDB interface {
	Lock(keyRange *spqrparser.KeyRange) error
	UnLock(keyRange *spqrparser.KeyRange) error

	Check(key int) bool
}
