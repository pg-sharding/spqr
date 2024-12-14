package xqdbseq

import (
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/sequencer"
)

type XQDBSeq struct {
	qdb  qdb.XQDB
	name string
}

// NextVal implements sequencer.SeqAM.
func (x *XQDBSeq) NextVal() (int64, error) {
	return x.qdb.NextVal(x.name)
}

// Read implements sequencer.SeqAM.
func (x *XQDBSeq) Read() (int64, error) {
	return x.qdb.GetVal(x.name)
}

func NewXQDBSeq(qdb qdb.XQDB, name string) sequencer.SeqAM {
	return &XQDBSeq{
		name: name,
		qdb:  qdb,
	}
}
