package sequences

import (
	"github.com/pg-sharding/spqr/qdb"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

type Sequence struct {
	RelName string
	ColName string
}

func SequencesFromSQL(drel *spqrparser.DistributedRelation) []*Sequence {
	seqs := make([]*Sequence, len(drel.Sequences))
	for _, colName := range drel.Sequences {
		seqs = append(seqs, &Sequence{
			RelName: drel.Name,
			ColName: colName,
		})
	}
	return seqs
}

func SequenceFromDB(seq *qdb.Sequence) *Sequence {
	return &Sequence{
		RelName: seq.RelName,
		ColName: seq.ColName,
	}
}
