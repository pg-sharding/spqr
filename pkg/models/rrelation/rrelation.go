package rrelation

import spqrparser "github.com/pg-sharding/spqr/yacc/console"

type ReferenceRelation struct {
	Name                  string
	SchemaVersion         uint64
	ColumnSequenceMapping map[string]string
	ShardId               []string
}

type AutoIncrementEntry struct {
	Column string
	Start  uint64
}

func ReferenceRelationEntriesFromDB(inEntries []*spqrparser.AutoIncrementEntry) []*AutoIncrementEntry {
	var ret []*AutoIncrementEntry

	for _, e := range inEntries {
		ret = append(ret, &AutoIncrementEntry{
			Column: e.Column,
			Start:  e.Start,
		})
	}

	return ret
}
