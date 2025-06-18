package rrelation

import (
	"github.com/pg-sharding/spqr/pkg/models/kr"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

type ReferenceRelation struct {
	TableName             string
	SchemaVersion         uint64
	ColumnSequenceMapping map[string]string
	ShardId               []string
}

type AutoIncrementEntry struct {
	Column string
	Start  uint64
}

func (r *ReferenceRelation) ListStorageRoutes() []*kr.ShardKey {
	var ret []*kr.ShardKey

	for _, id := range r.ShardId {
		ret = append(ret, &kr.ShardKey{
			Name: id,
		})
	}

	return ret
}

func ReferenceRelationEntriesFromSQL(inEntries []*spqrparser.AutoIncrementEntry) []*AutoIncrementEntry {
	var ret []*AutoIncrementEntry

	for _, e := range inEntries {
		ret = append(ret, &AutoIncrementEntry{
			Column: e.Column,
			Start:  e.Start,
		})
	}

	return ret
}

func AutoIncrementEntriesToProto(inEntries []*AutoIncrementEntry) []*protos.AutoIncrementEntry {
	var ret []*protos.AutoIncrementEntry

	for _, e := range inEntries {
		ret = append(ret, &protos.AutoIncrementEntry{
			ColName:    e.Column,
			StartValue: e.Start,
		})
	}

	return ret
}

func AutoIncrementEntriesFromProto(inEntries []*protos.AutoIncrementEntry) []*AutoIncrementEntry {
	var ret []*AutoIncrementEntry

	for _, e := range inEntries {
		ret = append(ret, &AutoIncrementEntry{
			Column: e.ColName,
			Start:  e.StartValue,
		})
	}

	return ret
}

func RefRelationFromProto(p *protos.ReferenceRelation) *ReferenceRelation {
	return &ReferenceRelation{
		TableName:             p.Name,
		SchemaVersion:         p.SchemaVersion,
		ColumnSequenceMapping: p.SequenceColumns,
		ShardId:               p.ShardIds,
	}
}

func RefRelationToProto(p *ReferenceRelation) *protos.ReferenceRelation {
	return &protos.ReferenceRelation{
		Name:            p.TableName,
		SchemaVersion:   p.SchemaVersion,
		SequenceColumns: p.ColumnSequenceMapping,
		ShardIds:        p.ShardId,
	}
}

func RefRelationToDB(p *ReferenceRelation) *qdb.ReferenceRelation {
	return &qdb.ReferenceRelation{
		TableName:             p.TableName,
		SchemaVersion:         p.SchemaVersion,
		ColumnSequenceMapping: p.ColumnSequenceMapping,
		ShardIds:              p.ShardId,
	}
}

func RefRelationFromDB(p *qdb.ReferenceRelation) *ReferenceRelation {
	return &ReferenceRelation{
		TableName:             p.TableName,
		SchemaVersion:         p.SchemaVersion,
		ColumnSequenceMapping: p.ColumnSequenceMapping,
		ShardId:               p.ShardIds,
	}
}
