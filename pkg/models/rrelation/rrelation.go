package rrelation

import (
	"github.com/pg-sharding/spqr/pkg/models/kr"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/rfqn"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

type ReferenceRelation struct {
	SchemaName            string
	TableName             string
	SchemaVersion         uint64
	ColumnSequenceMapping map[string]string
	ShardIds              []string
}

type AutoIncrementEntry struct {
	Column string
	Start  uint64
}

func (r *ReferenceRelation) ListStorageRoutes() []kr.ShardKey {
	var ret []kr.ShardKey

	for _, id := range r.ShardIds {
		ret = append(ret, kr.ShardKey{
			Name: id,
		})
	}

	return ret
}

func (r *ReferenceRelation) GetSchema() string {
	if r.SchemaName == "" {
		return "public"
	}
	return r.SchemaName
}

func (r *ReferenceRelation) QualifiedName() *rfqn.RelationFQN {
	return &rfqn.RelationFQN{RelationName: r.TableName, SchemaName: r.GetSchema()}
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
		SchemaName:            p.SchemaName,
		SchemaVersion:         p.SchemaVersion,
		ColumnSequenceMapping: p.SequenceColumns,
		ShardIds:              p.ShardIds,
	}
}

func RefRelationToProto(p *ReferenceRelation) *protos.ReferenceRelation {
	return &protos.ReferenceRelation{
		Name:            p.TableName,
		SchemaName:      p.SchemaName,
		SchemaVersion:   p.SchemaVersion,
		SequenceColumns: p.ColumnSequenceMapping,
		ShardIds:        p.ShardIds,
	}
}

func RefRelationToDB(p *ReferenceRelation) *qdb.ReferenceRelation {
	return &qdb.ReferenceRelation{
		TableName:             p.TableName,
		SchemaName:            p.SchemaName,
		SchemaVersion:         p.SchemaVersion,
		ColumnSequenceMapping: p.ColumnSequenceMapping,
		ShardIds:              p.ShardIds,
	}
}

func RefRelationFromDB(p *qdb.ReferenceRelation) *ReferenceRelation {
	return &ReferenceRelation{
		TableName:             p.TableName,
		SchemaName:            p.SchemaName,
		SchemaVersion:         p.SchemaVersion,
		ColumnSequenceMapping: p.ColumnSequenceMapping,
		ShardIds:              p.ShardIds,
	}
}
