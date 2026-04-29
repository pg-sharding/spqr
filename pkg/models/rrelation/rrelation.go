package rrelation

import (
	"github.com/pg-sharding/spqr/pkg/models/acl"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/rfqn"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

type ReferenceRelation struct {
	RelationName          *rfqn.RelationFQN
	SchemaVersion         uint64
	ColumnSequenceMapping map[string]string
	ShardIDs              []string

	Version uint64
	ACL     []acl.ACLItem
}

type AutoIncrementEntry struct {
	Column string
	Start  uint64
}

func (r *ReferenceRelation) ListStorageRoutes() []kr.ShardKey {
	var ret []kr.ShardKey

	for _, id := range r.ShardIDs {
		ret = append(ret, kr.ShardKey{
			Name: id,
		})
	}

	return ret
}
func (r *ReferenceRelation) QualifiedName() *rfqn.RelationFQN {
	return r.RelationName
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
		RelationName:          rfqn.RelationFQNFromProto(p.RelName),
		SchemaVersion:         p.SchemaVersion,
		ColumnSequenceMapping: p.SequenceColumns,
		ShardIDs:              p.ShardIDs,

		Version: p.Version,
		ACL:     acl.ACLFromProto(p.Acl),
	}
}

func RefRelationToProto(p *ReferenceRelation) *protos.ReferenceRelation {
	return &protos.ReferenceRelation{
		RelName:         rfqn.RelationFQNToProto(p.RelationName),
		SchemaVersion:   p.SchemaVersion,
		SequenceColumns: p.ColumnSequenceMapping,
		ShardIDs:        p.ShardIDs,

		Version: p.Version,
		Acl:     acl.ACLTOProto(p.ACL),
	}
}

func RefRelationToDB(p *ReferenceRelation) *qdb.ReferenceRelation {
	return &qdb.ReferenceRelation{
		TableName:             p.RelationName.RelationName,
		SchemaName:            p.RelationName.SchemaName,
		SchemaVersion:         p.SchemaVersion,
		ColumnSequenceMapping: p.ColumnSequenceMapping,
		ShardIDs:              p.ShardIDs,

		Version: p.Version,
		ACL:     acl.ACLTODB(p.ACL),
	}
}

func RefRelationFromDB(p *qdb.ReferenceRelation) *ReferenceRelation {
	return &ReferenceRelation{
		RelationName:          rfqn.RelationFQNFromFullName(p.SchemaName, p.TableName),
		SchemaVersion:         p.SchemaVersion,
		ColumnSequenceMapping: p.ColumnSequenceMapping,
		ShardIDs:              p.ShardIDs,

		Version: p.Version,
		ACL:     acl.ACLFromDB(p.ACL),
	}
}
