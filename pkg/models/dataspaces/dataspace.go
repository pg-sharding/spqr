package dataspaces

import (
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

var (
	ColumnTypeVarchar           = "varchar"
	ColumnTypeVarcharDeprecated = "varchar_d"
	ColumnTypeInteger           = "integer"
)

type ShardedRelation struct {
	Name        string
	ColumnNames []string
}

type Keyspace struct {
	Id        string
	ColTypes  []string
	Relations map[string]ShardedRelation
}

// local table sharding rule -> route to world

func NewDataspace(id string, rels map[string]ShardedRelation) *Keyspace {
	return &Keyspace{
		Id:        id,
		Relations: rels,
	}
}

func ShardedRelationFromDB(sr qdb.ShardedRelation) ShardedRelation {
	return ShardedRelation{
		Name:        sr.Name,
		ColumnNames: sr.ColNames,
	}
}

func (s *Keyspace) ID() string {
	return s.Id
}

func KeyspaceFromSQL(ds *spqrparser.DataspaceDefinition) *Keyspace {
	ret := &Keyspace{
		Id: ds.ID,
	}
	for _, r := range ds.Relations {
		ret.Relations[r.Name] = ShardedRelation{
			Name:        r.Name,
			ColumnNames: r.Columns,
		}
	}

	return ret
}

func DataspaceFromDB(ds *qdb.Dataspace) *Keyspace {
	ks := &Keyspace{
		Id:        ds.ID,
		Relations: map[string]ShardedRelation{},
		ColTypes:  ds.ColTypes,
	}
	for k, v := range ds.Relations {
		ks.Relations[k] = ShardedRelationFromDB(v)
	}
	return ks
}

func KeyspaceFromProto(ds *proto.Dataspace) *Keyspace {
	return &Keyspace{
		Id: ds.Id,
		// ColTypes: ds.ColTypes,
	}
}

func KeyspaceToProto(ds *Keyspace) *proto.Dataspace {
	return &proto.Dataspace{
		Id:       ds.Id,
		Coltypes: ds.ColTypes,
	}
}
