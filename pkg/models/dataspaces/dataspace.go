package dataspaces

import (
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

type ShardedRelation struct {
	Name        string
	ColumnNames []string
}

type Dataspace struct {
	Id        string
	ColTypes  []string
	Relations []ShardedRelation
}

// local table sharding rule -> route to world

func NewDataspace(id string, rels []ShardedRelation) *Dataspace {
	return &Dataspace{
		Id:        id,
		Relations: rels,
	}
}

func (s *Dataspace) ID() string {
	return s.Id
}

func DataspaceFromSQL(ds *spqrparser.DataspaceDefinition) *Dataspace {
	ret := &Dataspace{
		Id: ds.ID,
	}
	for _, r := range ds.Relations {
		ret.Relations = append(ret.Relations, ShardedRelation{
			Name:        r.Name,
			ColumnNames: r.Columns,
		})
	}

	return ret
}

func DataspaceFromDB(ds *qdb.Dataspace) *Dataspace {
	return &Dataspace{
		Id: ds.ID,
		// Relations: rule.Relations,
		// ColTypes:  rule.ColTypes,
	}
}

func DataspaceFromProto(ds *proto.Dataspace) *Dataspace {
	return &Dataspace{
		Id: ds.Id,
		// ColTypes: ds.ColTypes,
	}
}
func DataspaceToProto(ds *Dataspace) *proto.Dataspace {
	return &proto.Dataspace{
		Id: ds.Id,
	}
}
