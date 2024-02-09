package distributions

import (
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
)

type DistributedRelation struct {
	Name        string
	ColumnNames []string
}

func DistributedRelationFromDB(distr *qdb.DistributedRelation) *DistributedRelation {
	return &DistributedRelation{
		Name:        distr.Name,
		ColumnNames: distr.ColumnNames,
	}
}

func DistributedRelationToDB(distr *DistributedRelation) *qdb.DistributedRelation {
	return &qdb.DistributedRelation{
		Name:        distr.Name,
		ColumnNames: distr.ColumnNames,
	}
}

func DistributedRelatitonToProto(distr *DistributedRelation) *proto.DistributedRelation {
	return &proto.DistributedRelation{
		Name:    distr.Name,
		Columns: distr.ColumnNames,
	}
}

func DistributedRelationFromProto(rel *proto.DistributedRelation) *DistributedRelation {
	return &DistributedRelation{
		Name:        rel.Name,
		ColumnNames: rel.Columns,
	}
}

type Distribution struct {
	Id string
	// column types to be used
	ColTypes  []string
	Relations map[string]*DistributedRelation
}

// local table sharding distr -> route to world

func NewDistribution(id string, coltypes []string) *Distribution {
	return &Distribution{
		Id:        id,
		ColTypes:  coltypes,
		Relations: map[string]*DistributedRelation{},
	}
}

func (s *Distribution) ID() string {
	return s.Id
}

func DistributionFromDB(distr *qdb.Distribution) *Distribution {
	ret := NewDistribution(distr.ID, distr.ColTypes)
	for name, val := range distr.Relations {
		ret.Relations[name] = DistributedRelationFromDB(val)
	}

	return ret
}

func DistributionFromProto(ds *proto.Distribution) *Distribution {
	return &Distribution{
		Id: ds.Id,
	}
}

func DistributionToProto(ds *Distribution) *proto.Distribution {
	drels := make([]*proto.DistributedRelation, 0)
	for _, r := range ds.Relations {
		drels = append(drels, &proto.DistributedRelation{
			Name:    r.Name,
			Columns: r.ColumnNames,
		})
	}
	return &proto.Distribution{
		Id:          ds.Id,
		ColumnTypes: ds.ColTypes,
		Relations:   drels,
	}
}

func DistributionToDB(ds *Distribution) *qdb.Distribution {
	d := &qdb.Distribution{
		ID:        ds.Id,
		ColTypes:  ds.ColTypes,
		Relations: map[string]*qdb.DistributedRelation{},
	}

	for _, r := range ds.Relations {
		d.Relations[r.Name] = DistributedRelationToDB(r)
	}

	return d
}
