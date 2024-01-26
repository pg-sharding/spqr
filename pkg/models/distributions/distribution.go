package distributions

import (
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
)

type DistributedRelation struct {
	Name        string
	ColumnNames []string
}

func DistributedRelatitonFromDB(distr *qdb.DistributedRelation) *DistributedRelation {
	return &DistributedRelation{
		Name:        distr.Name,
		ColumnNames: distr.ColumnNames,
	}
}

func DistributedRelatitonToDB(distr *DistributedRelation) *qdb.DistributedRelation {
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
		ret.Relations[name] = DistributedRelatitonFromDB(val)
	}

	return ret
}

func DistributionFromProto(ds *proto.Distribution) *Distribution {
	return &Distribution{
		Id: ds.Id,
	}
}

func DistributionToProto(ds *Distribution) *proto.Distribution {
	return &proto.Distribution{
		Id: ds.Id,
	}
}

func DistributionToDB(ds *Distribution) *qdb.Distribution {
	return &qdb.Distribution{
		ID:        ds.Id,
		ColTypes:  ds.ColTypes,
		Relations: map[string]*qdb.DistributedRelation{},
	}
}
