package distributions

import (
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
)

type DistributedRelatiton struct {
	Name        string
	ColumnNames []string
}

func DistributedRelatitonFromDB(distr *qdb.DistributedRelatiton) *DistributedRelatiton {
	return &DistributedRelatiton{
		Name:        distr.Name,
		ColumnNames: distr.ColumnNames,
	}
}

func DistributedRelatitonToDB(distr *DistributedRelatiton) *qdb.DistributedRelatiton {
	return &qdb.DistributedRelatiton{
		Name:        distr.Name,
		ColumnNames: distr.ColumnNames,
	}
}

func DistributedRelatitonToProto(distr *DistributedRelatiton) *proto.DistributedRelation {
	return &proto.DistributedRelation{
		Name:    distr.Name,
		Columns: distr.ColumnNames,
	}
}

type Distribution struct {
	Id string
	// column types to be used
	ColTypes  []string
	Relations map[string]*DistributedRelatiton
}

// local table sharding distr -> route to world

func NewDistribution(id string) *Distribution {
	return &Distribution{
		Id: id,
	}
}

func (s *Distribution) ID() string {
	return s.Id
}

func DistributionFromDB(distr *qdb.Distribution) *Distribution {
	ret := &Distribution{
		Id:       distr.ID,
		ColTypes: distr.ColTypes,
	}
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
		ID:       ds.Id,
		ColTypes: ds.ColTypes,
	}
}
