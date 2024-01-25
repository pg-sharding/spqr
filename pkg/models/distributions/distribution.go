package distributions

import (
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
)

type DistributedRelatiton struct {
	Name        string
	ColumnNames []string
}

func DistributedRelatitonFromDB(rule *qdb.DistributedRelatiton) *DistributedRelatiton {
	return &DistributedRelatiton{
		Name:        rule.Name,
		ColumnNames: rule.ColumnNames,
	}
}

type Distribution struct {
	Id string
	// column types to be used
	ColTypes  []string
	Relations map[string]*DistributedRelatiton
}

// local table sharding rule -> route to world

func NewDistribution(id string) *Distribution {
	return &Distribution{
		Id: id,
	}
}

func (s *Distribution) ID() string {
	return s.Id
}

func DistributionFromDB(rule *qdb.Distribution) *Distribution {
	distr := &Distribution{
		Id:       rule.ID,
		ColTypes: rule.ColTypes,
	}
	for name, val := range rule.Relations {
		distr.Relations[name] = DistributedRelatitonFromDB(val)
	}

	return distr
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
