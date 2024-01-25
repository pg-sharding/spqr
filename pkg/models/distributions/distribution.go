package distributions

import (
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
)

type DistributedRelatiton struct {
	Name        string
	ColumnNames []string
}

type Distribution struct {
	Id string
	// column types to be used
	ColTypes  []string
	Relations map[string]DistributedRelatiton
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
	return &Distribution{
		Id: rule.ID,
	}
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
