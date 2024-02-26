package distributions

import (
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

type DistributionKeyEntry struct {
	Column       string
	HashFunction string
}

type DistributedRelation struct {
	Name            string
	DistributionKey []DistributionKeyEntry
}

func DistributedRelationFromDB(rel *qdb.DistributedRelation) *DistributedRelation {
	rdistr := &DistributedRelation{
		Name: rel.Name,
	}

	for _, e := range rel.DistributionKey {
		rdistr.DistributionKey = append(rdistr.DistributionKey, DistributionKeyEntry{
			Column:       e.Column,
			HashFunction: e.HashFunction,
		})
	}

	return rdistr
}

func DistributedRelationToDB(rel *DistributedRelation) *qdb.DistributedRelation {
	rdistr := &qdb.DistributedRelation{
		Name: rel.Name,
	}

	for _, e := range rel.DistributionKey {
		rdistr.DistributionKey = append(rdistr.DistributionKey, qdb.DistributionKeyEntry{
			Column:       e.Column,
			HashFunction: e.HashFunction,
		})
	}

	return rdistr
}

func DistributedRelatitonToProto(rel *DistributedRelation) *proto.DistributedRelation {
	rdistr := &proto.DistributedRelation{
		Name: rel.Name,
	}

	for _, e := range rel.DistributionKey {
		rdistr.DistributionKey = append(rdistr.DistributionKey, &proto.DistributionKeyEntry{
			Column:       e.Column,
			HashFunction: e.HashFunction,
		})
	}

	return rdistr
}

func DistributedRelationFromProto(rel *proto.DistributedRelation) *DistributedRelation {
	rdistr := &DistributedRelation{
		Name: rel.Name,
	}

	for _, e := range rel.DistributionKey {
		rdistr.DistributionKey = append(rdistr.DistributionKey, DistributionKeyEntry{
			Column:       e.Column,
			HashFunction: e.HashFunction,
		})
	}

	return rdistr
}

func DistributedRelationFromSQL(rel *spqrparser.DistributedRelation) *DistributedRelation {
	rdistr := &DistributedRelation{
		Name: rel.Name,
	}

	for _, e := range rel.DistributionKey {
		rdistr.DistributionKey = append(rdistr.DistributionKey, DistributionKeyEntry{
			Column:       e.Column,
			HashFunction: e.HashFunction,
		})
	}

	return rdistr
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
		Id:       ds.Id,
		ColTypes: ds.ColumnTypes,
		Relations: func() map[string]*DistributedRelation {
			res := make(map[string]*DistributedRelation)
			for _, rel := range ds.Relations {
				res[rel.Name] = DistributedRelationFromProto(rel)
			}
			return res
		}(),
	}
}

func DistributionToProto(ds *Distribution) *proto.Distribution {
	drels := make([]*proto.DistributedRelation, 0)
	for _, r := range ds.Relations {
		drels = append(drels, DistributedRelatitonToProto(r))
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
