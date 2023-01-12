package dataspaces

import "github.com/pg-sharding/spqr/qdb"

type Dataspace struct {
	Id string
	// list or map of related sharding rules TBD
}

// local table sharding rule -> route to world

func NewDataspace(id string) *Dataspace {
	return &Dataspace{
		Id: id,
	}
}

func (s *Dataspace) ID() string {
	return s.Id
}

func DataspaceFromDB(rule *qdb.Dataspace) *Dataspace {
	return &Dataspace{
		Id: rule.ID,
	}
}
