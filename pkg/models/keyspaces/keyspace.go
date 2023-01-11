package keyspaces

import "github.com/pg-sharding/spqr/qdb"

type Keyspace struct {
	Id string
	// list or map of related sharding rules TBD
}

// local table sharding rule -> route to world

func NewKeyspace(id string) *Keyspace {
	return &Keyspace{
		Id: id,
	}
}

func (s *Keyspace) ID() string {
	return s.Id
}

func KeyspaceFromDB(rule *qdb.Keyspace) *Keyspace {
	return &Keyspace{
		Id: rule.ID,
	}
}
