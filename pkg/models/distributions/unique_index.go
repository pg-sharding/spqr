package distributions

import (
	"github.com/pg-sharding/spqr/pkg/models/acl"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/rfqn"
)

type UniqueIndex struct {
	ID           string
	RelationName *rfqn.RelationFQN
	Columns      []string
	ColTypes     []string

	Version uint64
	/* XXX: should we have implicit acl here? */
	ACL []acl.ACLItem
}

func UniqueIndexToProto(idx *UniqueIndex) *proto.UniqueIndex {
	return &proto.UniqueIndex{
		Id:        idx.ID,
		TableName: rfqn.RelationFQNToProto(idx.RelationName),
		Columns:   idx.Columns,
		ColTypes:  idx.ColTypes,
	}
}

func UniqueIndexFromProto(idx *proto.UniqueIndex) *UniqueIndex {
	return &UniqueIndex{
		ID:           idx.Id,
		RelationName: rfqn.RelationFQNFromProto(idx.TableName),
		Columns:      idx.Columns,
		ColTypes:     idx.ColTypes,
	}
}

func UniqueIndexToDB(dsid string, idx *UniqueIndex) *qdb.UniqueIndex {
	return &qdb.UniqueIndex{
		ID:             idx.ID,
		Relation:       idx.RelationName,
		ColumnNames:    idx.Columns,
		DistributionId: dsid,
		ColTypes:       idx.ColTypes,
	}
}

func UniqueIndexFromDB(idx *qdb.UniqueIndex) *UniqueIndex {
	return &UniqueIndex{
		ID:           idx.ID,
		RelationName: idx.Relation,
		Columns:      idx.ColumnNames,
		ColTypes:     idx.ColTypes,
	}
}
