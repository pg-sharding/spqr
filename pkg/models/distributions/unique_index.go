package distributions

import (
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/rfqn"
)

type UniqueIndex struct {
	ID           string
	RelationName *rfqn.RelationFQN
	ColumnName   string
}

func UniqueIndexToProto(idx *UniqueIndex) *proto.UniqueIndex {
	return &proto.UniqueIndex{
		Id:        idx.ID,
		TableName: rfqn.RelationFQNToProto(idx.RelationName),
		ColName:   idx.ColumnName,
	}
}

func UniqueIndexFromProto(idx *proto.UniqueIndex) *UniqueIndex {
	return &UniqueIndex{
		ID:           idx.Id,
		RelationName: rfqn.RelationFQNFromProto(idx.TableName),
		ColumnName:   idx.ColName,
	}
}

func UniqueIndexToDB(dsId string, idx *UniqueIndex) *qdb.UniqueIndex {
	return &qdb.UniqueIndex{
		ID:             idx.ID,
		Relation:       idx.RelationName,
		ColumnName:     idx.ColumnName,
		DistributionId: dsId,
	}
}

func UniqueIndexFromDB(idx *qdb.UniqueIndex) *UniqueIndex {
	return &UniqueIndex{
		ID:           idx.ID,
		RelationName: idx.Relation,
		ColumnName:   idx.ColumnName,
	}
}
