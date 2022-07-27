package kr

import (
	"github.com/pg-sharding/spqr/qdb"
	proto "github.com/pg-sharding/spqr/router/protos"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

type KeyRangeBound []byte

type ShardKey struct {
	Name string
	RW   bool
}

type KeyRange struct {
	LowerBound []byte
	UpperBound []byte
	ShardID    string
	ID         string
}

func CmpRangesLess(kr []byte, other []byte) bool {
	if len(kr) == len(other) {
		return string(kr) <= string(other)
	}

	return len(kr) <= len(other)
}

func KeyRangeFromDB(kr *qdb.KeyRange) *KeyRange {
	return &KeyRange{
		LowerBound: kr.LowerBound,
		UpperBound: kr.UpperBound,
		ShardID:    kr.ShardID,
		ID:         kr.KeyRangeID,
	}
}

func KeyRangeFromSQL(kr *spqrparser.AddKeyRange) *KeyRange {
	if kr == nil {
		return nil
	}
	return &KeyRange{
		LowerBound: kr.LowerBound,
		UpperBound: kr.UpperBound,
		ShardID:    kr.ShardID,
		ID:         kr.KeyRangeID,
	}
}

func KeyRangeFromProto(kr *proto.KeyRangeInfo) *KeyRange {

	if kr == nil {
		return nil
	}
	return &KeyRange{
		LowerBound: []byte(kr.KeyRange.LowerBound),
		UpperBound: []byte(kr.KeyRange.UpperBound),
		ShardID:    kr.ShardId,
		ID:         kr.Krid,
	}
}

func (kr *KeyRange) ToSQL() *qdb.KeyRange {
	return &qdb.KeyRange{
		LowerBound: kr.LowerBound,
		UpperBound: kr.UpperBound,
		ShardID:    kr.ShardID,
		KeyRangeID: kr.ID,
	}
}

func (kr *KeyRange) ToProto() *proto.KeyRangeInfo {
	return &proto.KeyRangeInfo{
		KeyRange: &proto.KeyRange{
			LowerBound: string(kr.LowerBound),
			UpperBound: string(kr.UpperBound),
		},
		ShardId: kr.ShardID,
		Krid:    kr.ID,
	}
}
