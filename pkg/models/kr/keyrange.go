package kr

import (
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

type KeyRangeBound []byte

type ShardKey struct {
	Name string
	RW   bool
}

type KeyRange struct {
	LowerBound   KeyRangeBound
	ShardID      string
	ID           string
	Distribution string
}

// TODO : unit tests
func CmpRangesLess(kr []byte, other []byte) bool {
	if len(kr) == len(other) {
		return string(kr) < string(other)
	}

	return len(kr) < len(other)
}

// TODO : unit tests
func CmpRangesLessEqual(kr []byte, other []byte) bool {
	if len(kr) == len(other) {
		return string(kr) <= string(other)
	}

	return len(kr) < len(other)
}

// TODO : unit tests
func CmpRangesEqual(kr []byte, other []byte) bool {
	if len(kr) == len(other) {
		return string(kr) == string(other)
	}

	return false
}

// TODO : unit tests
func KeyRangeFromDB(kr *qdb.KeyRange) *KeyRange {
	return &KeyRange{
		LowerBound:   kr.LowerBound,
		ShardID:      kr.ShardID,
		ID:           kr.KeyRangeID,
		Distribution: kr.DistributionId,
	}
}

// TODO : unit tests
func KeyRangeFromSQL(kr *spqrparser.KeyRangeDefinition) *KeyRange {
	if kr == nil {
		return nil
	}
	return &KeyRange{
		LowerBound:   kr.LowerBound,
		ShardID:      kr.ShardID,
		ID:           kr.KeyRangeID,
		Distribution: kr.Distribution,
	}
}

// TODO : unit tests
func KeyRangeFromProto(kr *proto.KeyRangeInfo) *KeyRange {
	if kr == nil {
		return nil
	}
	return &KeyRange{
		LowerBound:   KeyRangeBound(kr.KeyRange.LowerBound),
		ShardID:      kr.ShardId,
		ID:           kr.Krid,
		Distribution: kr.DistributionId,
	}
}

// TODO : unit tests
func (kr *KeyRange) ToDB() *qdb.KeyRange {
	return &qdb.KeyRange{
		LowerBound:     kr.LowerBound,
		ShardID:        kr.ShardID,
		KeyRangeID:     kr.ID,
		DistributionId: kr.Distribution,
	}
}

// TODO : unit tests
func (kr *KeyRange) ToProto() *proto.KeyRangeInfo {
	return &proto.KeyRangeInfo{
		KeyRange: &proto.KeyRange{
			LowerBound: string(kr.LowerBound),
		},
		ShardId:        kr.ShardID,
		Krid:           kr.ID,
		DistributionId: kr.Distribution,
	}
}
