package kr

import (
	"encoding/binary"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
	"strconv"
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

// CmpBounds compares two key range bounds dependent on their type
// TODO: unit tests
// TODO: compare composite key bounds
func CmpBounds(l, r []byte, t string) (int, error) {
	switch t {
	case "integer":
		leftBound, err := strconv.Atoi(string(l))
		if err != nil {
			return 0, err
		}
		rightBound, err := strconv.Atoi(string(r))
		if err != nil {
			return 0, err
		}
		if leftBound < rightBound {
			return -1, nil
		} else if leftBound == rightBound {
			return 0, nil
		}
		return 1, nil
	case "uniteger":
		leftBound := binary.BigEndian.Uint64(l)
		rightBound := binary.BigEndian.Uint64(r)
		if leftBound < rightBound {
			return -1, nil
		} else if leftBound == rightBound {
			return 0, nil
		}
		return 1, nil
	case "varchar":
		leftBound := string(l)
		rightBound := string(r)
		if leftBound < rightBound {
			return -1, nil
		} else if leftBound == rightBound {
			return 0, nil
		}
		return 1, nil
	default:
		return 0, spqrerror.Newf(spqrerror.SPQR_METADATA_CORRUPTION, "incorrect column type \"%s\"", t)
	}
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
