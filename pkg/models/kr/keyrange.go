package kr

import (
	"fmt"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
	"strings"
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

// GetKRCondition returns SQL condition for elements of distributed relation between two key ranges
// TODO support multidimensional key ranges
func GetKRCondition(ds *distributions.Distribution, rel *distributions.DistributedRelation, kRange *KeyRange, upperBound KeyRangeBound, prefix string) string {
	buf := make([]string, len(rel.DistributionKey))
	for i, entry := range rel.DistributionKey {
		// TODO remove after multidimensional key range support
		if i > 0 {
			break
		}
		// TODO add hash (depends on col type)
		hashedCol := ""
		if prefix != "" {
			hashedCol = fmt.Sprintf("%s.%s", prefix, entry.Column)
		} else {
			hashedCol = entry.Column
		}
		lBound := ""
		if ds.ColTypes[i] == "varchar" {
			lBound = fmt.Sprintf("'%s'", string(kRange.LowerBound))
		} else {
			lBound = fmt.Sprintf("%s", string(kRange.LowerBound))
		}
		if upperBound != nil {
			rBound := ""
			if ds.ColTypes[i] == "varchar" {
				rBound = fmt.Sprintf("'%s'", string(upperBound))
			} else {
				rBound = fmt.Sprintf("%s", string(upperBound))
			}
			buf[i] = fmt.Sprintf("%s >= %s AND %s < %s", hashedCol, lBound, hashedCol, rBound)
		} else {
			buf[i] = fmt.Sprintf("%s >= %s", hashedCol, lBound)
		}
	}
	return strings.Join(buf, " AND ")
}
