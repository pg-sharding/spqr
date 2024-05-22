package kr

import (
	"fmt"
	"strings"

	"github.com/pg-sharding/spqr/pkg/models/distributions"
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

// CmpRangesLess compares two byte slices, kr and other, and returns true if kr is less than other.
// The comparison is based on the length of the slices and the lexicographic order of their string representations.
// TODO : unit tests
func CmpRangesLess(kr []byte, other []byte) bool {
	if len(kr) == len(other) {
		return string(kr) < string(other)
	}

	return len(kr) < len(other)
}

// CmpRangesLessEqual compares two byte slices, kr and other, and returns true if kr is less than or equal to other.
// The comparison is done by comparing the lengths of the slices first. If the lengths are equal, the function compares the byte values lexicographically.
// Returns true if kr is less than or equal to other, false otherwise.
// TODO : unit tests
func CmpRangesLessEqual(kr []byte, other []byte) bool {
	if len(kr) == len(other) {
		return string(kr) <= string(other)
	}

	return len(kr) < len(other)
}

// CmpRangesEqual compares two byte slices, kr and other, and returns true if they are equal.
// It checks if the lengths of kr and other are the same, and then compares their string representations.
// TODO : unit tests
func CmpRangesEqual(kr []byte, other []byte) bool {
	if len(kr) == len(other) {
		return string(kr) == string(other)
	}

	return false
}

// KeyRangeFromDB converts a qdb.KeyRange object to a KeyRange object.
// It creates a new KeyRange object with the values from the qdb.KeyRange object.
// TODO : unit tests
func KeyRangeFromDB(kr *qdb.KeyRange) *KeyRange {
	return &KeyRange{
		LowerBound:   kr.LowerBound,
		ShardID:      kr.ShardID,
		ID:           kr.KeyRangeID,
		Distribution: kr.DistributionId,
	}
}

// KeyRangeFromSQL converts a spqrparser.KeyRangeDefinition into a KeyRange.
// If kr is nil, it returns nil.
// Otherwise, it creates a new KeyRange with the provided values and returns a pointer to it.
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

// KeyRangeFromProto converts a protobuf KeyRangeInfo to a KeyRange object.
// If the input KeyRangeInfo is nil, it returns nil.
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

// ToDB converts the KeyRange struct to a qdb.KeyRange struct.
// It returns a pointer to the converted qdb.KeyRange struct.
// TODO : unit tests
func (kr *KeyRange) ToDB() *qdb.KeyRange {
	return &qdb.KeyRange{
		LowerBound:     kr.LowerBound,
		ShardID:        kr.ShardID,
		KeyRangeID:     kr.ID,
		DistributionId: kr.Distribution,
	}
}

// ToProto converts the KeyRange struct to a protobuf KeyRangeInfo message.
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
//
// Parameters:
//   - ds: The distribution object.
//   - rel: The distributed relation object.
//   - kRange: The key range object.
//   - upperBound: The upper bound of the key range.
//   - prefix: The prefix to use for the column names.
//
// Returns:
//   - string: The SQL condition for the key range.
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
			lBound = string(kRange.LowerBound)
		}
		if upperBound != nil {
			rBound := ""
			if ds.ColTypes[i] == "varchar" {
				rBound = fmt.Sprintf("'%s'", string(upperBound))
			} else {
				rBound = string(upperBound)
			}
			buf[i] = fmt.Sprintf("%s >= %s AND %s < %s", hashedCol, lBound, hashedCol, rBound)
		} else {
			buf[i] = fmt.Sprintf("%s >= %s", hashedCol, lBound)
		}
	}
	return strings.Join(buf, " AND ")
}
