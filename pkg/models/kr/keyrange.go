package kr

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/pg-sharding/spqr/pkg/models/distributions"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

type KeyRangeBound []interface{}

type ShardKey struct {
	Name string
	RW   bool
}

// qdb KeyRange with its distibtion column types
// stored in case struct for fast convertion/access
type KeyRange struct {
	LowerBound   KeyRangeBound
	ShardID      string
	ID           string
	Distribution string

	ColumnTypes []string
}

// CmpRangesLess compares two byte slices, kr and other, and returns true if kr is less than other.
// The comparison is based on the length of the slices and the lexicographic order of their string representations.
//
// Parameters:
//   - kr: The first byte slice to compare.
//   - other: The second byte slice to compare.
//
// Returns:
//   - bool: True if kr is less than other, false otherwise.
//
// TODO : unit tests
func CmpRangesLessStringsDeprecated(bound string, key string) bool {
	if len(bound) == len(key) {
		return bound < key
	}

	return len(bound) < len(key)
}

func (kr *KeyRange) InFunc(attribInd int, raw []byte) {
	switch kr.ColumnTypes[attribInd] {
	case qdb.ColumnTypeInteger:
		n, _ := binary.Varint(raw)
		kr.LowerBound[attribInd] = n
	case qdb.ColumnTypeUinteger:
		n, _ := binary.Uvarint(raw)
		kr.LowerBound[attribInd] = n
	case qdb.ColumnTypeVarcharDeprecated:
		fallthrough
	case qdb.ColumnTypeVarchar:
		kr.LowerBound[attribInd] = string(raw)
	}
}

func (kr *KeyRange) OutFunc(attribInd int) []byte {
	switch kr.ColumnTypes[attribInd] {
	case qdb.ColumnTypeInteger:
		raw := make([]byte, 8)
		_ = binary.PutVarint(raw, kr.LowerBound[attribInd].(int64))
		return raw
	case qdb.ColumnTypeUinteger:
		raw := make([]byte, 8)
		_ = binary.PutUvarint(raw, kr.LowerBound[attribInd].(uint64))
		return raw
	case qdb.ColumnTypeVarcharDeprecated:
		fallthrough
	case qdb.ColumnTypeVarchar:
		return []byte(kr.LowerBound[attribInd].(string))
	}
	return nil
}

func (kr *KeyRange) SendFunc(attribInd int) string {
	switch kr.ColumnTypes[attribInd] {
	case qdb.ColumnTypeInteger:
		fallthrough
	case qdb.ColumnTypeUinteger:
		return fmt.Sprintf("%v", kr.LowerBound[attribInd])
	default:
		return fmt.Sprintf("'%v'", kr.LowerBound[attribInd])
	}
}

func (kr *KeyRange) Raw() [][]byte {
	res := make([][]byte, len(kr.ColumnTypes))

	for i := 0; i < len(kr.ColumnTypes); i++ {
		res[i] = kr.OutFunc(i)
	}

	return res
}

func (kr *KeyRange) SendRaw() []string {
	res := make([]string, len(kr.ColumnTypes))

	for i := 0; i < len(kr.ColumnTypes); i++ {
		res[i] = kr.SendFunc(i)
	}

	return res
}

// TODO: use it
var MissTypedKeyRange = fmt.Errorf("key range bound is mistyped")

// TODO : unit tests
func CmpRangesLess(bound KeyRangeBound, key KeyRangeBound, types []string) bool {
	// Here we panic if we failed to convert key range bound
	// element to expected type. We consider panic as much better
	// result that data corruption caused by erroreus routing logic.
	// Big TODO here is to use and check specific error of types mismatch.

	for i := 0; i < len(bound); i++ {
		switch types[i] {
		case qdb.ColumnTypeInteger:
			i1 := bound[i].(int64)
			i2 := key[i].(int64)
			if i1 == i2 {
				// continue
			} else if i1 < i2 {
				return true
			} else {
				return false
			}
		case qdb.ColumnTypeVarchar:
			i1 := bound[i].(string)
			i2 := key[i].(string)
			if i1 == i2 {
				// continue
			} else if i1 < i2 {
				return true
			} else {
				return false
			}
		case qdb.ColumnTypeVarcharDeprecated:
			i1 := bound[i].(string)
			i2 := key[i].(string)
			if i1 == i2 {
				// continue
			} else if CmpRangesLessStringsDeprecated(i1, i2) {
				return true
			} else {
				return false
			}
		default:
			panic(MissTypedKeyRange)
		}
	}

	// keys are actually equal. return false
	return false
}

func CmpRangesEqual(bound KeyRangeBound, key KeyRangeBound, types []string) bool {
	for i := 0; i < len(bound); i++ {
		switch types[i] {
		case qdb.ColumnTypeInteger:
			i1 := bound[i].(int64)
			i2 := key[i].(int64)
			if i1 == i2 {
				// continue
			} else {
				return false
			}
		case qdb.ColumnTypeVarchar:
			i1 := bound[i].(string)
			i2 := key[i].(string)
			if i1 == i2 {
				// continue

			} else {
				return false
			}
		case qdb.ColumnTypeVarcharDeprecated:
			i1 := bound[i].(string)
			i2 := key[i].(string)
			if i1 == i2 {
				// continue
			} else {
				return false
			}
		default:
			panic(MissTypedKeyRange)
		}
	}

	// keys are actually equal.
	return true
}

func CmpRangesLessEqual(bound KeyRangeBound, key KeyRangeBound, types []string) bool {
	return CmpRangesEqual(bound, key, types) || CmpRangesLess(bound, key, types)
}

// TODO : unit tests
func KeyRangeFromDB(krdb *qdb.KeyRange, colTypes []string) *KeyRange {
	kr := &KeyRange{
		ShardID:      krdb.ShardID,
		ID:           krdb.KeyRangeID,
		Distribution: krdb.DistributionId,
		ColumnTypes:  colTypes,

		LowerBound: make(KeyRangeBound, len(colTypes)),
	}

	// TODO: Fix this! (krdb.LowerBound -> krqb.LowerBound[i])
	// now this works only for unidim distributions
	for i := 0; i < len(colTypes); i++ {
		kr.InFunc(i, krdb.LowerBound[i])
	}

	return kr
}

// KeyRangeFromSQL converts a spqrparser.KeyRangeDefinition into a KeyRange.
// If kr is nil, it returns nil.
// Otherwise, it creates a new KeyRange with the provided values and returns a pointer to it.
//
// Parameters:
//   - kr: The spqrparser.KeyRangeDefinition to convert.
//
// Returns:
//   - *KeyRange: A pointer to the new KeyRange object.
//
// TODO : unit tests
func KeyRangeFromSQL(krsql *spqrparser.KeyRangeDefinition, colTypes []string) (*KeyRange, error) {
	if krsql == nil {
		return nil, nil
	}
	kr := &KeyRange{
		ShardID:      krsql.ShardID,
		ID:           krsql.KeyRangeID,
		Distribution: krsql.Distribution,

		ColumnTypes: colTypes,

		LowerBound: make(KeyRangeBound, len(colTypes)),
	}

	if len(colTypes) != len(krsql.LowerBound.Pivots) {
		return nil, fmt.Errorf("number of columns mismatches with distribution")
	}

	for i := 0; i < len(colTypes); i++ {
		kr.InFunc(i, krsql.LowerBound.Pivots[i])
	}

	return kr, nil
}

func KeyRangeFromBytes(val [][]byte, colTypes []string) *KeyRange {

	kr := &KeyRange{
		ColumnTypes: colTypes,

		LowerBound: make(KeyRangeBound, len(colTypes)),
	}

	for i := 0; i < len(colTypes); i++ {
		kr.InFunc(i, val[i])
	}

	return kr
}

// KeyRangeFromProto converts a protobuf KeyRangeInfo to a KeyRange object.
// If the input KeyRangeInfo is nil, it returns nil.
// Otherwise, it creates a new KeyRange object with the values from the KeyRangeInfo object and returns a pointer to it.
//
// Parameters:
//   - kr: The protobuf KeyRangeInfo to convert.
//
// Returns:
//   - *KeyRange: A pointer to the new KeyRange object.
//
// TODO : unit tests
func KeyRangeFromProto(krproto *proto.KeyRangeInfo, colTypes []string) *KeyRange {
	if krproto == nil {
		return nil
	}
	kr := &KeyRange{
		ShardID:      krproto.ShardId,
		ID:           krproto.Krid,
		Distribution: krproto.DistributionId,
		ColumnTypes:  colTypes,

		LowerBound: make(KeyRangeBound, len(colTypes)),
	}
	//if len(colTypes) != len(krsql.LowerBound.Pivots) {
	//	return nil, fmt.Errorf("number of columns mismatches with distribution")
	//}

	for i := 0; i < len(colTypes); i++ {
		kr.InFunc(i, krproto.Bound.Values[i])
	}

	return kr
}

// ToDB converts the KeyRange struct to a qdb.KeyRange struct.
// It returns a pointer to the converted qdb.KeyRange struct.
//
// Returns:
//   - *qdb.KeyRange: A pointer to the converted qdb.KeyRange struct.
//
// TODO : unit tests
func (kr *KeyRange) ToDB() *qdb.KeyRange {
	krDb := &qdb.KeyRange{
		LowerBound:     make([][]byte, len(kr.ColumnTypes)),
		ShardID:        kr.ShardID,
		KeyRangeID:     kr.ID,
		DistributionId: kr.Distribution,
	}
	for i := 0; i < len(kr.ColumnTypes); i++ {
		krDb.LowerBound[i] = kr.OutFunc(i)
	}
	return krDb
}

// ToProto converts the KeyRange struct to a protobuf KeyRangeInfo message.
// It returns a pointer to the converted KeyRangeInfo message.
//
// Returns:
//   - *proto.KeyRangeInfo: A pointer to the converted KeyRangeInfo message.
//
// TODO : unit tests
func (kr *KeyRange) ToProto() *proto.KeyRangeInfo {
	krProto := &proto.KeyRangeInfo{
		Bound: &proto.KeyRangeBound{
			Values: make([][]byte, len(kr.ColumnTypes)),
		},
		ShardId:        kr.ShardID,
		Krid:           kr.ID,
		DistributionId: kr.Distribution,
	}

	for i := 0; i < len(kr.ColumnTypes); i++ {
		krProto.Bound.Values[i] = kr.OutFunc(i)
	}

	return krProto
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
		fqCol := ""
		if prefix != "" {
			fqCol = fmt.Sprintf("%s.%s", prefix, entry.Column)
		} else {
			fqCol = entry.Column
		}

		krTmp := KeyRange{

			LowerBound:  upperBound,
			ColumnTypes: kRange.ColumnTypes,
		}

		if upperBound != nil {
			buf[i] = fmt.Sprintf("%s >= %s AND %s < %s", fqCol, kRange.SendFunc(i), fqCol, krTmp.SendFunc(i))
		} else {
			buf[i] = fmt.Sprintf("%s >= %s", fqCol, kRange.SendFunc(i))
		}
	}
	return strings.Join(buf, " AND ")
}
