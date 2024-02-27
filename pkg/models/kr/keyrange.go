package kr

import (
	"encoding/binary"

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

func CmpRangesLessEqualStringsDeprecated(bound string, key string) bool {
	if len(bound) == len(key) {
		return bound <= key
	}

	return len(bound) <= len(key)
}

func (kr *KeyRange) InFunc(attribind int, raw []byte) {
	switch kr.ColumnTypes[attribind] {
	case qdb.ColumnTypeInteger:
		n, _ := binary.Varint(raw)
		kr.LowerBound[attribind] = n
	case qdb.ColumnTypeUinteger:
		n, _ := binary.Uvarint(raw)
		kr.LowerBound[attribind] = n
	case qdb.ColumnTypeVarcharDeprecated:
		fallthrough
	case qdb.ColumnTypeVarchar:
		kr.LowerBound[attribind] = string(raw)
	}
}

func (kr *KeyRange) OutFunc(attribind int) []byte {
	switch kr.ColumnTypes[attribind] {
	case qdb.ColumnTypeInteger:
		raw := make([]byte, 8)
		_ = binary.PutVarint(raw, kr.LowerBound[attribind].(int64))
		return raw
	case qdb.ColumnTypeUinteger:
		raw := make([]byte, 8)
		_ = binary.PutUvarint(raw, kr.LowerBound[attribind].(uint64))
		return raw
	case qdb.ColumnTypeVarcharDeprecated:
		fallthrough
	case qdb.ColumnTypeVarchar:
		return []byte(kr.LowerBound[attribind].(string))
	}
	return nil
}

func (kr *KeyRange) Raw() [][]byte {
	res := make([][]byte, len(kr.ColumnTypes))

	for i := 0; i < len(kr.ColumnTypes); i++ {
		res = append(res, kr.OutFunc(i))
	}

	return res
}

// TODO : unit tests
func CmpRangesLessEqual(bound KeyRangeBound, key KeyRangeBound, types []string) bool {
	for i := 0; i < len(bound); i++ {
		switch types[i] {
		case distributions.ColumnTypeInteger:
			i1 := bound[i].(int64)
			i2 := key[i].(int64)
			if i1 == i2 {
				// continue
			} else if i1 < i2 {
				return true
			} else {
				return false
			}
		case distributions.ColumnTypeVarchar:
			i1 := bound[i].(string)
			i2 := key[i].(string)
			if i1 == i2 {
				// continue
			} else if i1 < i2 {
				return true
			} else {
				return false
			}
		case distributions.ColumnTypeVarcharDeprecated:
			i1 := bound[i].(string)
			i2 := key[i].(string)
			if i1 == i2 {
				// continue
			} else if CmpRangesLessEqualStringsDeprecated(i1, i2) {
				return true
			} else {
				return false
			}
		default:
			// wtf?
		}
	}
	return true
}

// TODO : unit tests
func CmpRangesEqual(kr []byte, other []byte) bool {
	if len(kr) == len(other) {
		return string(kr) == string(other)
	}

	return false
}

// TODO : unit tests
func KeyRangeFromDB(krdb *qdb.KeyRange, colTypes []string) *KeyRange {
	kr := &KeyRange{
		ShardID:      krdb.ShardID,
		ID:           krdb.KeyRangeID,
		Distribution: krdb.DistributionId,
		ColumnTypes:  colTypes,
	}

	for i := 0; i < len(colTypes); i++ {
		kr.InFunc(i, krdb.LowerBound[i])
	}

	return kr
}

// TODO : unit tests
func KeyRangeFromSQL(krsql *spqrparser.KeyRangeDefinition, coltypes []string) *KeyRange {
	if krsql == nil {
		return nil
	}
	kr := &KeyRange{
		ShardID:      krsql.ShardID,
		ID:           krsql.KeyRangeID,
		Distribution: krsql.Distribution,
	}

	for i := 0; i < len(coltypes); i++ {
		kr.InFunc(i, krsql.LowerBound.Pivots[i])
	}

	return kr
}

// TODO : unit tests
func KeyRangeFromProto(krproto *proto.KeyRangeInfo, coltypes []string) *KeyRange {
	if krproto == nil {
		return nil
	}
	kr := &KeyRange{
		ShardID:      krproto.ShardId,
		ID:           krproto.Krid,
		Distribution: krproto.DistributionId,
	}

	for i := 0; i < len(coltypes); i++ {
		kr.InFunc(i, krproto.Bound.LowerBound[i])
	}

	return kr
}

// TODO : unit tests
func (kr *KeyRange) ToDB() *qdb.KeyRange {
	krqb := &qdb.KeyRange{
		LowerBound: make([][]byte, len(kr.ColumnTypes)),
		ShardID:    kr.ShardID,
		KeyRangeID: kr.ID,
		KeyspaceId: kr.Keyspace,
	}
	for i := 0; i < len(kr.ColumnTypes); i++ {
		krqb.LowerBound[i] = kr.OutFunc(i)
	}
	return krqb
}

// TODO : unit tests
func (kr *KeyRange) ToProto() *proto.KeyRangeInfo {
	krprot := &proto.KeyRangeInfo{
		Bound: &proto.KeyRangeBound{
			LowerBound: make([][]byte, len(kr.ColumnTypes)),
		},
		ShardId:    kr.ShardID,
		Krid:       kr.ID,
		KeyspaceId: kr.Keyspace,
	}

	for i := 0; i < len(kr.ColumnTypes); i++ {
		krprot.Bound.LowerBound[i] = kr.OutFunc(i)
	}

	return krprot
}
