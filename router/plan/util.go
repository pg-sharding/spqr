package plan

import (
	"bytes"
	"encoding/binary"
	"strconv"

	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/xproto"
)

func ParseResolveParamValue(paramCode int16, ind int, tp string, bindParams [][]byte) (any, bool) {

	switch paramCode {
	case xproto.FormatCodeBinary:
		switch tp {
		case qdb.ColumnTypeUUID:
			val := string(bindParams[ind])
			return []any{val}, true
		case qdb.ColumnTypeVarcharDeprecated:
			fallthrough
		case qdb.ColumnTypeVarcharHashed:
			fallthrough
		case qdb.ColumnTypeVarchar:
			return []any{string(bindParams[ind])}, true
		case qdb.ColumnTypeInteger:

			var num int64
			var err error

			buf := bytes.NewBuffer(bindParams[ind])

			if len(bindParams[ind]) == 4 {
				var tmpnum int32
				err = binary.Read(buf, binary.BigEndian, &tmpnum)
				num = int64(tmpnum)
			} else {
				err = binary.Read(buf, binary.BigEndian, &num)
			}
			if err != nil {
				return nil, false
			}

			return num, true
		case qdb.ColumnTypeUinteger:

			var num uint64
			var err error

			buf := bytes.NewBuffer(bindParams[ind])

			if len(bindParams[ind]) == 4 {
				var tmpnum uint32
				err = binary.Read(buf, binary.BigEndian, &tmpnum)
				num = uint64(tmpnum)
			} else {
				err = binary.Read(buf, binary.BigEndian, &num)
			}
			if err != nil {
				return nil, false
			}

			return num, true
		}
	case xproto.FormatCodeText:
		switch tp {
		case qdb.ColumnTypeUUID:
			fallthrough
		case qdb.ColumnTypeVarcharDeprecated:
			fallthrough
		case qdb.ColumnTypeVarcharHashed:
			fallthrough
		case qdb.ColumnTypeVarchar:
			return []any{string(bindParams[ind])}, true
		case qdb.ColumnTypeInteger:
			num, err := strconv.ParseInt(string(bindParams[ind]), 10, 64)
			if err != nil {
				return nil, false
			}
			return num, true
		case qdb.ColumnTypeUinteger:
			num, err := strconv.ParseUint(string(bindParams[ind]), 10, 64)
			if err != nil {
				return nil, false
			}
			return num, true
		}
	default:
		// ??? protoc violation

	}

	return nil, false
}
