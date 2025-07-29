package plan

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/xproto"
)

var ErrResolvingValue = fmt.Errorf("Error while resolving expression value")

func ParseResolveParamValue(paramCode int16, ind int, tp string, bindParams [][]byte) (any, error) {

	switch paramCode {
	case xproto.FormatCodeBinary:
		switch tp {
		case qdb.ColumnTypeUUID:
			val := string(bindParams[ind])
			return []any{val}, nil
		case qdb.ColumnTypeVarcharDeprecated:
			fallthrough
		case qdb.ColumnTypeVarcharHashed:
			fallthrough
		case qdb.ColumnTypeVarchar:
			return []any{string(bindParams[ind])}, nil
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
				return nil, ErrResolvingValue
			}

			return num, nil
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
				return nil, err
			}

			return num, nil
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
			return []any{string(bindParams[ind])}, nil
		case qdb.ColumnTypeInteger:
			num, err := strconv.ParseInt(string(bindParams[ind]), 10, 64)
			if err != nil {
				return nil, err
			}
			return num, nil
		case qdb.ColumnTypeUinteger:
			num, err := strconv.ParseUint(string(bindParams[ind]), 10, 64)
			if err != nil {
				return nil, err
			}
			return num, nil
		}
	default:
		// ??? protoc violation

	}

	return nil, ErrResolvingValue
}
