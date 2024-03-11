package hashfunction

import (
	"encoding/binary"
	"fmt"

	"github.com/go-faster/city"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/spaolacci/murmur3"
)

type HashFunctionType int

/* Pre-defined hash functions */
const (
	HashFunctionIdent  = 0
	HashFunctionMurmur = 1
	HashFunctionCity   = 2
)

var (
	errNoSuchHashFunction = fmt.Errorf("no such hash function")
)

// ApplyHashFunction applies the specified hash function to the input byte slice.
// It returns the hashed byte slice and an error, if any.
//
// Parameters:
//   - inp: The input byte slice to hash.
//   - hf: The hash function to apply.
//
// Returns:
//   - []byte: The hashed byte slice.
//   - error: An error if any error occurs during the process.
func ApplyHashFunction(inp interface{}, ctype string, hf HashFunctionType) (interface{}, error) {
	switch hf {
	case HashFunctionIdent:
		return inp, nil
	case HashFunctionMurmur:
		switch ctype {
		case qdb.ColumnTypeInteger:
			buf := make([]byte, 8)
			binary.PutVarint(buf, inp.(int64))
			h := murmur3.Sum64(buf)
			return h, nil

		case qdb.ColumnTypeUinteger:
			buf := make([]byte, 8)
			binary.PutUvarint(buf, inp.(uint64))
			h := murmur3.Sum64(buf)
			return h, nil
		case qdb.ColumnTypeVarcharDeprecated:
			fallthrough
		case qdb.ColumnTypeVarchar:
			h := murmur3.Sum64(inp.([]byte))
			return h, nil
		default:
			return nil, errNoSuchHashFunction
		}
	case HashFunctionCity:
		switch ctype {
		case qdb.ColumnTypeInteger:
			buf := make([]byte, 8)
			binary.PutVarint(buf, inp.(int64))
			h := city.Hash64(buf)
			return h, nil

		case qdb.ColumnTypeUinteger:
			buf := make([]byte, 8)
			binary.PutUvarint(buf, inp.(uint64))
			h := city.Hash64(buf)
			return h, nil
		case qdb.ColumnTypeVarcharDeprecated:
			fallthrough
		case qdb.ColumnTypeVarchar:
			h := city.Hash64(inp.([]byte))
			return h, nil
		default:
			return nil, errNoSuchHashFunction
		}
	default:
		return nil, errNoSuchHashFunction
	}
}

// HashFunctionByName returns the corresponding HashFunctionType based on the given hash function name.
// It accepts a string parameter `hfn` representing the hash function name.
// It returns the corresponding HashFunctionType and an error if the hash function name is not recognized.
//
// Parameters:
//   - hfn: The name of the hash function.
//
// Returns:
//   - HashFunctionType: The corresponding HashFunctionType.
//   - error: An error if the hash function name is not recognized.
func HashFunctionByName(hfn string) (HashFunctionType, error) {
	switch hfn {
	case "identity", "ident", "":
		return HashFunctionIdent, nil
	case "murmur":
		return HashFunctionMurmur, nil
	case "city":
		return HashFunctionCity, nil
	default:
		return 0, errNoSuchHashFunction
	}
}

// ToString converts a HashFunctionType to its corresponding string representation.
// It takes a HashFunctionType as input and returns the string representation of the hash function.
// If the input HashFunctionType is not recognized, an empty string is returned.
//
// Parameters:
//   - hf: The HashFunctionType to convert.
//
// Returns:
//   - string: The string representation of the hash function.
func ToString(hf HashFunctionType) string {
	switch hf {
	case HashFunctionIdent:
		return "identity"
	case HashFunctionMurmur:
		return "murmur"
	case HashFunctionCity:
		return "city"
	}
	return ""
}
