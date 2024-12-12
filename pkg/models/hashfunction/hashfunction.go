package hashfunction

import (
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/go-faster/city"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/spaolacci/murmur3"
)

type HashFunctionType int

/* Pre-defined hash functions */
const (
	HashFunctionIdent  = HashFunctionType(0)
	HashFunctionMurmur = HashFunctionType(1)
	HashFunctionCity   = HashFunctionType(2)
)

var (
	errUnknownColumnType = func(ctype string, hf HashFunctionType) error {
		return fmt.Errorf("unknown column type '%s' for hash function '%d'", ctype, hf)
	}
	errUnknownValueType = func(v interface{}, hf HashFunctionType) error {
		return fmt.Errorf("unknown type of value that the hash will be calculated from: %T for %d hash type", v, hf)
	}
)

// ApplyHashFunction applies the specified hash function to the input byte slice.
// It returns the hashed byte slice and an error, if any.
//
// Parameters:
//   - inp: The input byte slice to hash.
//   - hf: The hash function to apply.
//
// Returns:
//   - []interface: The hashed byte slice.
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
			h := murmur3.Sum32(buf)
			return uint64(h), nil

		case qdb.ColumnTypeUinteger:
			buf := make([]byte, 8)
			binary.PutUvarint(buf, inp.(uint64))
			h := murmur3.Sum32(buf)
			return uint64(h), nil
		case qdb.ColumnTypeVarcharHashed:
			switch v := inp.(type) {
			case []byte:
				h := murmur3.Sum32(v)

				return uint64(h), nil
			case string:
				h := murmur3.Sum32([]byte(v))

				return uint64(h), nil
			default:
				return nil, errUnknownValueType(inp, hf)
			}
		default:
			return nil, errUnknownColumnType(ctype, hf)
		}
	case HashFunctionCity:
		switch ctype {
		case qdb.ColumnTypeInteger:
			buf := make([]byte, 8)
			binary.PutVarint(buf, inp.(int64))
			h := city.Hash32(buf)
			return uint64(h), nil

		case qdb.ColumnTypeUinteger:
			buf := make([]byte, 8)
			binary.PutUvarint(buf, inp.(uint64))
			h := city.Hash32(buf)
			return uint64(h), nil
		case qdb.ColumnTypeVarcharHashed:
			switch v := inp.(type) {
			case []byte:
				h := city.Hash32(v)

				return uint64(h), nil
			case string:
				h := city.Hash32([]byte(v))

				return uint64(h), nil
			default:
				return nil, errUnknownValueType(inp, hf)
			}
		default:
			return nil, errUnknownColumnType(ctype, hf)
		}
	default:
		return nil, fmt.Errorf("unknown hash function type: %d", hf)
	}
}

/*
* Apply routing hash function on bytes receieved in their string representation (from COPY).
 */
func ApplyHashFunctionOnStringRepr(inp []byte, ctype string, hf HashFunctionType) (interface{}, error) {

	var parsedInp interface{}

	/*
	* We need to convert raw bytes to approrpiate interface
	* because caller expect data in form compatable with CompareKeyRange
	 */
	switch ctype {
	case qdb.ColumnTypeInteger:
		n, err := strconv.ParseInt(string(inp), 10, 64)
		if err != nil {
			return nil, err
		}
		parsedInp = n
	case qdb.ColumnTypeUinteger:
		n, err := strconv.ParseUint(string(inp), 10, 64)
		if err != nil {
			return nil, err
		}
		parsedInp = n

	case qdb.ColumnTypeVarchar:
		fallthrough
	case qdb.ColumnTypeVarcharHashed:
		fallthrough
	case qdb.ColumnTypeVarcharDeprecated:
		parsedInp = string(inp)
	}

	return ApplyHashFunction(parsedInp, ctype, hf)
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
		return 0, fmt.Errorf("unknown hash function type: %s", hfn)
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
