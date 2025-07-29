package hashfunction

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-faster/city"
	"github.com/google/uuid"
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

func EncodeUInt64(input uint64) []byte {
	const ENCODING_BYTES_BIG = binary.MaxVarintLen64
	const ENCODING_BYTES = 8
	const BOUND = 1 << 56 /* 72057594037927936 */

	sz := ENCODING_BYTES
	if input >= BOUND {
		sz = ENCODING_BYTES_BIG
	}

	buf := make([]byte, sz)
	binary.PutUvarint(buf, input)
	return buf
}

func ApplyMurmurHashFunction(input any, ctype string) (uint32, error) {
	switch ctype {
	case qdb.ColumnTypeInteger:
		if res, ok := input.(int64); ok {
			buf := EncodeUInt64(uint64(res))
			h := murmur3.Sum32(buf)
			return h, nil
		} else {
			return 0, fmt.Errorf("invalid type for murmurhash '%s'", qdb.ColumnTypeInteger)
		}
	case qdb.ColumnTypeUinteger:
		if res, ok := input.(uint64); ok {
			buf := EncodeUInt64(res)
			h := murmur3.Sum32(buf)
			return h, nil
		} else {
			return 0, fmt.Errorf("invalid type for murmurhash '%s'", qdb.ColumnTypeUinteger)
		}
	case qdb.ColumnTypeVarcharHashed:
		switch v := input.(type) {
		case []byte:
			h := murmur3.Sum32(v)

			return h, nil
		case string:
			h := murmur3.Sum32([]byte(v))

			return h, nil
		default:
			return 0, errUnknownValueType(input, HashFunctionMurmur)
		}
	default:
		return 0, errUnknownColumnType(ctype, HashFunctionMurmur)
	}
}

func ApplyCityHashFunction(input any, ctype string) (uint32, error) {
	switch ctype {
	case qdb.ColumnTypeInteger:
		if res, ok := input.(int64); ok {
			buf := EncodeUInt64(uint64(res))
			h := city.Hash32(buf)
			return h, nil
		} else {
			return 0, fmt.Errorf("invalid type for cityhash '%s'", qdb.ColumnTypeInteger)
		}
	case qdb.ColumnTypeUinteger:
		if res, ok := input.(uint64); ok {
			buf := EncodeUInt64(res)
			h := city.Hash32(buf)
			return h, nil
		} else {
			return 0, fmt.Errorf("invalid type for cityhash '%s'", qdb.ColumnTypeUinteger)
		}
	case qdb.ColumnTypeVarcharHashed:
		switch v := input.(type) {
		case []byte:
			h := city.Hash32(v)

			return h, nil
		case string:
			h := city.Hash32([]byte(v))

			return h, nil
		default:
			return 0, errUnknownValueType(input, HashFunctionCity)
		}
	default:
		return 0, errUnknownColumnType(ctype, HashFunctionCity)
	}
}

func ApplyHashFunction(input any, ctype string, hf HashFunctionType) (any, error) {

	switch hf {
	case HashFunctionIdent:
		if ctype == qdb.ColumnTypeUUID {
			if err := uuid.Validate(strings.ToLower(input.(string))); err != nil {
				return nil, err
			}
		}
		return input, nil
	case HashFunctionMurmur:
		v, err := ApplyMurmurHashFunction(input, ctype)
		return uint64(v), err
	case HashFunctionCity:
		v, err := ApplyCityHashFunction(input, ctype)
		return uint64(v), err
	default:
		return nil, fmt.Errorf("unknown hash function type: %d", hf)
	}
}

/*
* Apply routing hash function on bytes received in their string representation (from COPY).
 */
func ApplyHashFunctionOnStringRepr(input []byte, ctype string, hf HashFunctionType) (interface{}, error) {

	var parsedInput any

	/*
	* We need to convert raw bytes to appropriate interface
	* because caller expect data in form compatable with CompareKeyRange
	 */
	switch ctype {
	case qdb.ColumnTypeInteger:
		n, err := strconv.ParseInt(string(input), 10, 64)
		if err != nil {
			return nil, err
		}
		parsedInput = n
	case qdb.ColumnTypeUinteger:
		n, err := strconv.ParseUint(string(input), 10, 64)
		if err != nil {
			return nil, err
		}
		parsedInput = n

	case qdb.ColumnTypeUUID:
		parsedInput = string(input)
	case qdb.ColumnTypeVarchar:
		fallthrough
	case qdb.ColumnTypeVarcharHashed:
		fallthrough
	case qdb.ColumnTypeVarcharDeprecated:
		parsedInput = string(input)
	}

	return ApplyHashFunction(parsedInput, ctype, hf)
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
