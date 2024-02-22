package hashfunction

import (
	"fmt"
	"strconv"

	"github.com/go-faster/city"
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

func ApplyHashFunction(inp []byte, hf HashFunctionType) ([]byte, error) {
	switch hf {
	case HashFunctionIdent:
		return inp, nil
	case HashFunctionMurmur:
		h := murmur3.Sum32(inp)
		return []byte(strconv.FormatUint(uint64(h), 10)), nil
	case HashFunctionCity:
		h := city.Hash32(inp)
		return []byte(strconv.FormatUint(uint64(h), 10)), nil
	default:
		return nil, errNoSuchHashFunction
	}
}

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
