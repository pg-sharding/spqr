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
	noSuchHashFunction = fmt.Errorf("no such hash function")
)

func ApplyHashFunction(inp []byte, hf HashFunctionType) ([]byte, error) {
	switch hf {
	case HashFunctionIdent:
		return inp, nil
	case HashFunctionMurmur:
		h := murmur3.Sum64(inp)
		return []byte(strconv.FormatUint(h, 10)), nil
	case HashFunctionCity:
		h := city.Hash64(inp)
		return []byte(strconv.FormatUint(h, 10)), nil
	default:
		return nil, noSuchHashFunction
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
		return 0, noSuchHashFunction
	}
}
