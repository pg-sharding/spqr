package spqrerror

import "fmt"

const (
	SPQR_UNEXPECTED          = "SPQRU"
	SPQR_NO_DATASHARD        = "SPQRD"
	SPQR_SKIP                = "SPQRE"
	SPQR_COMPLEX_QUERY       = "SPQRC"
	SPQR_FAILED_MATCH        = "SPQRF"
	SPQR_SKIP_COLUMN         = "SPQRS"
	SPQR_MISS_SHARDING_KEY   = "SPQRM"
	SPQR_CROSS_SHARD_QUERY   = "SPQRX"
	SPQR_ROUTING_ERROR       = "SPQRR"
	SPQR_CONNECTION_ERROR    = "SPQRO"
	SPQR_KEYRANGE_ERROR      = "SPQRK"
	SPQR_SHARDING_RULE_ERROR = "SPQRH"
	SPQR_TRANSFER_ERROR      = "SPQRT"
	SPQR_NO_DISTRIBUTION     = "SPQRN"
	SPQR_NOT_IMPLEMENTED     = "SPQRI"
	SPQR_ROUTER_ERROR        = "SPQRL"
	SPQR_METADATA_CORRUPTION = "SPQRZ"
	SPQR_INVALID_REQUEST     = "SPQRJ"
)

var existingErrorCodeMap = map[string]string{
	SPQR_NO_DATASHARD:        "failed to match any datashard",
	SPQR_SKIP:                "skip executing this query, wait for next",
	SPQR_COMPLEX_QUERY:       "ComplexQuery",
	SPQR_FAILED_MATCH:        "FailedToMatch",
	SPQR_SKIP_COLUMN:         "SkipColumn",
	SPQR_MISS_SHARDING_KEY:   "ShardingKeysMissing",
	SPQR_CROSS_SHARD_QUERY:   "CrossShardQueryUnsupported",
	SPQR_ROUTING_ERROR:       "Routing error",
	SPQR_CONNECTION_ERROR:    "Connection error",
	SPQR_KEYRANGE_ERROR:      "Keyrange error",
	SPQR_SHARDING_RULE_ERROR: "Sharding rule error",
	SPQR_TRANSFER_ERROR:      "Transfer error",
	SPQR_NO_DISTRIBUTION:     "No distribution",
	SPQR_NOT_IMPLEMENTED:     "Not implemented",
	SPQR_ROUTER_ERROR:        "Router error",
	SPQR_METADATA_CORRUPTION: "routing metadata corrupted",
	SPQR_INVALID_REQUEST:     "Invalid Request",
}

var ShardingKeysRemoved = New(SPQR_INVALID_REQUEST, "sharding rules are removed from SPQR, see https://github.com/pg-sharding/spqr/blob/master/docs/Syntax.md")

func GetMessageByCode(errorCode string) string {
	rep, ok := existingErrorCodeMap[errorCode]
	if ok {
		return rep
	}
	return "Unexpected error"
}

var _ error = &SpqrError{}

type SpqrError struct {
	Err error

	ErrorCode string
}

func New(errorCode string, errorMsg string) *SpqrError {
	err := &SpqrError{
		Err:       fmt.Errorf(errorMsg),
		ErrorCode: errorCode,
	}
	return err
}

func NewByCode(errorCode string) *SpqrError {
	err := &SpqrError{
		Err:       fmt.Errorf(GetMessageByCode(errorCode)),
		ErrorCode: errorCode,
	}
	return err
}

func Newf(errorCode string, format string, a ...any) *SpqrError {
	err := &SpqrError{
		Err:       fmt.Errorf(format, a...),
		ErrorCode: errorCode,
	}
	return err
}

func (er *SpqrError) Error() string {
	return fmt.Sprintf("%s.", er.Err.Error())
}
