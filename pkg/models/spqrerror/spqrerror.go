package spqrerror

import "fmt"

const (
	SPQR_UNEXPECTED        = "SPQRU"
	SPQR_NO_DATASHARD      = "SPQRD"
	SPQR_SKIP              = "SPQRE"
	SPQR_COMPLEX_QUERY     = "SPQRC"
	SPQR_FAILED_MATCH      = "SPQRF"
	SPQR_SKIP_COLUMN       = "SPQRS"
	SPQR_MISS_SHARDING_KEY = "SPQRM"
	SPQR_CROSS_SHARD_QUERY = "SPQRX"
	SPQR_ROUTING_ERROR     = "SPQRR"
)

var existingErrorCodeMap = map[string]string{
	"SPQRD": "failed to match any datashard",
	"SQPRE": "skip executing this query, wait for next",
	"SPQRC": "ComplexQuery",
	"SPQRF": "FailedToMatch",
	"SPQRS": "SkipColumn",
	"SPQRM": "ShardingKeysMissing",
	"SPQRX": "CrossShardQueryUnsupported",
	"SPQRR": "Routing error",
}

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

func NewSpqrError(errorMsg string, errorCode string) SpqrError {
	err := SpqrError{
		Err:       fmt.Errorf(errorMsg),
		ErrorCode: errorCode,
	}
	return err
}

func (er *SpqrError) Error() string {
	return fmt.Sprintf("Code: %s. Name: %s. Description: %s.",
		er.ErrorCode, GetMessageByCode(er.ErrorCode), er.Err)
}
