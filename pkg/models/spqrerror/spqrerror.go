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
	SPQR_CONNECTION_ERROR  = "SPQRO"
)

var existingErrorCodeMap = map[string]string{
	SPQR_NO_DATASHARD:      "failed to match any datashard",
	SPQR_SKIP:              "skip executing this query, wait for next",
	SPQR_COMPLEX_QUERY:     "ComplexQuery",
	SPQR_FAILED_MATCH:      "FailedToMatch",
	SPQR_SKIP_COLUMN:       "SkipColumn",
	SPQR_MISS_SHARDING_KEY: "ShardingKeysMissing",
	SPQR_CROSS_SHARD_QUERY: "CrossShardQueryUnsupported",
	SPQR_ROUTING_ERROR:     "Routing error",
	SPQR_CONNECTION_ERROR:  "Connection error",
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
