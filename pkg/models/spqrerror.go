package models

import "fmt"

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

func getMessageByCode(errorCode string) string {
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
		er.ErrorCode, getMessageByCode(er.ErrorCode), er.Err)
}
