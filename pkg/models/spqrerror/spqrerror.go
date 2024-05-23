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

// GetMessageByCode returns the error message associated with the provided error code.
// If the error code is not found in the existingErrorCodeMap, the function returns "Unexpected error".
//
// Parameters:
//   - errorCode: The error code for which to retrieve the error message.
//
// Returns:
//   - string: The error message associated with the provided error code.
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

// New creates a new SpqrError with the given error code and error message.
// It returns a pointer to the created SpqrError.
//
// Parameters:
//   - errorCode: The error code for the error.
//   - errorMsg: The error message for the error.
//
// Returns:
//   - *SpqrError: The created SpqrError.
func New(errorCode string, errorMsg string) *SpqrError {
	err := &SpqrError{
		Err:       fmt.Errorf(errorMsg),
		ErrorCode: errorCode,
	}
	return err
}

// NewByCode creates a new SpqrError instance based on the provided error code.
// It returns a pointer to the created SpqrError.
//
// Parameters:
//   - errorCode: The error code for the error.
//
// Returns:
//   - *SpqrError: The created SpqrError.
func NewByCode(errorCode string) *SpqrError {
	err := &SpqrError{
		Err:       fmt.Errorf(GetMessageByCode(errorCode)),
		ErrorCode: errorCode,
	}
	return err
}

// Newf creates a new SpqrError with the given error code and formatted error message.
// It uses the fmt.Errorf function to format the error message using the provided format and arguments.
// The error code is used to identify the specific type of error.
//
// Parameters:
//   - errorCode: The error code for the error.
//   - format: The format string for the error message.
//   - a: The arguments to be used in the format string.
//
// Returns:
//   - *SpqrError: The created SpqrError.
func Newf(errorCode string, format string, a ...any) *SpqrError {
	err := &SpqrError{
		Err:       fmt.Errorf(format, a...),
		ErrorCode: errorCode,
	}
	return err
}

// Error returns the error message associated with the SpqrError.
// It formats the error message using the underlying error's Error method.
//
// Returns:
//   - string: The formatted error message.
func (er *SpqrError) Error() string {
	return fmt.Sprintf("%s.", er.Err.Error())
}
