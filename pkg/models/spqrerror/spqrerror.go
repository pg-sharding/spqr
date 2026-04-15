package spqrerror

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
	"google.golang.org/grpc/status"
)

const (
	SpqrUnexpected           = "SPQRU"
	SpqrNoDatashard         = "SPQRD"
	SpqrSkip                 = "SPQRE"
	SpqrComplexQuery        = "SPQRC"
	SpqrFailedMatch         = "SPQRF"
	SpqrSkipColumn          = "SPQRS"
	SpqrMissShardingKey    = "SPQRM"
	SpqrCrossShardQuery    = "SPQRX"
	SpqrRoutingError        = "SPQRR"
	SpqrConnectionError     = "SPQRO"
	SpqrKeyrangeError       = "SPQRK"
	SpqrTransferError       = "SPQRT"
	SpqrObjectNotExist     = "SPQRN"
	SpqrNotImplemented      = "SPQRI"
	SpqrRouterError         = "SPQRL"
	SpqrMetadataCorruption  = "SPQRZ"
	SpqrInvalidRequest      = "SPQRJ"
	SpqrConfigError         = "SPQRM"
	SpqrSequenceError       = "SPQRQ"
	SpqrStopMoveTaskGroup = "SPQRA"
	SpqrQueryBlocked        = "SPQRB"

	PgActiveSQLTransaction    = "25001"
	PgNoActiveSQLTransaction = "25P01"

	PgErrcodeProtocolViolation         = "08P01"
	PgPreparedStatementDoesNotExist = "26000"
	PgPortalDoesNotExist             = "34000"

	PgErrcodeUndefinedTable = "42P01"
	PgSyntaxError            = "42601"
)

var ExistingErrorCodeMap = map[string]string{
	SpqrNoDatashard:         "failed to match any datashard",
	SpqrSkip:                 "skip executing this query, wait for next",
	SpqrComplexQuery:        "ComplexQuery",
	SpqrSkipColumn:          "SkipColumn",
	SpqrMissShardingKey:    "ShardingKeysMissing",
	SpqrCrossShardQuery:    "CrossShardQueryUnsupported",
	SpqrRoutingError:        "Routing error",
	SpqrConnectionError:     "Connection error",
	SpqrKeyrangeError:       "Keyrange error",
	SpqrTransferError:       "Transfer data error",
	SpqrObjectNotExist:     "No object",
	SpqrNotImplemented:      "Not implemented",
	SpqrRouterError:         "Router error",
	SpqrMetadataCorruption:  "routing metadata corrupted",
	SpqrInvalidRequest:      "Invalid Request",
	SpqrSequenceError:       "Sequence error",
	SpqrStopMoveTaskGroup: "Task group stopped",
	SpqrQueryBlocked:        "query is blocked due to the default_route_behavior",
}

// GetMessageByCode returns the error message associated with the provided error code.
// If the error code is not found in the existingErrorCodeMap, the function returns "Unexpected error".
//
// Parameters:
//   - errorCode: The error code for which to retrieve the error message.
//
// Returns:
//   - string: The error message associated with the provided error code.
func GetMessageByCode(errorCode string) string {
	rep, ok := ExistingErrorCodeMap[errorCode]
	if ok {
		return rep
	}
	return "Unexpected error"
}

var _ error = &SpqrError{}

type SpqrError struct {
	Err error

	ErrorCode string
	Position  int32
	ErrHint   string
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
		Err:       fmt.Errorf("%s", errorMsg),
		ErrorCode: errorCode,
	}
	return err
}

func NewWithHint(errorCode string, errorMsg string, errhint string) *SpqrError {
	err := &SpqrError{
		Err:       fmt.Errorf("%s", errorMsg),
		ErrorCode: errorCode,
		ErrHint:   errhint,
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
		Err:       fmt.Errorf("%s", GetMessageByCode(errorCode)),
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
	return er.Err.Error()
}

// Try convert grpc error to error without "rpc error: code..."
//
// Returns:
//   - error: non grpc error.
func CleanGrpcError(err error) error {
	if err == nil {
		return nil
	}
	if st, ok := status.FromError(err); ok {
		return fmt.Errorf("%s", st.Message())
	}
	return err // non grpc error
}

func ErrorMsgFromErr(
	msg string,
	code string,
	hint string, pos int32) *pgproto3.ErrorResponse {

	return &pgproto3.ErrorResponse{
		Message:  msg,
		Severity: "ERROR",
		Code:     code,
		Hint:     hint,
		Position: pos,
	}
}
