package spqrerror

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5/pgproto3"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

//revive:disable:var-naming
const (
	SPQR_UNEXPECTED           = "SPQRU"
	SPQR_NO_DATASHARD         = "SPQRD"
	SPQR_SKIP                 = "SPQRE"
	SPQR_COMPLEX_QUERY        = "SPQRC"
	SPQR_FAILED_MATCH         = "SPQRF"
	SPQR_SKIP_COLUMN          = "SPQRS"
	SPQR_MISS_SHARDING_KEY    = "SPQRM"
	SPQR_CROSS_SHARD_QUERY    = "SPQRX"
	SPQR_ROUTING_ERROR        = "SPQRR"
	SPQR_CONNECTION_ERROR     = "SPQRO"
	SPQR_KEYRANGE_ERROR       = "SPQRK"
	SPQR_TRANSFER_ERROR       = "SPQRT"
	SPQR_OBJECT_NOT_EXIST     = "SPQRN"
	SPQR_NOT_IMPLEMENTED      = "SPQRI"
	SPQR_ROUTER_ERROR         = "SPQRL"
	SPQR_METADATA_CORRUPTION  = "SPQRZ"
	SPQR_INVALID_REQUEST      = "SPQRJ"
	SPQR_CONFIG_ERROR         = "SPQRM"
	SPQR_SEQUENCE_ERROR       = "SPQRQ"
	SPQR_STOP_MOVE_TASK_GROUP = "SPQRA"
	SPQR_QUERY_BLOCKED        = "SPQRB"
	SPQR_VALUE_ERROR          = "SPQRV"

	PG_ACTIVE_SQL_TRANSACTION    = "25001"
	PG_NO_ACTIVE_SQL_TRANSACTION = "25P01"

	PG_ERRCODE_PROTOCOL_VIOLATION         = "08P01"
	PG_PREPARED_STATEMENT_DOES_NOT_EXISTS = "26000"
	PG_PORTAL_DOES_NOT_EXISTS             = "34000"

	PG_ERRCODE_UNDEFINED_TABLE = "42P01"
	PG_SYNTAX_ERROR            = "42601"
)

//revive:enable:var-naming

var ExistingErrorCodeMap = map[string]string{
	SPQR_NO_DATASHARD:         "failed to match any datashard",
	SPQR_SKIP:                 "skip executing this query, wait for next",
	SPQR_COMPLEX_QUERY:        "ComplexQuery",
	SPQR_SKIP_COLUMN:          "SkipColumn",
	SPQR_MISS_SHARDING_KEY:    "ShardingKeysMissing",
	SPQR_CROSS_SHARD_QUERY:    "CrossShardQueryUnsupported",
	SPQR_ROUTING_ERROR:        "Routing error",
	SPQR_CONNECTION_ERROR:     "Connection error",
	SPQR_KEYRANGE_ERROR:       "Keyrange error",
	SPQR_TRANSFER_ERROR:       "Transfer data error",
	SPQR_OBJECT_NOT_EXIST:     "No object",
	SPQR_NOT_IMPLEMENTED:      "Not implemented",
	SPQR_ROUTER_ERROR:         "Router error",
	SPQR_METADATA_CORRUPTION:  "routing metadata corrupted",
	SPQR_INVALID_REQUEST:      "Invalid Request",
	SPQR_SEQUENCE_ERROR:       "Sequence error",
	SPQR_STOP_MOVE_TASK_GROUP: "Task group stopped",
	SPQR_QUERY_BLOCKED:        "query is blocked due to the default_route_behavior",
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

	ErrorCode     string
	Position      int32
	InternalQuery string
	ErrHint       string
	ErrDetail     string
	ErrContext    string
}

const (
	grpcErrorDomain       = "spqr"
	grpcErrorCodeKey      = "code"
	grpcErrorHintKey      = "hint"
	grpcErrorDetailKey    = "detail"
	grpcErrorContextKey   = "context"
	grpcErrorPositionKey  = "position"
	grpcErrorQueryKey     = "internal_query"
	shardNotFoundHintText = "Run 'SHOW shards' to see all configured shards."
)

func (e *SpqrError) Hint(h string) *SpqrError {
	e.ErrHint = h
	return e
}

func (e *SpqrError) Query(q string) *SpqrError {
	e.InternalQuery = q
	return e
}

func (e *SpqrError) Code(c string) *SpqrError {
	e.ErrorCode = c
	return e
}

func (e *SpqrError) Detail(d string) *SpqrError {
	e.ErrDetail = d
	return e
}

func (e *SpqrError) Context(c string) *SpqrError {
	e.ErrContext = c
	return e
}

func (e *SpqrError) Pos(p int32) *SpqrError {
	e.Position = p
	return e
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

func ShardNotFound(shardID string) *SpqrError {
	return Newf(SPQR_NO_DATASHARD, "Shard %q not found.", shardID).Hint(shardNotFoundHintText)
}

// Error returns the error message associated with the SpqrError.
// It formats the error message using the underlying error's Error method.
//
// Returns:
//   - string: The formatted error message.
func (e *SpqrError) Error() string {
	return e.Err.Error()
}

func ToGrpcError(err error) error {
	if err == nil {
		return nil
	}

	var spErr *SpqrError
	if !errors.As(err, &spErr) {
		return err
	}

	metadata := map[string]string{
		grpcErrorCodeKey: spErr.ErrorCode,
	}
	if spErr.ErrHint != "" {
		metadata[grpcErrorHintKey] = spErr.ErrHint
	}
	if spErr.ErrDetail != "" {
		metadata[grpcErrorDetailKey] = spErr.ErrDetail
	}
	if spErr.ErrContext != "" {
		metadata[grpcErrorContextKey] = spErr.ErrContext
	}
	if spErr.Position != 0 {
		metadata[grpcErrorPositionKey] = strconv.FormatInt(int64(spErr.Position), 10)
	}
	if spErr.InternalQuery != "" {
		metadata[grpcErrorQueryKey] = spErr.InternalQuery
	}

	st, detailErr := status.New(codes.Unknown, spErr.Error()).WithDetails(&errdetails.ErrorInfo{
		Reason:   spErr.ErrorCode,
		Domain:   grpcErrorDomain,
		Metadata: metadata,
	})
	if detailErr != nil {
		return err
	}

	return st.Err()
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
		for _, detail := range st.Details() {
			info, ok := detail.(*errdetails.ErrorInfo)
			if !ok || info.Domain != grpcErrorDomain {
				continue
			}

			code := info.Reason
			if code == "" {
				code = info.Metadata[grpcErrorCodeKey]
			}
			if code == "" {
				break
			}

			spErr := New(code, st.Message()).
				Hint(info.Metadata[grpcErrorHintKey]).
				Detail(info.Metadata[grpcErrorDetailKey]).
				Context(info.Metadata[grpcErrorContextKey]).
				Query(info.Metadata[grpcErrorQueryKey])

			if position, parseErr := strconv.ParseInt(info.Metadata[grpcErrorPositionKey], 10, 32); parseErr == nil {
				spErr.Position = int32(position)
			}

			return spErr
		}
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
