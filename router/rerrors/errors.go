package rerrors

import (
	"fmt"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
)

func NewComplexQueryError() *spqrerror.SpqrError {
	return spqrerror.Newf(spqrerror.SPQR_NOT_IMPLEMENTED, "too complex query to route")
}

func NewExecutorSyncLostError() *spqrerror.SpqrError {
	return spqrerror.Newf(spqrerror.SPQR_UNEXPECTED, "sync lost in execution phase")
}

var ErrRelaySyncLost = fmt.Errorf("relay sync lost")
var ErrInformationSchemaCombinedQuery = fmt.Errorf("combined information schema and regular relation is not supported")
