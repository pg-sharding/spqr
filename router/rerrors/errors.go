package rerrors

import (
	"fmt"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
)

var ErrComplexQuery = spqrerror.Newf(spqrerror.SPQR_NOT_IMPLEMENTED, "too complex query to route")
var ErrExecutorSyncLost = fmt.Errorf("sync lost in execution phase")
var ErrRelaySyncLost = fmt.Errorf("relay sync lost")
var ErrInformationSchemaCombinedQuery = fmt.Errorf("combined information schema and regular relation is not supported")
