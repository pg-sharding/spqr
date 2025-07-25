package rerrors

import "fmt"

var ErrComplexQuery = fmt.Errorf("too complex query to route")
var ErrExecutorSyncLost = fmt.Errorf("sync lost in execution phase")
var ErrInformationSchemaCombinedQuery = fmt.Errorf("combined information schema and regular relation is not supported")
