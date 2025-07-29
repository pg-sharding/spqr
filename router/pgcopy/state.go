package pgcopy

import (
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/hashfunction"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/router/rmeta"
)

type CopyState struct {
	RM *rmeta.RoutingMetadataContext

	Delimiter byte
	Ds        *distributions.Distribution

	/* For replicated relations */
	Scatter          bool
	ExecutionTargets []kr.ShardKey

	/* execute on/in explicit tx */
	Attached bool

	/* For distributed relations */
	SchemaColumnMp map[string]int
	ColTypesMp     map[string]string
	SchemaColumns  []string
	Krs            []*kr.KeyRange
	HashFunc       []hashfunction.HashFunctionType
}
