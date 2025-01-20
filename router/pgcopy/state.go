package pgcopy

import (
	"github.com/pg-sharding/spqr/pkg/models/hashfunction"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/router/rmeta"
)

type CopyState struct {
	RM *rmeta.RoutingMetadataContext

	ExpRoute   *kr.ShardKey
	Delimiter  byte
	TargetType string

	/* For replicated relations */
	Scatter bool

	/* execute on/in explicit tx */
	Attached bool

	/* For distributed relations */
	ColumnOffset int
	Krs          []*kr.KeyRange
	HashFunc     hashfunction.HashFunctionType
}
