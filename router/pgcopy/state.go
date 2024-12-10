package pgcopy

import (
	"github.com/pg-sharding/spqr/pkg/models/hashfunction"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/router/routingstate"
)

type CopyState struct {
	ExpRoute   *routingstate.DataShardRoute
	Delimiter  byte
	TargetType string

	/* For replicated relations */
	Scatter bool

	/* For distributed relations */
	ColumnOffset    int
	AllowMultishard bool
	Krs             []*kr.KeyRange
	HashFunc        hashfunction.HashFunctionType
}
