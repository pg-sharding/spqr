package pgcopy

import (
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/router/routingstate"
)

type CopyState struct {
	ExpRoute        *routingstate.DataShardRoute
	Delimiter       byte
	TargetType      string
	ColumnOffset    int
	AllowMultishard bool
	Krs             []*kr.KeyRange
}
