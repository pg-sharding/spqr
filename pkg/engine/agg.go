package engine

import (
	"strconv"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/catalog"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
)

const (
	CountAgg = "count"
	MinAgg   = "min"
	MaxAgg   = "max"
	SumAgg   = "sum"
)

type AggregateState interface {
	Init(oid int)

	Aggregate(v []byte)

	Finalize() any
}

type CountAggregate struct {
	v   int64
	oid int
}

// Aggregate implements [AggregateState].
func (c *CountAggregate) Aggregate(v []byte) {
	switch c.oid {
	case catalog.INT4OID:
		n, _ := strconv.ParseInt(string(v), 10, 64)
		c.v += n
	case catalog.INT8OID:
		n, _ := strconv.ParseInt(string(v), 10, 64)
		c.v += n
	}
}

// Finalize implements [AggregateState].
func (c *CountAggregate) Finalize() any {
	return c.v
}

// Init implements [AggregateState].
func (c *CountAggregate) Init(oid int) {
	c.v = 0
	c.oid = oid
}

var _ AggregateState = &CountAggregate{}

func EngineV2AggregateFunction(n string) bool {
	switch n {
	case CountAgg, MaxAgg, MinAgg, SumAgg:
		return true
	default:
		return false
	}
}

func CreateAggregate(name string, args []lyx.Node) (AggregateState, error) {
	switch name {
	case CountAgg:
		return &CountAggregate{}, nil
	default:
		return nil, spqrerror.Newf(spqrerror.SPQR_UNEXPECTED, "not supported aggregate %s", name)
	}
}
