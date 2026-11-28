package engine

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/catalog"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
)

const (
	CountAgg  = "count"
	MinAgg    = "min"
	MaxAgg    = "max"
	SumAgg    = "sum"
	ExistsAgg = "exists"
)

type AggregateState interface {
	Init(oid int) error

	Aggregate(v []byte) error

	Finalize() any
}

type CountAggregate struct {
	v   int64
	oid int
}

// Aggregate implements [AggregateState].
func (c *CountAggregate) Aggregate(v []byte) error {
	switch c.oid {
	case catalog.INT4OID, catalog.INT8OID:
		n, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return err
		}
		c.v += n
	}
	return nil
}

// Finalize implements [AggregateState].
func (c *CountAggregate) Finalize() any {
	return c.v
}

// Init implements [AggregateState].
func (c *CountAggregate) Init(oid int) error {
	c.v = 0
	c.oid = oid
	return nil
}

var _ AggregateState = &CountAggregate{}

type ExistsAggregate struct {
	v []byte
}

// Aggregate implements [AggregateState].
func (c *ExistsAggregate) Aggregate(v []byte) error {
	c.v = bytes.Clone(BoolOr(c.v, v))
	return nil
}

// Finalize implements [AggregateState].
func (c *ExistsAggregate) Finalize() any {
	if len(c.v) == 1 && (c.v[0] == 't' || c.v[0] == 1) {
		return "t"
	}
	return "f"
}

// Init implements [AggregateState].
func (c *ExistsAggregate) Init(_ int) error {
	c.v = []byte{'f'}
	return nil
}

var _ AggregateState = &ExistsAggregate{}

type SumAggregate struct {
	oid    int
	seen   bool
	intSum int64
}

func (s *SumAggregate) Init(oid int) error {
	*s = SumAggregate{oid: oid}
	switch oid {
	case catalog.INT4OID, catalog.INT8OID:
		return nil
	default:
		return spqrerror.Newf(spqrerror.SPQR_NOT_IMPLEMENTED, "sum result type %d is not supported", oid)
	}
}

func (s *SumAggregate) Aggregate(v []byte) error {
	if v == nil {
		return nil
	}
	s.seen = true

	bitSize := 64
	if s.oid == catalog.INT4OID {
		bitSize = 32
	}
	n, err := strconv.ParseInt(string(v), 10, bitSize)
	if err != nil {
		return err
	}
	maxValue := int64(1<<63 - 1)
	minValue := int64(-1 << 63)
	if s.oid == catalog.INT4OID {
		maxValue = 1<<31 - 1
		minValue = -1 << 31
	}
	if (n > 0 && s.intSum > maxValue-n) || (n < 0 && s.intSum < minValue-n) {
		return fmt.Errorf("integer out of range in sum")
	}
	s.intSum += n
	return nil
}

func (s *SumAggregate) Finalize() any {
	if !s.seen {
		return nil
	}
	if s.oid == catalog.INT4OID {
		return int32(s.intSum)
	}
	return s.intSum
}

var _ AggregateState = &SumAggregate{}

func EngineV2AggregateFunction(n string) bool {
	switch strings.ToLower(n) {
	case CountAgg, MaxAgg, MinAgg, SumAgg, ExistsAgg:
		return true
	default:
		return false
	}
}

func CreateAggregate(name string, _ []lyx.Node) (AggregateState, error) {
	switch strings.ToLower(name) {
	case CountAgg:
		return &CountAggregate{}, nil
	case ExistsAgg:
		return &ExistsAggregate{}, nil
	case SumAgg:
		return &SumAggregate{}, nil
	default:
		return nil, spqrerror.Newf(spqrerror.SPQR_UNEXPECTED, "not supported aggregate %s", name)
	}
}
