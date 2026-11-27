package engine

import (
	"bytes"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/catalog"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
)

const (
	CountAgg = "count"
	MinAgg   = "min"
	MaxAgg   = "max"
	SumAgg   = "sum"
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
	oid      int
	seen     bool
	intSum   int64
	floatSum float64
	numeric  decimalSum
}

func (s *SumAggregate) Init(oid int) error {
	*s = SumAggregate{oid: oid}
	switch oid {
	case catalog.INT8OID, catalog.FLOAT4OID, catalog.DOUBLEOID, catalog.NUMERICOID:
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

	switch s.oid {
	case catalog.INT8OID:
		n, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return err
		}
		const maxInt64 = 1<<63 - 1
		const minInt64 = -1 << 63
		if (n > 0 && s.intSum > maxInt64-n) || (n < 0 && s.intSum < minInt64-n) {
			return fmt.Errorf("bigint out of range in sum")
		}
		s.intSum += n
	case catalog.FLOAT4OID:
		n, err := strconv.ParseFloat(string(v), 32)
		if err != nil {
			return err
		}
		s.floatSum += n
	case catalog.DOUBLEOID:
		n, err := strconv.ParseFloat(string(v), 64)
		if err != nil {
			return err
		}
		s.floatSum += n
	case catalog.NUMERICOID:
		if err := s.numeric.Add(string(v)); err != nil {
			return err
		}
	}
	return nil
}

func (s *SumAggregate) Finalize() any {
	if !s.seen {
		return nil
	}
	switch s.oid {
	case catalog.INT8OID:
		return s.intSum
	case catalog.FLOAT4OID:
		return formatFloat(float64(float32(s.floatSum)), 32)
	case catalog.DOUBLEOID:
		return formatFloat(s.floatSum, 64)
	case catalog.NUMERICOID:
		return s.numeric.String()
	default:
		return nil
	}
}

var _ AggregateState = &SumAggregate{}

type decimalSum struct {
	value   big.Rat
	scale   int
	special string
}

func (d *decimalSum) Add(value string) error {
	special := numericSpecial(value)
	if special != "" {
		switch {
		case d.special == "NaN" || special == "NaN":
			d.special = "NaN"
		case d.special == "Infinity" && special == "-Infinity":
			d.special = "NaN"
		case d.special == "-Infinity" && special == "Infinity":
			d.special = "NaN"
		case d.special == "":
			d.special = special
		}
		return nil
	}
	if d.special != "" {
		return nil
	}

	var n big.Rat
	if _, ok := n.SetString(value); !ok {
		return fmt.Errorf("invalid numeric value %q", value)
	}
	scale, err := decimalScale(value)
	if err != nil {
		return err
	}
	d.value.Add(&d.value, &n)
	if scale > d.scale {
		d.scale = scale
	}
	return nil
}

func (d *decimalSum) String() string {
	if d.special != "" {
		return d.special
	}
	return d.value.FloatString(d.scale)
}

func decimalScale(value string) (int, error) {
	exponent := int64(0)
	if i := strings.IndexAny(value, "eE"); i >= 0 {
		parsed, err := strconv.ParseInt(value[i+1:], 10, 32)
		if err != nil {
			return 0, err
		}
		exponent = parsed
		value = value[:i]
	}
	if i := strings.IndexByte(value, '.'); i >= 0 {
		value = value[i+1:]
	} else {
		value = ""
	}
	scale := int64(len(value)) - exponent
	if scale < 0 {
		return 0, nil
	}
	if scale > 16383 {
		return 0, fmt.Errorf("numeric scale out of range")
	}
	return int(scale), nil
}

func numericSpecial(value string) string {
	switch strings.ToLower(value) {
	case "nan":
		return "NaN"
	case "infinity", "+infinity":
		return "Infinity"
	case "-infinity":
		return "-Infinity"
	default:
		return ""
	}
}

func formatFloat(value float64, bitSize int) string {
	switch {
	case math.IsNaN(value):
		return "NaN"
	case math.IsInf(value, 1):
		return "Infinity"
	case math.IsInf(value, -1):
		return "-Infinity"
	default:
		return strconv.FormatFloat(value, 'f', -1, bitSize)
	}
}

func EngineV2AggregateFunction(n string) bool {
	switch n {
	case CountAgg, MaxAgg, MinAgg, SumAgg, ExistsAgg:
		return true
	default:
		return false
	}
}

func CreateAggregate(name string, _ []lyx.Node) (AggregateState, error) {
	switch name {
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
