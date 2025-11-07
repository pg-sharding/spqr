package engine

import (
	"github.com/pg-sharding/spqr/pkg/catalog"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
)

var s_operators = map[int]Operator{
	catalog.TEXTOID: &TEXTOperator{},
}

func SearchSysCacheOperator(oid int) (Operator, error) {
	if op, ok := s_operators[oid]; ok {
		return op, nil
	}
	return nil, spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "operator not supported")
}
