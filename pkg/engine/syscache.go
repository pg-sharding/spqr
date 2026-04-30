package engine

import (
	"github.com/pg-sharding/spqr/pkg/catalog"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
)

var sOperators = map[int]Operator{
	catalog.TEXTOID: &TEXTOperator{},
}

func SearchSysCacheOperator(oid int) (Operator, error) {
	if op, ok := sOperators[oid]; ok {
		return op, nil
	}
	return nil, spqrerror.New(spqrerror.SPQR_NOT_IMPLEMENTED, "operator not supported")
}
