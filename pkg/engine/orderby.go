package engine

import (
	"fmt"
	"sort"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/catalog"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

func ProcessOrderBy(data [][][]byte, colOrder map[string]int, order lyx.Node) ([][][]byte, error) {

	for _, r := range data {
		spqrlog.Zero.Debug().Str("data", string(r[0])).Msg("print row before")
	}

	ord, ok := order.(*lyx.SortBy)
	if ok {
		var ascDesc int

		switch ord.SortbyDir {
		case lyx.SORTBY_ASC:
			ascDesc = ASC
		case lyx.SORTBY_DESC:
			ascDesc = DESC
		case lyx.SORTBY_DEFAULT:
			ascDesc = ASC
		default:
			return nil, fmt.Errorf("wrong sorting option (asc/desc)")
		}
		/*XXX: very hacky*/
		op, err := SearchSysCacheOperator(catalog.TEXTOID)
		if err != nil {
			return nil, err
		}
		colRef, ok := ord.Node.(*lyx.ColumnRef)
		if !ok {
			return nil, fmt.Errorf("unsupported ORDER BY node type %T", ord.Node)
		}
		sortable := SortableWithContext{
			Data:     data,
			ColIndex: colOrder[colRef.ColName],
			Order:    ascDesc,
			Op:        op,
		}
		sort.Sort(sortable)
	}

	for _, r := range data {
		spqrlog.Zero.Debug().Str("data", string(r[0])).Msg("print row after")
	}
	return data, nil
}
