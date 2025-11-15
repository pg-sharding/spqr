package engine

import (
	"fmt"
	"sort"

	"github.com/pg-sharding/spqr/pkg/catalog"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

func ProcessOrderBy(data [][][]byte, colOrder map[string]int, order spqrparser.OrderClause) ([][][]byte, error) {

	switch order.(type) {
	case *spqrparser.Order:
		ord := order.(*spqrparser.Order)
		var asc_desc int

		switch ord.OptAscDesc.(type) {
		case *spqrparser.SortByAsc:
			asc_desc = ASC
		case *spqrparser.SortByDesc:
			asc_desc = DESC
		case *spqrparser.SortByDefault, nil:
			asc_desc = ASC
		default:
			return nil, fmt.Errorf("wrong sorting option (asc/desc)")
		}
		/*XXX: very hacky*/
		op, err := SearchSysCacheOperator(catalog.TEXTOID)
		if err != nil {
			return nil, err
		}
		sortable := SortableWithContext{
			Data:      data,
			Col_index: colOrder[ord.Col.ColName],
			Order:     asc_desc,
			Op:        op,
		}
		sort.Sort(sortable)
	}
	return data, nil
}
