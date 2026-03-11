package engine

import (
	"fmt"
	"sort"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/catalog"
)

func ProcessOrderBy(data [][][]byte, colOrder map[string]int, order lyx.Node) ([][][]byte, error) {

	switch order.(type) {
	case *lyx.SortBy:
		ord := order.(*lyx.SortBy)
		var asc_desc int

		switch ord.SortbyDir {
		case lyx.SORTBY_ASC:
			asc_desc = ASC
		case lyx.SORTBY_DESC:
			asc_desc = DESC
		case lyx.SORTBY_DEFAULT:
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
			Col_index: colOrder[ord.Node.(*lyx.ColumnRef).ColName],
			Order:     asc_desc,
			Op:        op,
		}
		sort.Sort(sortable)
	}
	return data, nil
}
