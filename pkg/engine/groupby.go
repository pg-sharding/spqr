package engine

import (
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/tupleslot"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

// Outputs groupBy get list values and counts its 'groupByCol' property.
// 'groupByCol' sorted in grouped result by string key ASC mode
//
// Parameters:
// - values []T: list of objects for grouping
// - getters (map[string]toString[T]): getters which gets object property as string
// - groupByCol string: property names for counting
// - pi *PSQLInteractor:  output object
// Returns:
// - error: An error if there was a problem dropping the sequence.
func groupBy(tts *tupleslot.TupleTableSlot, groupByCols []string) (*tupleslot.TupleTableSlot, error) {
	groups := make(map[string][][][]byte)
	for _, row := range tts.Raw {
		key := ""
		for _, groupByCol := range groupByCols {
			off, err := tts.ColNameOffset(groupByCol)
			if err != nil {
				return nil, err
			}
			key = fmt.Sprintf("%s:-:%s", key, string(row[off]))

		}
		groups[key] = append(groups[key], row)
	}

	colDescs := make([]pgproto3.FieldDescription, 0, len(groupByCols)+1)
	for _, groupByCol := range groupByCols {
		colDescs = append(colDescs, TextOidFD(groupByCol))
	}

	keys := make([]string, 0, len(groups))
	for groupKey := range groups {
		keys = append(keys, groupKey)
	}
	sort.Strings(keys)

	resTTS := &tupleslot.TupleTableSlot{
		Desc: append(colDescs, IntOidFD("count")),
	}

	for _, key := range keys {
		group := groups[key]
		cols := strings.Split(key, ":-:")[1:]

		resTTS.WriteDataRow(append(cols, fmt.Sprintf("%d", len(group)))...)
	}
	return resTTS, nil
}

func GroupBy(tts *tupleslot.TupleTableSlot, gb spqrparser.GroupByClause) (*tupleslot.TupleTableSlot, error) {

	switch gb := gb.(type) {
	case spqrparser.GroupBy:
		groupByCols := []string{}
		for _, col := range gb.Col {
			groupByCols = append(groupByCols, col.ColName)
		}
		tts, err := groupBy(tts, groupByCols)
		if err != nil {
			return nil, err
		}
		return tts, nil
	case spqrparser.GroupByClauseEmpty:
		return tts, nil
	default:
		return nil, spqrerror.NewByCode(spqrerror.SPQR_INVALID_REQUEST)
	}
}
