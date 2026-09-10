package engine

import (
	"fmt"

	"github.com/pg-sharding/spqr/pkg/tupleslot"
	"github.com/pg-sharding/lyx/lyx"
)

// ExtractProjectionColumns returns column names from a target list,
// or nil if any target is not a plain ColumnRef (e.g. `*`, expressions).
func ExtractProjectionColumns(targetList []lyx.Node) []string {
	columns := []string{}
	for _, tle := range targetList {
		var cr *lyx.ColumnRef
		switch el := tle.(type) {
		case *lyx.ColumnRef:
			cr = el
		case *lyx.ResTarget:
			if c, ok := el.Value.(*lyx.ColumnRef); ok {
				cr = c
			}
		}
		if cr == nil || cr.ColName == "*" || cr.ColName == "" {
			return nil
		}
		columns = append(columns, cr.ColName)
	}
	if len(columns) == 0 {
		return nil
	}
	return columns
}

func Project(tts *tupleslot.TupleTableSlot, columns []string) (*tupleslot.TupleTableSlot, error) {
	/* Do tuple projection */
	if columns != nil {
		colMp := tts.Desc.GetColumnsMap()
		offsets := []int{}

		tuplesProjected := &tupleslot.TupleTableSlot{}

		for _, c := range columns {
			off, ok := colMp[c]
			if !ok {
				return &tupleslot.TupleTableSlot{}, fmt.Errorf("no such column %s", c)
			}
			offsets = append(offsets, off)
			tuplesProjected.Desc = append(tuplesProjected.Desc, tts.Desc[off])
		}

		for _, r := range tts.Raw {
			rowProjection := [][]byte{}
			for _, off := range offsets {
				rowProjection = append(rowProjection, r[off])
			}
			tuplesProjected.Raw = append(tuplesProjected.Raw, rowProjection)
		}

		return tuplesProjected, nil
	} /* nil means all cols */

	return tts, nil
}
