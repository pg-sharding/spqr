package engine

import (
	"fmt"

	"github.com/pg-sharding/spqr/pkg/tupleslot"
)

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
