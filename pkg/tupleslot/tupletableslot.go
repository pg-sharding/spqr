package tupleslot

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
)

type TupleDesc []pgproto3.FieldDescription

type TupleTableSlot struct {
	Desc TupleDesc

	//

	Raw [][][]byte
}

func (tts *TupleTableSlot) ColNameOffset(n string) (int, error) {
	for i, d := range tts.Desc {
		if string(d.Name) == n {
			return i, nil
		}
	}
	return -1, fmt.Errorf("failed to resolve '%s' column offset", n)
}

func (tts *TupleTableSlot) WriteDataRow(msgs ...string) {
	vals := make([][]byte, 0)
	for _, msg := range msgs {
		vals = append(vals, []byte(msg))
	}
	tts.Raw = append(tts.Raw, vals)
}

// GetColumnsMap generates a map that maps column names to their respective indices in the table description header.
//
// Parameters:
// - desc (TableDesc): The table description.
//
// Returns:
// - map[string]int: A map that maps column names to their respective indices in the table description header.

func (td TupleDesc) GetColumnsMap() map[string]int {
	columns := make(map[string]int, len(td))
	i := 0
	for _, key := range td {
		columns[string(key.Name)] = i
		i++
	}
	return columns
}
