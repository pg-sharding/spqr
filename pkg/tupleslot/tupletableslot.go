package tupleslot

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
)

type TupleTableSlot struct {
	Desc []pgproto3.FieldDescription

	//

	Raw [][][]byte
}

func (tts *TupleTableSlot) ColNameOffset(n string) (int, error) {
	for i, d := range tts.Desc {
		if string(d.Name) == n {
			return i, nil
		}
	}
	return -1, fmt.Errorf("failed to resolve %s column offset", n)
}

func (tts *TupleTableSlot) WriteDataRow(msgs ...string) {
	vals := make([][]byte, 0)
	for _, msg := range msgs {
		vals = append(vals, []byte(msg))
	}
	tts.Raw = append(tts.Raw, vals)
}
