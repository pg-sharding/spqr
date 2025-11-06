package tupleslot

import "github.com/jackc/pgx/v5/pgproto3"

type TupleTableSlot struct {
	Desc []pgproto3.FieldDescription

	//

	Raw [][][]byte
}

func (tts *TupleTableSlot) WriteDataRow(msgs ...string) {
	vals := make([][]byte, 0)
	for _, msg := range msgs {
		vals = append(vals, []byte(msg))
	}
	tts.Raw = append(tts.Raw, vals)
}
