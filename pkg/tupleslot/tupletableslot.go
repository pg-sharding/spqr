package tupleslot

import "github.com/jackc/pgx/v5/pgproto3"

type TupleTableSlot struct {
	Desc []pgproto3.FieldDescription

	//

	Raw [][][]byte
}
