package datatransfers

import (
	pgx "github.com/jackc/pgx/v5"
	_ "github.com/lib/pq"
)

type Tx interface {
	pgx.Tx
}
