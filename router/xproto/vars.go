package xproto

import "github.com/jackc/pgx/v5/pgproto3"

const (
	FormatCodeText   = int16(0)
	FormatCodeBinary = int16(1)
)

var (
	PGexec   = &pgproto3.Execute{}
	PGsync   = &pgproto3.Sync{}
	PGNoData = &pgproto3.NoData{}

	PortalClose = &pgproto3.Close{
		ObjectType: 'P',
	}
)
