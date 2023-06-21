package console

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/clientinteractor"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

func ProcessQueryConsole(ctx context.Context, cli *clientinteractor.PSQLInteractor, q string, mgr meta.EntityMgr) error {
	tstmt, err := spqrparser.Parse(q)
	if err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	spqrlog.Logger.Printf(spqrlog.DEBUG1, "console statement '%s', parsed %T", q, tstmt)

	return meta.Proc(ctx, tstmt, mgr, cli)
}
