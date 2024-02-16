package qrouter

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/clientinteractor"
	"github.com/pg-sharding/spqr/pkg/spqrlog"

	"github.com/pg-sharding/lyx/lyx"
)

// TODO : unit tests
func (qr *ProxyQrouter) Explain(ctx context.Context, stmt *lyx.Explain, cli *clientinteractor.PSQLInteractor) error {
	meta := NewRoutingMetadataContext(nil, nil)

	switch node := stmt.Stmt.(type) {
	case *lyx.VariableSetStmt:
		/*
		* SET x = y etc, do not dispatch any statement to shards, just process this in router
		 */

		return cli.ReportStmtRoutedToAllShards(ctx)
	case *lyx.CreateTable:
		// XXX: need alter table which renames sharding column to non-sharding column check
		/*
		* Disallow to create table which does not contain any sharding column
		 */
		if err := qr.CheckTableIsRoutable(ctx, node, meta); err != nil {
			return cli.ReportError(err)
		}
		return cli.ReportStmtRoutedToAllShards(ctx)

	case *lyx.Index:
		/*
		* Disallow to index on table which does not contain any sharding column
		 */
		// XXX: doit
		return cli.ReportStmtRoutedToAllShards(ctx)
	case *lyx.Alter, *lyx.Drop, *lyx.Truncate:
		// support simple ddl commands, route them to every chard
		// this is not fully ACID (not atomic at least)
		return cli.ReportStmtRoutedToAllShards(ctx)
	// case *pgquery.Node_DropdbStmt, *pgquery.Node_DropRoleStmt:
	// 	// forbid under separate setting
	// 	return cli.ReportStmtRoutedToAllShards(ctx)
	case *lyx.CreateDatabase, *lyx.CreateRole:
		// forbid under separate setting
		// XXX: need alter table which renames sharding column to non-sharding column check
		return cli.ReportStmtRoutedToAllShards(ctx)

	case *lyx.Insert:
		err := qr.deparseShardingMapping(ctx, node, meta)
		if err != nil {
			if qr.cfg.MulticastUnroutableInsertStatement {
				switch err {
				case ShardingKeysMissing:
					return cli.ReportStmtRoutedToAllShards(ctx)
				}
			}
			return cli.ReportError(err)
		}
	default:
		// SELECT, UPDATE and/or DELETE stmts, which
		// would be routed with their WHERE clause
		err := qr.deparseShardingMapping(ctx, node, meta)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
			return cli.ReportError(err)
		}
	}

	return ReportStmtDeparsedAttrs(ctx, cli, meta)
}

// TODO : unit tests
func ReportStmtDeparsedAttrs(ctx context.Context, pi *clientinteractor.PSQLInteractor, meta *RoutingMetadataContext) error {
	if err := pi.WriteHeader("explain query"); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("")
		return err
	}

	return pi.CompleteMsg(0)
}
