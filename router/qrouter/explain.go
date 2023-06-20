package qrouter

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/clientinteractor"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	pgquery "github.com/pganalyze/pg_query_go/v4"
)

func (qr *ProxyQrouter) Explain(ctx context.Context, stmt *pgquery.RawStmt, cli *clientinteractor.PSQLInteractor) error {
	meta := NewRoutingMetadataContext()

	switch node := stmt.Stmt.Node.(type) {
	case *pgquery.Node_VariableSetStmt:
		/*
		* SET x = y etc, do not dispatch any statement to shards, just process this in router
		 */

		return cli.ReportStmtRoutedToAllShards(ctx)
	case *pgquery.Node_CreateStmt: // XXX: need alter table which renames sharding column to non-sharding column check
		/*
		* Disallow to create table which does not contain any sharding column
		 */
		if err := qr.CheckTableIsRoutable(ctx, node); err != nil {
			return cli.ReportError(err)
		}
		return cli.ReportStmtRoutedToAllShards(ctx)

	case *pgquery.Node_IndexStmt:
		/*
		* Disallow to index on table which does not contain any sharding column
		 */
		// XXX: doit
		return cli.ReportStmtRoutedToAllShards(ctx)
	case *pgquery.Node_AlterTableStmt, *pgquery.Node_DropStmt, *pgquery.Node_TruncateStmt:
		// support simple ddl commands, route them to every chard
		// this is not fully ACID (not atomic at least)
		return cli.ReportStmtRoutedToAllShards(ctx)
	case *pgquery.Node_DropdbStmt, *pgquery.Node_DropRoleStmt:
		// forbid under separate setting
		return cli.ReportStmtRoutedToAllShards(ctx)
	case *pgquery.Node_CreateRoleStmt, *pgquery.Node_CreatedbStmt:
		// forbid under separate setting
		return cli.ReportStmtRoutedToAllShards(ctx)
	case *pgquery.Node_InsertStmt:
		err := qr.deparseShardingMapping(ctx, stmt, meta)
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
		err := qr.deparseShardingMapping(ctx, stmt, meta)
		if err != nil {
			spqrlog.Logger.Errorf("parse error %v", err)
			return cli.ReportError(err)
		}
	}

	return ReportStmtDeparsedAttrs(ctx, cli, meta)
}

func ReportStmtDeparsedAttrs(ctx context.Context, pi *clientinteractor.PSQLInteractor, meta *RoutingMetadataContext) error {
	if err := pi.WriteHeader("explain query"); err != nil {
		spqrlog.Logger.PrintError(err)
		return err
	}

	return pi.CompleteMsg(0)
}
