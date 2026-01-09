package relay

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/prepstatement"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/parser"
	"github.com/pg-sharding/spqr/router/rerrors"
	"github.com/pg-sharding/spqr/router/twopc"
)

func AdvancedPoolModeNeeded(rst RelayStateMgr) bool {
	return rst.Client().Rule().PoolMode == config.PoolModeTransaction && rst.Client().Rule().PoolPreparedStatement || config.RouterMode(config.RouterConfig().RouterMode) == config.ProxyMode
}

func ReplyVirtualParamState(cl client.Client, name string, val []byte) {
	/* TODO: handle errors */
	_ = cl.Send(
		&pgproto3.RowDescription{
			Fields: []pgproto3.FieldDescription{
				{
					Name:         []byte(name),
					DataTypeOID:  25,
					DataTypeSize: -1,
					TypeModifier: -1,
				},
			},
		},
	)

	_ = cl.Send(
		&pgproto3.DataRow{
			Values: [][]byte{
				[]byte(val),
			},
		},
	)
}

var errAbortedTx = fmt.Errorf("current transaction is aborted, commands ignored until end of transaction block")

func (rst *RelayStateImpl) ProcQueryAdvancedTx(query string, binderQ func() error, doCaching, completeRelay bool) (*PortalDesc, error) {

	state, comment, err := rst.Parse(query, doCaching)
	if err != nil {
		if rst.QueryExecutor().TxStatus() == txstatus.TXACT {
			/* this way we format next msg correctly */
			rst.QueryExecutor().SetTxStatus(txstatus.TXERR)
		}

		if rst.QueryExecutor().TxStatus() == txstatus.TXERR {
			// TODO: figure out if we need this
			// _ = rst.Reset()
			return nil, rst.Client().ReplyErrWithTxStatus(err, txstatus.TXERR)
		}

		if rst.QueryExecutor().TxStatus() == txstatus.TXACT {
			return nil, rst.Client().ReplyErrWithTxStatus(err, txstatus.TXERR)
		}

		return nil, rst.ResetWithError(err)
	}

	txbefore := rst.QueryExecutor().TxStatus()
	if txbefore == txstatus.TXERR {

		/* If user supplied COMMIT in already-errored tx, simply rollback
		* and end tx block. */
		if _, ok := state.(parser.ParseStateTXCommit); ok {
			/* It is necessary here to change state to trigger correct
			* execution path ProcQueryAdvanced, that is, single-slice scatter-out
			* query (no 2pc commit management!) */
			state = parser.ParseStateTXRollback{}
			/* We will actually send COMMIT as use command to shards, do not
			* override `query` */
		} else {
			if _, ok := state.(parser.ParseStateTXRollback); !ok {
				return nil, rst.Client().ReplyErrWithTxStatus(errAbortedTx, txstatus.TXERR)
			}
		}
	}

	pd, err := rst.ProcQueryAdvanced(query, state, comment, binderQ, doCaching)

	if txbefore != txstatus.TXIDLE && err != nil {
		rst.QueryExecutor().SetTxStatus(txstatus.TXERR)
	}

	/* outer function will complete relay here */
	if err != nil {
		spqrlog.Zero.Error().Err(err).Uint("client-id", rst.Client().ID()).Msg("completing client relay with error")
	} else {
		spqrlog.Zero.Debug().Uint("client-id", rst.Client().ID()).Msg("completing client relay")
	}

	switch err {
	case nil:
		if !completeRelay {
			return pd, nil
		}

		/* Okay, respond with CommandComplete first. */
		if err := rst.QueryExecutor().DeriveCommandComplete(); err != nil {
			return nil, err
		}

		return pd, rst.CompleteRelay()
	case io.ErrUnexpectedEOF:
		fallthrough
	case io.EOF:
		err := rst.Reset()
		return nil, err
		// ok
	default:
		spqrlog.Zero.Error().
			Uint("client", rst.Client().ID()).Int("tx-status", int(rst.QueryExecutor().TxStatus())).Err(err).
			Msg("client iteration done with error")

		rerr := fmt.Errorf("client processing error: %v, tx status %s", err, rst.QueryExecutor().TxStatus().String())

		if rst.QueryExecutor().TxStatus() == txstatus.TXERR {
			return nil, rst.Client().ReplyErrWithTxStatus(rerr, txstatus.TXERR)
		}

		return nil, rst.ResetWithError(rerr)
	}
}

func (rst *RelayStateImpl) queryProc(comment string, binderQ func() error) error {
	mp, err := parser.ParseComment(comment)

	if err == nil {
		for key, val := range mp {
			switch key {
			case session.SPQR_TARGET_SESSION_ATTRS_ALIAS_2:
				fallthrough
			case session.SPQR_TARGET_SESSION_ATTRS_ALIAS:
				fallthrough
			case session.SPQR_TARGET_SESSION_ATTRS:
				// TBD: validate value
				spqrlog.Zero.Debug().Str("tsa", val).Msg("parse tsa from comment")
				rst.Client().SetTsa(session.VirtualParamLevelStatement, val)
			case session.SPQR_DEFAULT_ROUTE_BEHAVIOUR:
				spqrlog.Zero.Debug().Str("default route", val).Msg("parse default route behaviour from comment")
				rst.Client().SetDefaultRouteBehaviour(session.VirtualParamLevelStatement, val)
			case session.SPQR_SHARDING_KEY:
				spqrlog.Zero.Debug().Str("sharding key", val).Msg("parse sharding key from comment")
				rst.Client().SetShardingKey(session.VirtualParamLevelStatement, val)
			case session.SPQR_DISTRIBUTION:
				spqrlog.Zero.Debug().Str("distribution", val).Msg("parse distribution from comment")
				rst.Client().SetDistribution(session.VirtualParamLevelStatement, val)
			case session.SPQR_DISTRIBUTED_RELATION:
				spqrlog.Zero.Debug().Str("distributed relation", val).Msg("parse distributed relation from comment")
				rst.Client().SetDistributedRelation(session.VirtualParamLevelStatement, val)
			case session.SPQR_SCATTER_QUERY:
				/* any non-empty value of SPQR_SCATTER_QUERY is local and means ON */
				spqrlog.Zero.Debug().Str("scatter query", val).Msg("parse scatter query from comment")
				rst.Client().SetScatterQuery(val != "")
			case session.SPQR_EXECUTE_ON:

				if _, ok := config.RouterConfig().ShardMapping[val]; !ok {
					return fmt.Errorf("no such shard: %v", val)
				}
				rst.Client().SetExecuteOn(session.VirtualParamLevelStatement, val)
			case session.SPQR_ENGINE_V2:
				switch val {
				case "true", "ok", "on":
					rst.Client().SetEnhancedMultiShardProcessing(session.VirtualParamLevelStatement, true)
				case "false", "no", "off":
					rst.Client().SetEnhancedMultiShardProcessing(session.VirtualParamLevelStatement, false)
				}
			case session.SPQR_PREFERRED_ENGINE:
				spqrlog.Zero.Debug().Str("preferred engine", val).Msg("parse preferred engine from comment")
				rst.Client().SetPreferredEngine(session.VirtualParamLevelStatement, val)
			case session.SPQR_AUTO_DISTRIBUTION:
				/* Should we create distributed or reference relation? */

				if val == distributions.REPLICATED {
					/* This is an ddl query, which creates relation along with attaching to REPLICATED distribution */
					rst.Client().SetAutoDistribution(val)
				} else {
					if valDistrib, ok := mp[session.SPQR_DISTRIBUTION_KEY]; ok {
						_, err = rst.QueryRouter().Mgr().GetDistribution(context.TODO(), val)
						if err != nil {
							return err
						}

						/* This is an ddl query, which creates relation along with attaching to distribution */
						rst.Client().SetAutoDistribution(val)
						rst.Client().SetDistributionKey(valDistrib)

						/* this is too early to do anything with distribution hint, as we do not yet parsed
						* DDL of about-to-be-created relation
						 */
					} else {
						return fmt.Errorf("spqr distribution specified, but distribution key omitted")
					}
				}
			}
		}
	}

	return binderQ()
}

var (
	noDataPd = &PortalDesc{
		nodata: &pgproto3.NoData{},
	}
)

// ProcQueryAdvanced processes query, with router relay state
// There are several types of query that we want to process in non-passthrough way.
// For example, after BEGIN we wait until first client query witch can be router to some shard.
// So, we need to process SETs, BEGINs, ROLLBACKs etc ourselves.
// QueryStateExecutor provides set of function for either simple of extended protoc interactions
// query param is either plain query from simple proto or bind query from x proto
func (rst *RelayStateImpl) ProcQueryAdvanced(query string, state parser.ParseState, comment string, binderQ func() error, doCaching bool) (*PortalDesc, error) {
	startTime := time.Now()

	/* !!! Do not complete relay here (no TX status management) !!! */

	spqrlog.Zero.Debug().Str("query", query).Uint("client", rst.Client().ID()).Msgf("process relay state advanced")

	switch st := state.(type) {
	case parser.ParseStateTXBegin:

		if rst.QueryExecutor().TxStatus() != txstatus.TXIDLE {
			// ignore this
			_ = rst.Client().ReplyWarningf("there is already transaction in progress")
			return noDataPd, rst.QueryExecutor().ReplyCommandComplete("BEGIN")
		}
		err := rst.QueryExecutor().ExecBegin(rst, query, &st)
		spqrlog.SLogger.ReportStatement(spqrlog.StmtTypeQuery, query, time.Since(startTime))
		return noDataPd, err
	case parser.ParseStateTXCommit:

		if mp, err := parser.ParseComment(comment); err == nil {

			if val, ok := mp[session.SPQR_COMMIT_STRATEGY]; ok {
				switch val {
				case twopc.COMMIT_STRATEGY_2PC:
					fallthrough
				case twopc.COMMIT_STRATEGY_1PC:
					fallthrough
				case twopc.COMMIT_STRATEGY_BEST_EFFORT:
					rst.Client().SetCommitStrategy(val)
				default:
					/*should error-out*/
				}
			}
		}

		if rst.QueryExecutor().TxStatus() != txstatus.TXACT && rst.QueryExecutor().TxStatus() != txstatus.TXERR {
			_ = rst.Client().ReplyWarningf("there is no transaction in progress")
			return noDataPd, rst.QueryExecutor().ReplyCommandComplete("COMMIT")
		}
		err := rst.QueryExecutor().ExecCommit(rst, query)
		spqrlog.SLogger.ReportStatement(spqrlog.StmtTypeQuery, query, time.Since(startTime))
		return noDataPd, err
	case parser.ParseStateTXRollback:
		if rst.QueryExecutor().TxStatus() != txstatus.TXACT && rst.QueryExecutor().TxStatus() != txstatus.TXERR {
			_ = rst.Client().ReplyWarningf("there is no transaction in progress")
			return noDataPd, rst.QueryExecutor().ReplyCommandComplete("ROLLBACK")
		}
		err := rst.QueryExecutor().ExecRollback(rst, query)
		spqrlog.SLogger.ReportStatement(spqrlog.StmtTypeQuery, query, time.Since(startTime))
		return noDataPd, err
	case parser.ParseStateEmptyQuery:
		rst.QueryExecutor().ReplyEmptyQuery()
		// do not complete relay  here
		return noDataPd, nil
	// with tx pooling we might have no active connection while processing set x to y
	case parser.ParseStateShowStmt:
		var pd *PortalDesc

		for i, stmt := range st.Stmts {

			/* This is hacky and very-very bad. Should fix multi-statement. */
			if i > 0 {
				if err := rst.QueryExecutor().DeriveCommandComplete(); err != nil {
					return nil, err
				}
			}

			q, ok := stmt.(*lyx.VariableShowStmt)
			if !ok {
				return nil, rerrors.ErrComplexQuery
			}

			param := virtualParamTransformName(q.Name)

			// manually create router response
			// here we just reply single row with single column value

			pd = &PortalDesc{
				rd: &pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte(q.Name),
							DataTypeOID:  25,
							DataTypeSize: -1,
							TypeModifier: -1,
						},
					},
				},
			}

			switch param {
			case session.SPQR_DISTRIBUTION:
				err := rst.Client().Send(
					&pgproto3.ErrorResponse{
						Message: fmt.Sprintf("parameter \"%s\" isn't user accessible",
							session.SPQR_DISTRIBUTION),
						Severity: "ERROR",
						Code:     spqrerror.SPQR_NOT_IMPLEMENTED,
					})
				spqrlog.SLogger.ReportStatement(spqrlog.StmtTypeQuery, query, time.Since(startTime))
				return pd, err
			case session.SPQR_DISTRIBUTED_RELATION:
				err := rst.Client().Send(
					&pgproto3.ErrorResponse{
						Message: fmt.Sprintf("parameter \"%s\" isn't user accessible",
							session.SPQR_DISTRIBUTED_RELATION),
						Severity: "ERROR",
						Code:     spqrerror.SPQR_NOT_IMPLEMENTED,
					})
				spqrlog.SLogger.ReportStatement(spqrlog.StmtTypeQuery, query, time.Since(startTime))
				return pd, err

			case session.SPQR_DEFAULT_ROUTE_BEHAVIOUR:
				ReplyVirtualParamState(rst.Client(), "default route behaviour", []byte(rst.Client().DefaultRouteBehaviour()))
			case session.SPQR_REPLY_NOTICE:

				if rst.Client().ShowNoticeMsg() {
					ReplyVirtualParamState(rst.Client(), "show notice messages", []byte("true"))
				} else {
					ReplyVirtualParamState(rst.Client(), "show notice messages", []byte("false"))
				}

			case session.SPQR_MAINTAIN_PARAMS:

				if rst.Client().MaintainParams() {
					ReplyVirtualParamState(rst.Client(), "maintain params", []byte("true"))
				} else {
					ReplyVirtualParamState(rst.Client(), "maintain params", []byte("false"))
				}

			case session.SPQR_SHARDING_KEY:
				ReplyVirtualParamState(rst.Client(), "sharding key", []byte(rst.Client().ShardingKey()))
			case session.SPQR_SCATTER_QUERY:
				err := rst.Client().Send(
					&pgproto3.ErrorResponse{
						Message: fmt.Sprintf("parameter \"%s\" isn't user accessible",
							session.SPQR_SCATTER_QUERY),
						Severity: "ERROR",
						Code:     spqrerror.SPQR_NOT_IMPLEMENTED,
					})
				spqrlog.SLogger.ReportStatement(spqrlog.StmtTypeQuery, query, time.Since(startTime))
				return pd, err
			case session.SPQR_EXECUTE_ON:
				ReplyVirtualParamState(rst.Client(), "execute on", []byte(rst.Client().ExecuteOn()))
			case session.SPQR_ENGINE_V2:
				if rst.Client().EnhancedMultiShardProcessing() {
					ReplyVirtualParamState(rst.Client(), "engine v2", []byte("on"))
				} else {
					ReplyVirtualParamState(rst.Client(), "engine v2", []byte("off"))
				}
			case session.SPQR_TARGET_SESSION_ATTRS:
				fallthrough
			case session.SPQR_TARGET_SESSION_ATTRS_ALIAS:
				fallthrough
			case session.SPQR_TARGET_SESSION_ATTRS_ALIAS_2:
				ReplyVirtualParamState(rst.Client(), "target session attrs", []byte(rst.Client().GetTsa()))
			case session.SPQR_PREFERRED_ENGINE:
				ReplyVirtualParamState(rst.Client(), "preferred engine", []byte(rst.Client().PreferredEngine()))
			default:

				if strings.HasPrefix(param, "__spqr__") {
					ReplyVirtualParamState(rst.Client(), param, []byte(rst.Client().Params()[param]))
				} else {
					/* If router does dot have any info about param, fire query to random shard. */
					if _, ok := rst.Client().Params()[param]; !ok {
						err := rst.queryProc(comment, binderQ)
						spqrlog.SLogger.ReportStatement(spqrlog.StmtTypeQuery, query, time.Since(startTime))
						return pd, err
					}

					ReplyVirtualParamState(rst.Client(), param, []byte(rst.Client().Params()[param]))
				}
			}

			if err := rst.QueryExecutor().ReplyCommandComplete("SHOW"); err != nil {
				return nil, err
			}
		}

		spqrlog.SLogger.ReportStatement(spqrlog.StmtTypeQuery, query, time.Since(startTime))
		return pd, nil
	case parser.ParseStateSetStmt:

		for i, stmt := range st.Stmts {

			/* This is hacky and very-very bad. Should fix multi-statement. */
			if i > 0 {
				if err := rst.QueryExecutor().DeriveCommandComplete(); err != nil {
					return nil, err
				}
			}

			q, ok := stmt.(*lyx.VariableSetStmt)
			if !ok {
				return nil, rerrors.ErrComplexQuery
			}
			// XXX: TODO: support
			// if q.IsLocal {
			// 	// ignore for now
			// }

			switch q.Kind {
			case lyx.VarTypeResetAll:
				rst.Client().ResetAll()
				if err := rst.QueryExecutor().ReplyCommandComplete("RESET"); err != nil {
					return nil, err
				}
			case lyx.VarTypeReset:
				switch q.Name {
				case "session_authorization", "role":

					if err := rst.QueryExecutor().ExecResetMetadata(rst, query, q.Name); err != nil {
						return nil, err
					}

					rst.Client().ResetParam(q.Name)
					if q.Name == "session_authorization" {
						rst.Client().ResetParam("role")
					}

					if err := rst.QueryExecutor().ReplyCommandComplete("RESET"); err != nil {
						return nil, err
					}

				default:

					param := virtualParamTransformName(q.Name)
					switch param {
					case session.SPQR_TARGET_SESSION_ATTRS:
						fallthrough
					case session.SPQR_TARGET_SESSION_ATTRS_ALIAS:
						rst.Client().ResetTsa()
					default:
						rst.Client().ResetParam(param)

						if err := rst.QueryExecutor().ExecReset(rst, query, param); err != nil {
							return nil, err
						}
					}

					if err := rst.QueryExecutor().ReplyCommandComplete("RESET"); err != nil {
						return nil, err
					}

				}
			/* TBD: support multi-set */
			// case pgquery.VariableSetKind_VAR_SET_MULTI:
			// 	qp.state = ParseStateSetLocalStmt{}
			// 	return qp.state, comment, nil
			case lyx.VarTypeSet, "":
				name := q.Name
				val := ""
				if len(q.Value) > 0 {
					val = q.Value[0]
				}

				if strings.HasPrefix(name, "__spqr__") {
					ctx := context.TODO()
					if err := rst.processSpqrHint(ctx, name, val, q.IsLocal); err != nil {
						return nil, err
					}
				} else {
					if err := rst.QueryExecutor().ExecSet(rst, query, name, val); err != nil {
						return nil, err
					}
				}
			}

		}

		spqrlog.SLogger.ReportStatement(spqrlog.StmtTypeQuery, query, time.Since(startTime))
		return noDataPd, nil
	case parser.ParseStatePrepareStmt:
		// sql level prepares stmt pooling
		if AdvancedPoolModeNeeded(rst) {
			spqrlog.Zero.Debug().Msg("sql level prep statement pooling support is on")

			/* no oid for SQL level prep stmt */
			rst.Client().StorePreparedStatement(&prepstatement.PreparedStatementDefinition{
				Name:  st.Name,
				Query: st.Query,
			})
			spqrlog.SLogger.ReportStatement(spqrlog.StmtTypeQuery, query, time.Since(startTime))
			return nil, nil
		} else {
			// process like regular query
			err := rst.queryProc(comment, binderQ)
			spqrlog.SLogger.ReportStatement(spqrlog.StmtTypeQuery, query, time.Since(startTime))
			return nil, err
		}
	case parser.ParseStateExecute:
		if AdvancedPoolModeNeeded(rst) {
			// do nothing
			// wtf? TODO: test and fix
			rst.Client().PreparedStatementQueryByName(st.Name)
			spqrlog.SLogger.ReportStatement(spqrlog.StmtTypeQuery, query, time.Since(startTime))
			return nil, nil
		} else {
			// process like regular query
			err := rst.queryProc(comment, binderQ)
			spqrlog.SLogger.ReportStatement(spqrlog.StmtTypeQuery, query, time.Since(startTime))
			return nil, err
		}
	default:
		err := rst.queryProc(comment, binderQ)
		spqrlog.SLogger.ReportStatement(spqrlog.StmtTypeQuery, query, time.Since(startTime))
		return nil, err
	}
}

func (rst *RelayStateImpl) processSpqrHint(ctx context.Context, hintName string,
	hintVal string, isLocal bool) error {
	name := virtualParamTransformName(hintName)
	value := strings.ToLower(hintVal)

	lvl := session.VirtualParamLevelTxBlock

	if isLocal {
		lvl = session.VirtualParamLevelLocal
	}

	switch name {
	case session.SPQR_DISTRIBUTION:
		rst.Client().SetDistribution(lvl, hintVal)
	case session.SPQR_DISTRIBUTED_RELATION:
		rst.Client().SetDistributedRelation(lvl, hintVal)
	case session.SPQR_DEFAULT_ROUTE_BEHAVIOUR:
		rst.Client().SetDefaultRouteBehaviour(lvl, hintVal)
	case session.SPQR_SHARDING_KEY:
		rst.Client().SetShardingKey(lvl, hintVal)
	case session.SPQR_PREFERRED_ENGINE:
		rst.Client().SetPreferredEngine(lvl, hintVal)
	case session.SPQR_REPLY_NOTICE:
		if value == "on" || value == "true" {
			rst.Client().SetShowNoticeMsg(lvl, true)
		} else {
			rst.Client().SetShowNoticeMsg(lvl, false)
		}
	case session.SPQR_MAINTAIN_PARAMS:
		if value == "on" || value == "true" {
			rst.Client().SetMaintainParams(lvl, true)
		} else {
			rst.Client().SetMaintainParams(lvl, false)
		}
	case session.SPQR_EXECUTE_ON:
		rst.Client().SetExecuteOn(lvl, hintVal)
	case session.SPQR_TARGET_SESSION_ATTRS:
		fallthrough
	case session.SPQR_TARGET_SESSION_ATTRS_ALIAS:
		fallthrough
	case session.SPQR_TARGET_SESSION_ATTRS_ALIAS_2:
		rst.Client().SetTsa(lvl, hintVal)
	case session.SPQR_ENGINE_V2:
		/* Ignore statement level here */
		switch value {
		case "true", "on", "ok":
			rst.Client().SetEnhancedMultiShardProcessing(session.VirtualParamLevelTxBlock, true)
		case "false", "off", "no":
			rst.Client().SetEnhancedMultiShardProcessing(session.VirtualParamLevelTxBlock, false)
		}
	case session.SPQR_AUTO_DISTRIBUTION:
		if _, err := rst.Qr.Mgr().GetDistribution(ctx, hintVal); err != nil &&
			hintVal != distributions.REPLICATED {
			return fmt.Errorf("SPQR invalid distribution '%s' for hint %s", hintVal, hintName)
		} else {
			rst.Client().SetParam(name, hintVal)
		}
	default:
		rst.Client().SetParam(name, hintVal)
	}

	return rst.QueryExecutor().ReplyCommandComplete("SET")
}
