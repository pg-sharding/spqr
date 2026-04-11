package relay

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/catalog"
	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/prepstatement"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/tupleslot"
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

func ReplyVirtualParamStateTTS(cl client.Client, tts *tupleslot.TupleTableSlot) {
	/* TODO: handle errors */
	_ = cl.Send(
		&pgproto3.RowDescription{
			Fields: tts.Desc,
		},
	)

	for _, r := range tts.Raw {
		_ = cl.Send(
			&pgproto3.DataRow{
				Values: r,
			},
		)
	}
}

var errAbortedTx = fmt.Errorf("current transaction is aborted, commands ignored until end of transaction block")

func (rst *RelayStateImpl) ProcQueryAdvancedTx(query string, binderQ func() error, doCaching bool) (*PortalDesc, error) {

	state, comment, err := rst.Parse(query, doCaching)
	if err != nil {
		if rst.QueryExecutor().TxStatus() == txstatus.TXACT {
			/* this way we format next msg correctly */
			rst.QueryExecutor().SetTxStatus(txstatus.TXERR)
		}
		spqrlog.Zero.Debug().Uint("client", rst.Client().ID()).Err(err).Msg("failed to parse query")
		if rst.QueryExecutor().TxStatus() == txstatus.TXERR {
			return nil, err
		}
		return nil, err
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
				return nil, errAbortedTx
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
	return pd, err
}

func (rst *RelayStateImpl) queryProc(comment string, binderQ func() error) error {
	mp, err := parser.ParseComment(comment)

	if err == nil {
		for key, val := range mp {
			if session.ParamIsBoolean(key) {
				guc, err := rst.Client().FindBoolGUC(key)
				if err != nil {
					return err
				}

				var v bool

				switch val {
				case "true", "ok", "on":
					v = true
				case "false", "no", "off":
					v = false
				default:
					return fmt.Errorf("malformed value for GUC: %v", val)
				}
				guc.Set(rst.Client(), session.VirtualParamLevelStatement, v)

			} else {
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
			_ = rst.Client().ReplyWarningf(spqrerror.PG_ACTIVE_SQL_TRANSACTION, "there is already a transaction in progress")
			return noDataPd, rst.QueryExecutor().ReplyCommandComplete("BEGIN")
		}
		err := rst.QueryExecutor().ExecBegin(query, &st)
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
			_ = rst.Client().ReplyWarningf(spqrerror.PG_NO_ACTIVE_SQL_TRANSACTION, "there is no transaction in progress")
			return noDataPd, rst.QueryExecutor().ReplyCommandComplete("COMMIT")
		}
		err := rst.QueryExecutor().ExecCommit(query)
		spqrlog.SLogger.ReportStatement(spqrlog.StmtTypeQuery, query, time.Since(startTime))
		return noDataPd, err
	case parser.ParseStateTXRollback:
		if rst.QueryExecutor().TxStatus() != txstatus.TXACT && rst.QueryExecutor().TxStatus() != txstatus.TXERR {
			_ = rst.Client().ReplyWarningf(spqrerror.PG_NO_ACTIVE_SQL_TRANSACTION, "there is no transaction in progress")
			return noDataPd, rst.QueryExecutor().ReplyCommandComplete("ROLLBACK")
		}
		err := rst.QueryExecutor().ExecRollback(query)
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

			if session.ParamIsBoolean(param) {

				tts := tupleslot.TupleTableSlot{
					Desc: []pgproto3.FieldDescription{
						{
							Name:         []byte("allow split update"),
							DataTypeOID:  catalog.TEXTOID,
							DataTypeSize: -1,
							TypeModifier: -1,
						},
					},
				}

				guc, err := rst.Client().FindBoolGUC(param)
				if err != nil {
					return nil, err
				}

				if guc.Get(rst.Client()) {
					tts.WriteDataRow("true")
				} else {
					tts.WriteDataRow("false")
				}

				ReplyVirtualParamStateTTS(rst.Client(), &tts)

			} else {
				switch param {
				case session.SPQR_DISTRIBUTION:
					spqrlog.SLogger.ReportStatement(spqrlog.StmtTypeQuery, query, time.Since(startTime))
					return nil, spqrerror.Newf(spqrerror.SPQR_NOT_IMPLEMENTED, "parameter \"%s\" isn't user accessible",
						session.SPQR_DISTRIBUTION)

				case session.SPQR_DISTRIBUTED_RELATION:
					spqrlog.SLogger.ReportStatement(spqrlog.StmtTypeQuery, query, time.Since(startTime))
					return nil, spqrerror.Newf(spqrerror.SPQR_NOT_IMPLEMENTED, "parameter \"%s\" isn't user accessible",
						session.SPQR_DISTRIBUTED_RELATION)

				case session.SPQR_DEFAULT_ROUTE_BEHAVIOUR:

					tts := tupleslot.TupleTableSlot{
						Desc: []pgproto3.FieldDescription{
							{
								Name:         []byte("default route behaviour"),
								DataTypeOID:  catalog.TEXTOID,
								DataTypeSize: -1,
								TypeModifier: -1,
							},
						},
					}
					tts.WriteDataRow(rst.Client().DefaultRouteBehaviour())

					/* XXX: move this call out of this function */
					ReplyVirtualParamStateTTS(rst.Client(), &tts)

				case session.SPQR_SHARDING_KEY:

					tts := tupleslot.TupleTableSlot{
						Desc: []pgproto3.FieldDescription{
							{
								Name:         []byte("sharding key"),
								DataTypeOID:  catalog.TEXTOID,
								DataTypeSize: -1,
								TypeModifier: -1,
							},
						},
					}
					tts.WriteDataRow(rst.Client().ShardingKey())

					ReplyVirtualParamStateTTS(rst.Client(), &tts)
				case session.SPQR_SCATTER_QUERY:
					spqrlog.SLogger.ReportStatement(spqrlog.StmtTypeQuery, query, time.Since(startTime))
					return nil, spqrerror.Newf(spqrerror.SPQR_NOT_IMPLEMENTED, "parameter \"%s\" isn't user accessible",
						session.SPQR_SCATTER_QUERY)
				case session.SPQR_EXECUTE_ON:

					tts := tupleslot.TupleTableSlot{
						Desc: []pgproto3.FieldDescription{
							{
								Name:         []byte("execute on"),
								DataTypeOID:  catalog.TEXTOID,
								DataTypeSize: -1,
								TypeModifier: -1,
							},
						},
					}
					tts.WriteDataRow(rst.Client().ExecuteOn())

					ReplyVirtualParamStateTTS(rst.Client(), &tts)

				case session.SPQR_REPLY_NOTICE:

					tts := tupleslot.TupleTableSlot{
						Desc: []pgproto3.FieldDescription{
							{
								Name:         []byte("show notice messages"),
								DataTypeOID:  catalog.TEXTOID,
								DataTypeSize: -1,
								TypeModifier: -1,
							},
						},
					}

					if rst.Client().ShowNoticeMsg() {
						tts.WriteDataRow("true")
					} else {
						tts.WriteDataRow("false")
					}

					ReplyVirtualParamStateTTS(rst.Client(), &tts)

				case session.SPQR_MAINTAIN_PARAMS:

					tts := tupleslot.TupleTableSlot{
						Desc: []pgproto3.FieldDescription{
							{
								Name:         []byte("maintain params"),
								DataTypeOID:  catalog.TEXTOID,
								DataTypeSize: -1,
								TypeModifier: -1,
							},
						},
					}

					if rst.Client().MaintainParams() {
						tts.WriteDataRow("true")
					} else {
						tts.WriteDataRow("false")
					}

					ReplyVirtualParamStateTTS(rst.Client(), &tts)

				case session.SPQR_ENGINE_V2:

					tts := tupleslot.TupleTableSlot{
						Desc: []pgproto3.FieldDescription{
							{
								Name:         []byte("engine v2"),
								DataTypeOID:  catalog.TEXTOID,
								DataTypeSize: -1,
								TypeModifier: -1,
							},
						},
					}

					if rst.Client().EnhancedMultiShardProcessing() {
						tts.WriteDataRow("on")
					} else {
						tts.WriteDataRow("off")
					}

					ReplyVirtualParamStateTTS(rst.Client(), &tts)

				case session.SPQR_TARGET_SESSION_ATTRS:
					fallthrough
				case session.SPQR_TARGET_SESSION_ATTRS_ALIAS:
					fallthrough
				case session.SPQR_TARGET_SESSION_ATTRS_ALIAS_2:

					tts := tupleslot.TupleTableSlot{
						Desc: []pgproto3.FieldDescription{
							{
								Name:         []byte("target session attrs"),
								DataTypeOID:  catalog.TEXTOID,
								DataTypeSize: -1,
								TypeModifier: -1,
							},
						},
					}
					tts.WriteDataRow(string(rst.Client().GetTsa()))

					ReplyVirtualParamStateTTS(rst.Client(), &tts)

				case session.SPQR_PREFERRED_ENGINE:
					ReplyVirtualParamState(rst.Client(), "preferred engine", []byte(rst.Client().PreferredEngine()))

				case session.SPQR_COMMIT_STRATEGY:

					tts := tupleslot.TupleTableSlot{
						Desc: []pgproto3.FieldDescription{
							{
								Name:         []byte("commit strategy"),
								DataTypeOID:  25,
								DataTypeSize: -1,
								TypeModifier: -1,
							},
						},
					}
					tts.WriteDataRow(string(rst.Client().CommitStrategy()))

					ReplyVirtualParamStateTTS(rst.Client(), &tts)
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
					if err := rst.QueryExecutor().ExecSet(rst, query, name, val, q.IsLocal); err != nil {
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

	if session.ParamIsBoolean(name) {

		var v bool

		switch value {
		case "true", "ok", "on":
			v = true
		case "false", "no", "off":
			v = false
		default:
			return fmt.Errorf("malformed value for GUC: %v", value)
		}
		guc, err := rst.Client().FindBoolGUC(name)
		if err != nil {
			return err
		}

		guc.Set(rst.Client(), lvl, v)
	} else {

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
				rst.Client().SetParam(name, hintVal, isLocal)
			}
		case session.SPQR_COMMIT_STRATEGY:
			rst.Client().SetCommitStrategy(hintVal)
		default:
			rst.Client().SetParam(name, hintVal, isLocal)
		}
	}

	return rst.QueryExecutor().ReplyCommandComplete("SET")
}
