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
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/tupleslot"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/qparser"
	"github.com/pg-sharding/spqr/router/rerrors"
	"github.com/pg-sharding/spqr/router/statistics"
	"github.com/pg-sharding/spqr/router/twopc"
	"github.com/pg-sharding/spqr/router/xproto"
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

	stmts, comment, err := rst.Parse(query, doCaching)
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
	var pd *PortalDesc

l:
	for i, stmt := range stmts {

		if i > 0 {

			switch stmt.(type) {
			case *lyx.VariableSetStmt, *lyx.VariableShowStmt:
				/* ok */
			default:
				continue l
			}

			/* Okay, respond with CommandComplete first. */
			err = rst.QueryExecutor().DeriveCommandComplete()
			if err != nil {
				return nil, err
			}
		}

		rst.qp.SetStmt(stmt)

		txbefore := rst.QueryExecutor().TxStatus()
		if txbefore == txstatus.TXERR {

			/* If user supplied COMMIT in already-errored tx, simply rollback
			* and end tx block. */
			st, ok := stmt.(*lyx.TransactionStmt)
			if !ok {
				return nil, errAbortedTx
			}
			/* It is necessary here to change state to trigger correct
			* execution path ProcQueryAdvanced, that is, single-slice scatter-out
			* query (no 2pc commit management!) */
			if st.Kind == lyx.TRANS_STMT_COMMIT {
				st.Kind = lyx.TRANS_STMT_ROLLBACK
				/* We will actually send COMMIT as use command to shards, do not
				* override `query` */
			} else if st.Kind != lyx.TRANS_STMT_ROLLBACK {
				return nil, errAbortedTx
			}
		}

		startTime := time.Now()

		pd, err = rst.ProcQueryAdvanced(query, stmt, comment, binderQ, doCaching)

		spqrlog.SLogger.ReportStatement(spqrlog.StmtTypeQuery, query, time.Since(startTime))

		if txbefore != txstatus.TXIDLE && err != nil {
			rst.QueryExecutor().SetTxStatus(txstatus.TXERR)
		}

		/* outer function will complete relay here */
		if err != nil {
			spqrlog.Zero.Error().Err(err).Uint("client-id", rst.Client().ID()).Msg("completing client relay with error")
			return nil, err
		}
		spqrlog.Zero.Debug().Uint("client-id", rst.Client().ID()).Msg("executed statement in client relay")
	}
	return pd, err
}

func (rst *RelayStateImpl) queryProc(comment string, binderQ func() error) error {
	mp, err := qparser.ParseComment(comment)

	if err == nil {
		if err := rst.processSpqrHint(context.TODO(), mp, false, true); err != nil {
			return err
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
func (rst *RelayStateImpl) ProcQueryAdvanced(query string, stmt lyx.Node, comment string, binderQ func() error, _ bool) (*PortalDesc, error) {

	/* !!! Do not complete relay here (no TX status management) !!! */

	spqrlog.Zero.Debug().Str("query", query).Uint("client", rst.Client().ID()).Msgf("process relay state advanced")

	statistics.IncTotalRequest()

	/* XXX: support implicit tx semantics here */
	statistics.RecordStartTime(statistics.StatisticsTypeRouter, time.Now(), rst.Client())

	switch st := stmt.(type) {
	case nil:
		/* empty query */
		rst.QueryExecutor().ReplyEmptyQuery()
		// do not complete relay  here
		return noDataPd, nil
	case *lyx.TransactionStmt:

		switch st.Kind {
		case lyx.TRANS_STMT_BEGIN:

			if rst.QueryExecutor().TxStatus() != txstatus.TXIDLE {
				// ignore this
				_ = rst.Client().ReplyWarningf(spqrerror.PG_ACTIVE_SQL_TRANSACTION, "there is already a transaction in progress")
				return noDataPd, rst.QueryExecutor().ReplyCommandComplete("BEGIN")
			}

			err := rst.QueryExecutor().ExecBegin(query, st)
			return noDataPd, err

		case lyx.TRANS_STMT_COMMIT:
			if mp, err := qparser.ParseComment(comment); err == nil {

				if val, ok := mp[session.SPQR_COMMIT_STRATEGY]; ok {
					switch val {
					case twopc.CommitStrategy2pc:
						fallthrough
					case twopc.CommitStrategy1pc:
						fallthrough
					case twopc.CommitStrategyBestEffort:
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
			return noDataPd, err
		case lyx.TRANS_STMT_ROLLBACK:
			if rst.QueryExecutor().TxStatus() != txstatus.TXACT && rst.QueryExecutor().TxStatus() != txstatus.TXERR {
				_ = rst.Client().ReplyWarningf(spqrerror.PG_NO_ACTIVE_SQL_TRANSACTION, "there is no transaction in progress")
				return noDataPd, rst.QueryExecutor().ReplyCommandComplete("ROLLBACK")
			}
			err := rst.QueryExecutor().ExecRollback(query)
			return noDataPd, err

		default:
			return nil, fmt.Errorf("unknown transaction statement kind: %+v", st.Kind)
		}

	case *lyx.DiscardStmt:
		/* Close all prepared */

		for _, name := range rst.QueryExecutor().Client().ListPreparedStatements() {
			rst.QueryExecutor().Client().ClosePreparedStatement(name)
		}

		return noDataPd, rst.QueryExecutor().ReplyCommandComplete("DISCARD ALL")
	case *lyx.DeallocateStmt:
		var cmdTag string
		if st.Name == "" {
			/* Close all */

			for _, name := range rst.QueryExecutor().Client().ListPreparedStatements() {
				rst.QueryExecutor().Client().ClosePreparedStatement(name)
			}
			cmdTag = "DEALLOCATE ALL"
		} else {
			rst.QueryExecutor().Client().ClosePreparedStatement(st.Name)
			cmdTag = "DEALLOCATE"
		}

		return noDataPd, rst.QueryExecutor().ReplyCommandComplete(cmdTag)

	// with tx pooling we might have no active connection while processing set x to y
	case *lyx.VariableShowStmt:
		var pd *PortalDesc

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

			guc, err := rst.Client().FindBoolGUC(param)
			if err != nil {
				return nil, err
			}

			tts := tupleslot.TupleTableSlot{
				Desc: []pgproto3.FieldDescription{
					{
						Name:         []byte(guc.ShortName()),
						DataTypeOID:  catalog.TEXTOID,
						DataTypeSize: -1,
						TypeModifier: -1,
					},
				},
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
				return nil, spqrerror.Newf(spqrerror.SPQR_NOT_IMPLEMENTED, "parameter \"%s\" isn't user accessible",
					session.SPQR_DISTRIBUTION)

			case session.SPQR_DISTRIBUTED_RELATION:
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
						return pd, err
					}

					ReplyVirtualParamState(rst.Client(), param, []byte(rst.Client().Params()[param]))
				}
			}
		}

		if err := rst.QueryExecutor().ReplyCommandComplete("SHOW"); err != nil {
			return nil, err
		}

		return pd, nil
	case *lyx.VariableSetStmt:

		q, ok := stmt.(*lyx.VariableSetStmt)
		if !ok {
			return nil, rerrors.ErrComplexQuery
		}
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
				if err := rst.processSpqrHint(ctx, map[string]string{
					name: val,
				}, q.IsLocal, false); err != nil {
					return nil, err
				}
			} else {
				if err := rst.QueryExecutor().ExecSet(rst, query, name, val, q.IsLocal); err != nil {
					return nil, err
				}
			}
		}

		return noDataPd, nil
	case *lyx.PrepareStmt:
		// sql level prepares stmt pooling
		if AdvancedPoolModeNeeded(rst) {
			spqrlog.Zero.Debug().Msg("sql level prep statement pooling support is on")

			// prepare *name* as *query*
			ss := strings.Split(strings.Split(strings.Split(strings.ToLower(query), "prepare")[1], strings.ToLower(st.Name))[1], "as")[1]

			if _, err := rst.relayParsePrepared(context.TODO(), st.Name, ss, nil); err != nil {
				return nil, err
			}

			if err := rst.QueryExecutor().ReplyCommandComplete("PREPARE"); err != nil {
				return nil, err
			}

			return nil, nil
		} else {
			// process like regular query
			err := rst.queryProc(comment, binderQ)
			return nil, err
		}
	case *lyx.ExecuteStmt:
		if AdvancedPoolModeNeeded(rst) {

			var params [][]byte
			var parameterFormatCodes []int16
			var resultFormatCodes []int16

			for _, p := range st.Params {
				switch q := p.(type) {
				case *lyx.AExprSConst:
					params = append(params, []byte(q.Value))
				default:
					return nil, rerrors.ErrComplexQuery
				}
				parameterFormatCodes = append(parameterFormatCodes, xproto.FormatCodeText)
				resultFormatCodes = append(resultFormatCodes, xproto.FormatCodeText)
			}

			if err := rst.BindPrepared(context.TODO(),
				st.Name /* unnamed portal */, "",
				params,
				parameterFormatCodes,
				resultFormatCodes); err != nil {
				return nil, err
			}

			if err := rst.DescribePrepared(xproto.ObjectTypeStatement,
				st.Name,
				nil); err != nil {
				return nil, err
			}

			if err := rst.ExecutePortal( /* unnamed portal */ ""); err != nil {
				return nil, err
			}

			return nil, nil
		} else {
			// process like regular query
			err := rst.queryProc(comment, binderQ)
			return nil, err
		}
	default:
		err := rst.queryProc(comment, binderQ)
		return nil, err
	}
}

func (rst *RelayStateImpl) processSpqrHint(_ context.Context,
	mp map[string]string, isLocal bool, isStmt bool) error {

	lvl := session.VirtualParamLevelTxBlock

	if isStmt {
		lvl = session.VirtualParamLevelStatement
	}

	if isLocal {
		lvl = session.VirtualParamLevelLocal
	}

	for hintName, hintVal := range mp {
		name := virtualParamTransformName(hintName)
		value := strings.ToLower(hintVal)

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
			case session.SPQR_SCATTER_QUERY:
				/* any non-empty value of SPQR_SCATTER_QUERY is local and means ON */
				rst.Client().SetScatterQuery(hintVal != "")
			case session.SPQR_EXECUTE_ON:
				if _, err := topology.TopMgr.ShardById(hintVal); err != nil {
					return errAbortedTx
				}
				rst.Client().SetExecuteOn(lvl, hintVal)
			case session.SPQR_DISTRIBUTION:
				rst.Client().SetDistribution(lvl, hintVal)
			case session.SPQR_DISTRIBUTION_KEY:
				rst.Client().SetDistributionKey(hintVal)
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

				if hintVal == distributions.REPLICATED {
					/* This is an ddl query, which creates relation along with attaching to REPLICATED distribution */
					rst.Client().SetAutoDistribution(hintVal)
					continue
				}

				_, err := rst.QueryRouter().Mgr().GetDistribution(context.TODO(), hintVal)
				if err != nil {
					return fmt.Errorf("SPQR invalid distribution '%s' for hint %s", hintVal, hintName)
				}

				/* Should we create distributed or reference relation? */

				_, ok := mp[session.SPQR_DISTRIBUTION_KEY]
				if !ok {
					if rst.Client().DistributionKey() == "" {
						return fmt.Errorf("spqr distribution specified, but distribution key omitted")
					}
				}

				/* This is an ddl query, which creates relation along with attaching to distribution */
				rst.Client().SetAutoDistribution(hintVal)

				/*
				* this is too early to do anything with distribution hint, as we do not yet parsed
				* DDL of about-to-be-created relation
				 */

			case session.SPQR_COMMIT_STRATEGY:
				rst.Client().SetCommitStrategy(hintVal)
			default:
				rst.Client().SetParam(name, hintVal, isLocal)
			}
		}
	}

	return rst.QueryExecutor().ReplyCommandComplete("SET")
}
