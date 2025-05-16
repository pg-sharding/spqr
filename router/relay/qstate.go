package relay

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/prepstatement"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/parser"
	"github.com/pg-sharding/spqr/router/statistics"
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

func (rst *RelayStateImpl) ProcQueryAdvancedTx(query string, binderQ func() error, doCaching, completeRelay bool) error {

	state, comment, err := rst.Parse(query, doCaching)
	if err != nil {
		if rst.QueryExecutor().TxStatus() == txstatus.TXACT {
			/* this way we format next msg correctly */
			rst.QueryExecutor().SetTxStatus(txstatus.TXERR)
		}

		err = fmt.Errorf("client processing error: '%v': %w, tx status %s", query, err, rst.QueryExecutor().TxStatus().String())

		if rst.QueryExecutor().TxStatus() == txstatus.TXERR {
			// TODO: figure out if we need this
			// _ = rst.UnrouteRoutes(rst.ActiveShards())
			return rst.Client().ReplyErrWithTxStatus(err, txstatus.TXERR)
		}

		if rst.QueryExecutor().TxStatus() == txstatus.TXACT {
			return rst.Client().ReplyErrWithTxStatus(err, txstatus.TXERR)
		}

		return rst.UnRouteWithError(rst.ActiveShards(), err)
	}

	txbefore := rst.QueryExecutor().TxStatus()
	if txbefore == txstatus.TXERR {
		if _, ok := state.(parser.ParseStateTXRollback); !ok {
			return rst.Client().ReplyErrWithTxStatus(errAbortedTx, txstatus.TXERR)
		}
	}

	err = rst.ProcQueryAdvanced(query, state, comment, binderQ, doCaching)

	if txbefore != txstatus.TXIDLE && err != nil {
		rst.QueryExecutor().SetTxStatus(txstatus.TXERR)
	}

	if !completeRelay {
		return nil
	}

	if err == nil {
		return rst.CompleteRelay(true)
	}

	/* outer function will complete relay here */

	spqrlog.Zero.Debug().Err(err).Uint("client-id", rst.Client().ID()).Msg("completing client relay with error")

	switch err {
	case io.ErrUnexpectedEOF:
		fallthrough
	case io.EOF:
		_ = rst.Unroute(rst.ActiveShards())
		return err
		// ok
	default:
		spqrlog.Zero.Error().
			Uint("client", rst.Client().ID()).Int("tx-status", int(rst.QueryExecutor().TxStatus())).Err(err).
			Msg("client iteration done with error")

		rerr := fmt.Errorf("client processing error: %v, tx status %s", err, rst.QueryExecutor().TxStatus().String())

		if rst.QueryExecutor().TxStatus() == txstatus.TXERR {
			return rst.Client().ReplyErrWithTxStatus(rerr, txstatus.TXERR)
		}

		return rst.UnRouteWithError(rst.ActiveShards(), rerr)
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
				rst.Client().SetTsa(true, val)
			case session.SPQR_DEFAULT_ROUTE_BEHAVIOUR:
				spqrlog.Zero.Debug().Str("default route", val).Msg("parse default route behaviour from comment")
				rst.Client().SetDefaultRouteBehaviour(true, val)
			case session.SPQR_SHARDING_KEY:
				spqrlog.Zero.Debug().Str("sharding key", val).Msg("parse sharding key from comment")
				rst.Client().SetShardingKey(true, val)
			case session.SPQR_DISTRIBUTION:
				spqrlog.Zero.Debug().Str("distribution", val).Msg("parse distribution from comment")
				rst.Client().SetDistribution(true, val)
			case session.SPQR_SCATTER_QUERY:
				/* any non-empty value of SPQR_SCATTER_QUERY is local and means ON */
				spqrlog.Zero.Debug().Str("scatter query", val).Msg("parse scatter query from comment")
				rst.Client().SetScatterQuery(val != "")
			case session.SPQR_EXECUTE_ON:

				if _, ok := config.RouterConfig().ShardMapping[val]; !ok {
					return fmt.Errorf("no such shard: %v", val)
				}
				rst.Client().SetExecuteOn(true, val)
			case session.SPQR_ENGINE_V2:
				switch val {
				case "true", "ok", "on":
					rst.Client().SetEnhancedMultiShardProcessing(true, true)
				case "false", "no", "off":
					rst.Client().SetEnhancedMultiShardProcessing(true, false)
				}
			case session.SPQR_AUTO_DISTRIBUTION:
				if valDistrib, ok := mp[session.SPQR_DISTRIBUTION_KEY]; ok {
					_, err = rst.QueryRouter().Mgr().GetDistribution(context.TODO(), val)
					if err != nil {
						return err
					}

					/* This is an ddl query, which creates relation along with attaching to distribution */
					rst.Client().SetAutoDistribution(true, val)
					rst.Client().SetDistributionKey(true, valDistrib)

					/* this is too early to do anything with distribution hint, as we do not yet parsed
					* DDL of about-to-be-created relation
					 */
				} else {
					return fmt.Errorf("spqr distribution specified, but distribution key omitted")
				}
			}
		}
	}

	return binderQ()
}

// ProcQueryAdvanced processes query, with router relay state
// There are several types of query that we want to process in non-passthrough way.
// For example, after BEGIN we wait until first client query witch can be router to some shard.
// So, we need to process SETs, BEGINs, ROLLBACKs etc ourselves.
// QueryStateExecutor provides set of function for either simple of extended protoc interactions
// query param is either plain query from simple proto or bind query from x proto
func (rst *RelayStateImpl) ProcQueryAdvanced(query string, state parser.ParseState, comment string, binderQ func() error, doCaching bool) error {
	statistics.RecordStartTime(statistics.Router, time.Now(), rst.Client().ID())

	/* !!! Do not complete relay here (no TX status management) !!! */

	spqrlog.Zero.Debug().Str("query", query).Uint("client", rst.Client().ID()).Msgf("process relay state advanced")

	switch st := state.(type) {
	case parser.ParseStateTXBegin:
		if rst.QueryExecutor().TxStatus() != txstatus.TXIDLE {
			// ignore this
			_ = rst.Client().ReplyWarningf("there is already transaction in progress")
			return rst.Client().ReplyCommandComplete("BEGIN")
		}
		return rst.QueryExecutor().ExecBegin(rst, query, &st)
	case parser.ParseStateTXCommit:

		if mp, err := parser.ParseComment(comment); err == nil {

			if val, ok := mp[session.SPQR_COMMIT_STRATEGY]; ok {
				switch val {
				case twopc.COMMIT_STRATEGY_2PC:
					fallthrough
				case twopc.COMMIT_STRATEGY_1PC:
					fallthrough
				case twopc.COMMIT_STRATEGY_BEST_EFFORT:
					rst.Client().SetCommitStrategy(false, val)
				default:
					/*should error-out*/
				}
			}
		}

		if rst.QueryExecutor().TxStatus() != txstatus.TXACT && rst.QueryExecutor().TxStatus() != txstatus.TXERR {
			_ = rst.Client().ReplyWarningf("there is no transaction in progress")
			return rst.Client().ReplyCommandComplete("COMMIT")
		}
		return rst.QueryExecutor().ExecCommit(rst, query)
	case parser.ParseStateTXRollback:
		if rst.QueryExecutor().TxStatus() != txstatus.TXACT && rst.QueryExecutor().TxStatus() != txstatus.TXERR {
			_ = rst.Client().ReplyWarningf("there is no transaction in progress")
			return rst.Client().ReplyCommandComplete("ROLLBACK")
		}
		return rst.QueryExecutor().ExecRollback(rst, query)
	case parser.ParseStateEmptyQuery:
		if err := rst.Client().Send(&pgproto3.EmptyQueryResponse{}); err != nil {
			return err
		}
		// do not complete relay  here
		return nil
	// with tx pooling we might have no active connection while processing set x to y
	case parser.ParseStateSetStmt:
		spqrlog.Zero.Debug().
			Str("name", st.Name).
			Str("value", st.Value).
			Msg("applying parsed set stmt")

		if strings.HasPrefix(st.Name, "__spqr__") {
			switch st.Name {
			case session.SPQR_DISTRIBUTION:
				rst.Client().SetDistribution(false, st.Value)
			case session.SPQR_DEFAULT_ROUTE_BEHAVIOUR:
				rst.Client().SetDefaultRouteBehaviour(false, st.Value)
			case session.SPQR_SHARDING_KEY:
				rst.Client().SetShardingKey(false, st.Value)
			case session.SPQR_REPLY_NOTICE:
				if st.Value == "on" || st.Value == "true" {
					rst.Client().SetShowNoticeMsg(true)
				} else {
					rst.Client().SetShowNoticeMsg(false)
				}
			case session.SPQR_MAINTAIN_PARAMS:
				if st.Value == "on" || st.Value == "true" {
					rst.Client().SetMaintainParams(true)
				} else {
					rst.Client().SetMaintainParams(false)
				}
			case session.SPQR_EXECUTE_ON:
				rst.Client().SetExecuteOn(false, st.Value)
			case session.SPQR_TARGET_SESSION_ATTRS:
				fallthrough
			case session.SPQR_TARGET_SESSION_ATTRS_ALIAS:
				fallthrough
			case session.SPQR_TARGET_SESSION_ATTRS_ALIAS_2:
				rst.Client().SetTsa(false, st.Value)
			case session.SPQR_ENGINE_V2:
				switch st.Value {
				case "true", "on", "ok":
					rst.Client().SetEnhancedMultiShardProcessing(false, true)
				case "false", "off", "no":
					rst.Client().SetEnhancedMultiShardProcessing(false, false)
				}
			default:
				rst.Client().SetParam(st.Name, st.Value)
			}

			return rst.Client().ReplyCommandComplete("SET")
		}

		return rst.QueryExecutor().ExecSet(rst, query, st.Name, st.Value)
	case parser.ParseStateShowStmt:
		param := st.Name
		// manually create router response
		// here we just reply single row with single column value

		switch param {
		case session.SPQR_DISTRIBUTION:
			return rst.Client().Send(
				&pgproto3.ErrorResponse{
					Message: fmt.Sprintf("parameter \"%s\" isn't user accessible",
						session.SPQR_DISTRIBUTION),
					Severity: "ERROR",
					Code:     spqrerror.SPQR_NOT_IMPLEMENTED,
				})

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
			return rst.Client().Send(
				&pgproto3.ErrorResponse{
					Message: fmt.Sprintf("parameter \"%s\" isn't user accessible",
						session.SPQR_SCATTER_QUERY),
					Severity: "ERROR",
					Code:     spqrerror.SPQR_NOT_IMPLEMENTED,
				})
		case session.SPQR_EXECUTE_ON:
			ReplyVirtualParamState(rst.Client(), "execute on", []byte(rst.Client().ExecuteOn()))
		case session.SPQR_TARGET_SESSION_ATTRS:
			fallthrough
		case session.SPQR_TARGET_SESSION_ATTRS_ALIAS:
			fallthrough
		case session.SPQR_TARGET_SESSION_ATTRS_ALIAS_2:
			ReplyVirtualParamState(rst.Client(), "target session attrs", []byte(rst.Client().GetTsa()))
		default:

			if strings.HasPrefix(st.Name, "__spqr__") {
				ReplyVirtualParamState(rst.Client(), param, []byte(rst.Client().Params()[param]))
			} else {
				/* If router does dot have any info about param, fire query to random shard. */
				if _, ok := rst.Client().Params()[param]; !ok {
					return rst.queryProc(comment, binderQ)
				}

				ReplyVirtualParamState(rst.Client(), param, []byte(rst.Client().Params()[param]))
			}
		}
		return rst.Client().ReplyCommandComplete("SHOW")
	case parser.ParseStateResetStmt:
		switch st.Name {
		case session.SPQR_TARGET_SESSION_ATTRS:
			fallthrough
		case session.SPQR_TARGET_SESSION_ATTRS_ALIAS:
			rst.Client().ResetTsa()
		default:
			rst.Client().ResetParam(st.Name)

			if err := rst.QueryExecutor().ExecReset(rst, query, st.Name); err != nil {
				return err
			}
		}

		return rst.Client().ReplyCommandComplete("RESET")
	case parser.ParseStateResetMetadataStmt:
		if err := rst.QueryExecutor().ExecResetMetadata(rst, query, st.Setting); err != nil {
			return err
		}

		rst.Client().ResetParam(st.Setting)
		if st.Setting == "session_authorization" {
			rst.Client().ResetParam("role")
		}

		return rst.Client().ReplyCommandComplete("RESET")
	case parser.ParseStateResetAllStmt:
		rst.Client().ResetAll()
		return rst.Client().ReplyCommandComplete("RESET")
	case parser.ParseStateSetLocalStmt:
		if err := rst.QueryExecutor().ExecSetLocal(rst, query, st.Name, st.Value); err != nil {
			return err
		}
		return rst.Client().ReplyCommandComplete("SET")
	case parser.ParseStatePrepareStmt:
		// sql level prepares stmt pooling
		if AdvancedPoolModeNeeded(rst) {
			spqrlog.Zero.Debug().Msg("sql level prep statement pooling support is on")

			/* no oid for SQL level prep stmt */
			rst.Client().StorePreparedStatement(&prepstatement.PreparedStatementDefinition{
				Name:  st.Name,
				Query: st.Query,
			})
			return nil
		} else {
			// process like regular query
			return rst.queryProc(comment, binderQ)
		}
	case parser.ParseStateExecute:
		if AdvancedPoolModeNeeded(rst) {
			// do nothing
			// wtf? TODO: test and fix
			rst.Client().PreparedStatementQueryByName(st.Name)
			return nil
		} else {
			// process like regular query
			return rst.queryProc(comment, binderQ)
		}
	case parser.ParseStateExplain:
		return rst.Client().Send(
			&pgproto3.ErrorResponse{
				Message:  "SPQR explain to be supported",
				Severity: "ERROR",
				Code:     spqrerror.SPQR_NOT_IMPLEMENTED,
			})
	default:
		return rst.queryProc(comment, binderQ)
	}
}
