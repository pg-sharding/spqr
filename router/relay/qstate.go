package relay

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/parser"
	"github.com/pg-sharding/spqr/router/routehint"
	"github.com/pg-sharding/spqr/router/routingstate"
	"github.com/pg-sharding/spqr/router/statistics"
)

func AdvancedPoolModeNeeded(rst RelayStateMgr) bool {
	return rst.Client().Rule().PoolMode == config.PoolModeTransaction && rst.Client().Rule().PoolPreparedStatement || rst.RouterMode() == config.ProxyMode
}

func deparseRouteHint(rst RelayStateMgr, params map[string]string) (routehint.RouteHint, error) {
	if _, ok := params[session.SPQR_SCATTER_QUERY]; ok {
		return &routehint.ScatterRouteHint{}, nil
	}
	if val, ok := params[session.SPQR_SHARDING_KEY]; ok {
		spqrlog.Zero.Debug().Str("sharding key", val).Msg("checking hint key")

		dsId := ""
		if dsId, ok = params[session.SPQR_DISTRIBUTION]; !ok {
			return nil, spqrerror.New(spqrerror.SPQR_NO_DISTRIBUTION, "got sharding key in comment without distribution")
		}

		ctx := context.TODO()
		krs, err := rst.QueryRouter().Mgr().ListKeyRanges(ctx, dsId)
		if err != nil {
			return nil, err
		}

		ds, err := rst.QueryRouter().DeparseKeyWithRangesInternal(context.TODO(), val, krs)
		if err != nil {
			return nil, err
		}
		return &routehint.TargetRouteHint{
			State: routingstate.ShardMatchState{
				Route: ds,
			},
		}, nil
	}

	return &routehint.EmptyRouteHint{}, nil
}

// ProcQueryAdvanced processes query, with router relay state
// There are several types of query that we want to process in non-passthrough way.
// For example, after BEGIN we wait until first client query witch can be router to some shard.
// So, we need to proccess SETs, BEGINs, ROLLBACKs etc ourselves.
// ProtoStateHandler provides set of function for either simple of extended protoc interactions
// query param is either plain query from simple proto or bind query from x proto
func ProcQueryAdvanced(rst RelayStateMgr, query string, ph ProtoStateHandler, binder func() error) error {
	statistics.RecordStartTime(statistics.Router, time.Now(), rst.Client().ID())

	spqrlog.Zero.Debug().Str("query", query).Uint("client", spqrlog.GetPointer(rst.Client())).Msgf("process relay state advanced")
	state, comment, err := rst.Parse(query)
	if err != nil {
		return fmt.Errorf("error processing query '%v': %w", query, err)
	}

	mp, err := parser.ParseComment(comment)

	if err == nil {
		routeHint, _ := deparseRouteHint(rst, mp)
		rst.Client().SetRouteHint(routeHint)

		if val, ok := mp["target-session-attrs"]; ok {
			// TBD: validate
			spqrlog.Zero.Debug().Str("tsa", val).Msg("parse tsa from comment")
			rst.Client().SetTsa(val)
		}
		if val, ok := mp[session.SPQR_DEFAULT_ROUTE_BEHAVIOUR]; ok {
			spqrlog.Zero.Debug().Str("tsa", val).Msg("parse default route behaviour from comment")
			rst.Client().SetDefaultRouteBehaviour(val)
		}
		if val, ok := mp[session.SPQR_SHARDING_KEY]; ok {
			spqrlog.Zero.Debug().Str("tsa", val).Msg("parse sharding key from comment")
			rst.Client().SetShardingKey(val)
		}
	}

	switch st := state.(type) {
	case parser.ParseStateTXBegin:
		if rst.TxStatus() != txstatus.TXIDLE {
			// ignore this
			_ = rst.Client().ReplyWarningf("there is already transaction in progress")
			return rst.Client().ReplyCommandComplete("BEGIN")
		}
		// explicitly set silent query message, as it can differ from query begin in xporot
		rst.AddSilentQuery(&pgproto3.Query{
			String: query,
		})

		rst.SetTxStatus(txstatus.TXACT)
		rst.Client().StartTx()

		spqrlog.Zero.Debug().Msg("start new transaction")

		for _, opt := range st.Options {
			switch opt {
			case lyx.TransactionReadOnly:
				rst.Client().SetTsa(config.TargetSessionAttrsPS)
			case lyx.TransactionReadWrite:
				rst.Client().SetTsa(config.TargetSessionAttrsRW)
			}
		}
		return rst.Client().ReplyCommandComplete("BEGIN")
	case parser.ParseStateTXCommit:
		if rst.TxStatus() != txstatus.TXACT && rst.TxStatus() != txstatus.TXERR {
			_ = rst.Client().ReplyWarningf("there is no transaction in progress")
			return rst.Client().ReplyCommandComplete("COMMIT")
		}
		return ph.ExecCommit(rst, query)
	case parser.ParseStateTXRollback:
		if rst.TxStatus() != txstatus.TXACT && rst.TxStatus() != txstatus.TXERR {
			_ = rst.Client().ReplyWarningf("there is no transaction in progress")
			return rst.Client().ReplyCommandComplete("ROLLBACK")
		}
		return ph.ExecRollback(rst, query)
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
				return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "setting \"%s\" is forbidden", session.SPQR_DISTRIBUTION)
			case session.SPQR_DEFAULT_ROUTE_BEHAVIOUR:
				rst.Client().SetDefaultRouteBehaviour(st.Value)
			case session.SPQR_SHARDING_KEY:
				rst.Client().SetShardingKey(st.Value)
			default:
				rst.Client().SetParam(st.Name, st.Value)
			}

			return rst.Client().ReplyCommandComplete("SET")
		}

		return ph.ExecSet(rst, query, st.Name, st.Value)
	case parser.ParseStateShowStmt:
		param := st.Name
		// manually create router responce
		// here we just reply single row with single column value

		switch param {
		case session.SPQR_DISTRIBUTION:
			_ = rst.Client().Send(&pgproto3.ErrorResponse{
				Message: fmt.Sprintf("parameter \"%s\" isn't user accessible", session.SPQR_DISTRIBUTION),
			})
		case session.SPQR_DEFAULT_ROUTE_BEHAVIOUR:

			_ = rst.Client().Send(
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("default route behaviour"),
							DataTypeOID:  25,
							DataTypeSize: -1,
							TypeModifier: -1,
						},
					},
				},
			)

			_ = rst.Client().Send(
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte(rst.Client().DefaultRouteBehaviour()),
					},
				},
			)
		case session.SPQR_SHARDING_KEY:

			_ = rst.Client().Send(
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("sharding key"),
							DataTypeOID:  25,
							DataTypeSize: -1,
							TypeModifier: -1,
						},
					},
				},
			)

			_ = rst.Client().Send(
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("no val"),
					},
				},
			)
		case session.SPQR_SCATTER_QUERY:

			_ = rst.Client().Send(
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte("scatter query"),
							DataTypeOID:  25,
							DataTypeSize: -1,
							TypeModifier: -1,
						},
					},
				},
			)

			_ = rst.Client().Send(
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte("no val"),
					},
				},
			)
		default:

			_ = rst.Client().Send(
				&pgproto3.RowDescription{
					Fields: []pgproto3.FieldDescription{
						{
							Name:         []byte(param),
							DataTypeOID:  25,
							DataTypeSize: -1,
							TypeModifier: -1,
						},
					},
				},
			)
			_ = rst.Client().Send(
				&pgproto3.DataRow{
					Values: [][]byte{
						[]byte(rst.Client().Params()[param]),
					},
				},
			)
		}
		return rst.Client().ReplyCommandComplete("SHOW")
	case parser.ParseStateResetStmt:
		rst.Client().ResetParam(st.Name)

		if err := ph.ExecReset(rst, query, st.Name); err != nil {
			return err
		}

		return rst.Client().ReplyCommandComplete("RESET")
	case parser.ParseStateResetMetadataStmt:
		if err := ph.ExecResetMetadata(rst, query, st.Setting); err != nil {
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
		if err := ph.ExecSetLocal(rst, query, st.Name, st.Value); err != nil {
			return err
		}

		return rst.Client().ReplyCommandComplete("SET")
	case parser.ParseStatePrepareStmt:
		// sql level prepares stmt pooling
		if AdvancedPoolModeNeeded(rst) {
			spqrlog.Zero.Debug().Msg("sql level prep statement pooling support is on")
			rst.Client().StorePreparedStatement(st.Name, st.Query)
			return nil
		} else {
			// process like regular query
			return binder()
		}
	case parser.ParseStateExecute:
		if AdvancedPoolModeNeeded(rst) {
			// do nothing
			rst.Client().PreparedStatementQueryByName(st.Name)
			return nil
		} else {
			// process like regular query
			return binder()
		}
	case parser.ParseStateExplain:
		_ = rst.Client().ReplyErrMsgByCode(spqrerror.SPQR_UNEXPECTED)
		return nil
	default:
		return binder()
	}
}
